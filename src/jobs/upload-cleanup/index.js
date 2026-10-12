/**
 * Job: upload-cleanup — deletes direct-to-Cloudinary uploads that were never attached to a record.
 *
 * Clients upload files straight to Cloudinary with a signature from seefix-api (issue #12). Every such upload is
 * tagged `seefix-pending`. When a client then submits the form, the API saves the upload's public ID in
 * ReportImages / WorkOrderImages / ProcurementDocuments. Uploads whose form was abandoned are never saved.
 *
 * Each pass looks at `seefix-pending` uploads older than UPLOAD_CLEANUP_MIN_AGE_HOURS:
 * - saved in the database -> the pending tag is removed (kept for good, never looked at again);
 * - not saved             -> deleted from Cloudinary.
 * shortcut: an upload is deleted if it is still unsaved after the minimum age (24 h by default). A form submitted
 * later than that would point at a deleted file; raise the age if clients ever keep uploads around longer.
 */
import { v2 as cloudinary } from "cloudinary";
import { config } from "../../config.js";
import { query } from "../../database.js";
import { repeat } from "../../repeat.js";

export const name = "upload-cleanup";
export const isEnabled = () => config.jobUploadCleanup;

export const PENDING_TAG = "seefix-pending"; // same tag seefix-api signs into every direct upload
// Documents are uploaded with resource_type "auto", so they can land as image, raw or video.
const RESOURCE_TYPES = ["image", "raw", "video"];

export const USED_SQL = `
SELECT "PublicId" AS "publicId" FROM "dbo"."ReportImages" WHERE "PublicId" = ANY($1)
UNION SELECT "PublicId" FROM "dbo"."WorkOrderImages" WHERE "PublicId" = ANY($1)
UNION SELECT "PublicId" FROM "dbo"."ProcurementDocuments" WHERE "PublicId" = ANY($1)`;

function cloudinaryClient() {
  cloudinary.config({
    cloud_name: config.cloudinaryCloudName,
    api_key: config.cloudinaryApiKey,
    api_secret: config.cloudinaryApiSecret,
    secure: true,
  });
  return {
    listPending: (resource_type, next_cursor) =>
      cloudinary.api.resources_by_tag(PENDING_TAG, { resource_type, max_results: 500, next_cursor }),
    removePendingTag: (resource_type, ids) => cloudinary.uploader.remove_tag(PENDING_TAG, ids, { resource_type }),
    destroy: (resource_type, ids) => cloudinary.api.delete_resources(ids, { resource_type, invalidate: true }),
  };
}

const chunks = (list, size) => Array.from({ length: Math.ceil(list.length / size) }, (_, i) => list.slice(i * size, (i + 1) * size));

/** One pass over every pending upload. Returns how many were kept (attached) and deleted (never attached). */
export async function runOnce({ q = query, cld = cloudinaryClient(), log = console, now = Date.now } = {}) {
  const cutoff = now() - config.uploadCleanupMinAgeHours * 3600 * 1000;
  const total = { kept: 0, deleted: 0 };
  for (const resourceType of RESOURCE_TYPES) {
    let cursor;
    do {
      const page = await cld.listPending(resourceType, cursor);
      cursor = page.next_cursor;
      const old = (page.resources || []).filter((r) => Date.parse(r.created_at) <= cutoff).map((r) => r.public_id);
      if (!old.length) continue;

      // Check the database before deleting anything: an attached upload must never be removed.
      const used = new Set((await q(USED_SQL, [old])).rows.map((r) => r.publicId));
      const attached = old.filter((id) => used.has(id));
      const orphans = old.filter((id) => !used.has(id));
      for (const ids of chunks(attached, 1000)) await cld.removePendingTag(resourceType, ids);
      for (const ids of chunks(orphans, 100)) {
        await cld.destroy(resourceType, ids);
        log.log?.(`[${name}] Deleted ${ids.length} unattached ${resourceType} upload(s): ${ids.join(", ")}`);
      }
      total.kept += attached.length;
      total.deleted += orphans.length;
    } while (cursor);
  }
  return total;
}

/** Continuous mode: one pass every UPLOAD_CLEANUP_INTERVAL_MS. Returns stop(). Never throws into the caller. */
export function start({ log = console, q = query, cld } = {}) {
  log.log?.(`[${name}] Checking every ${config.uploadCleanupIntervalMs / 60000} min; deletes unattached uploads older than ${config.uploadCleanupMinAgeHours} h.`);
  const client = cld || cloudinaryClient();
  return repeat(name, config.uploadCleanupIntervalMs, () => runOnce({ q, cld: client, log }), log);
}
