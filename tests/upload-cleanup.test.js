import test from "node:test";
import assert from "node:assert/strict";
import { validateConfig } from "../src/config.js";
import { runOnce, USED_SQL } from "../src/jobs/upload-cleanup/index.js";

const HOUR = 3600 * 1000;
const NOW = Date.parse("2026-10-12T12:00:00Z");
const quiet = { log() {}, warn() {} };
const upload = (id, hoursOld) => ({ public_id: id, created_at: new Date(NOW - hoursOld * HOUR).toISOString() });

function fakeCloudinary(pages) {
  const calls = { removed: [], destroyed: [] };
  return {
    calls,
    listPending: async (type, cursor) => pages[`${type}:${cursor ?? ""}`] ?? { resources: [] },
    removePendingTag: async (type, ids) => void calls.removed.push([type, ...ids]),
    destroy: async (type, ids) => void calls.destroyed.push([type, ...ids]),
  };
}

test("deletes old unattached uploads, untags attached ones and leaves recent ones alone", async () => {
  const cld = fakeCloudinary({
    "image:": { resources: [upload("old-orphan", 30), upload("old-used", 30), upload("recent", 1)], next_cursor: "p2" },
    "image:p2": { resources: [upload("old-orphan-2", 48)] },
    "raw:": { resources: [upload("doc-orphan", 25)] },
  });
  const seen = [];
  const q = async (sql, [ids]) => {
    assert.equal(sql, USED_SQL);
    seen.push(...ids);
    return { rows: ids.filter((id) => id === "old-used").map((publicId) => ({ publicId })) };
  };
  const total = await runOnce({ q, cld, log: quiet, now: () => NOW });

  assert.deepEqual(total, { kept: 1, deleted: 3 });
  assert.ok(!seen.includes("recent"), "uploads younger than the minimum age are not even looked up");
  assert.deepEqual(cld.calls.removed, [["image", "old-used"]]);
  assert.deepEqual(cld.calls.destroyed, [["image", "old-orphan"], ["image", "old-orphan-2"], ["raw", "doc-orphan"]]);
});

test("a database error stops the pass before anything is deleted", async () => {
  const cld = fakeCloudinary({ "image:": { resources: [upload("a", 30)] } });
  const q = async () => { throw new Error("database unreachable"); };
  await assert.rejects(runOnce({ q, cld, log: quiet, now: () => NOW }), /database unreachable/);
  assert.deepEqual(cld.calls.destroyed, []);
});

test("validateConfig requires Cloudinary settings only when upload-cleanup is on", () => {
  const base = { databaseUrl: "postgresql://u@db.invalid/seefixdb", jobUploadCleanup: true };
  assert.throws(() => validateConfig(base), (e) => /CLOUDINARY_CLOUD_NAME/.test(e.message) && /CLOUDINARY_API_SECRET/.test(e.message));
  assert.doesNotThrow(() => validateConfig({ ...base, cloudinaryCloudName: "c", cloudinaryApiKey: "k", cloudinaryApiSecret: "s" }));
});
