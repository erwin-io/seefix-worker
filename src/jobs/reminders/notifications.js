/**
 * In-app notification + realtime outbox row, written in the caller's transaction. Same SQL and dedupe rules as
 * seefix-api src/services/notifications.js createNotification()/insertOutbox(), which the API's reminders contract
 * (docs/V2_ASSISTANT.md §5) names. Not a verbatim copy: only these two functions are needed here.
 *
 * - Dedupe: Payload.deduplicationKey per user, under a transaction-scoped advisory lock, so a replay creates nothing.
 * - Outbox: `notification.created` for the recipient only (RecipientUserId -> private-user-<id>), ids only.
 */
export async function createNotification(client, { userId, type, title, message, entityType = null, entityId = null, payload = {}, deduplicationKey = null }) {
  const normalized = deduplicationKey ? { ...payload, deduplicationKey } : payload;
  if (deduplicationKey) {
    await client.query(`SELECT pg_advisory_xact_lock(hashtext($1)::bigint)`, [`notification:${userId}:${deduplicationKey}`]);
  }
  const result = await client.query(
    `INSERT INTO "dbo"."Notifications" ("UserId","Type","Title","Message","EntityType","EntityId","Payload","CreatedAt","UpdatedAt")
     SELECT $1,$2,$3,$4,$5,$6,$7::jsonb,NOW(),NOW()
      WHERE $8::text IS NULL
         OR NOT EXISTS (SELECT 1 FROM "dbo"."Notifications" n WHERE n."UserId"=$1 AND n."Payload"->>'deduplicationKey'=$8)
     RETURNING "Id" AS id, "Type" AS type, "EntityType" AS "entityType", "EntityId" AS "entityId"`,
    [userId, type, title, message, entityType, entityId, JSON.stringify(normalized), deduplicationKey],
  );
  const created = result.rows[0] || null;
  if (created) {
    await client.query(
      `INSERT INTO "dbo"."OutboxEvents"
         ("AggregateType","AggregateId","Transport","RecipientUserId","ChannelName","Destination","EventName","Payload","DeduplicationKey")
       VALUES ('NOTIFICATION',$1,'PUSHER',$2,NULL,NULL,'notification.created',$3::jsonb,$4)
       ON CONFLICT ("DeduplicationKey") WHERE "DeduplicationKey" IS NOT NULL DO NOTHING`,
      [
        created.id,
        userId,
        JSON.stringify({ notificationId: created.id, type: created.type, entityType: created.entityType, entityId: created.entityId }),
        `notification:${created.id}:created`,
      ],
    );
  }
  return created;
}

/** Id of the notification already written for (userId, deduplicationKey), or null. */
export async function findNotification(client, userId, deduplicationKey) {
  const r = await client.query(
    `SELECT "Id" FROM "dbo"."Notifications" WHERE "UserId"=$1 AND "Payload"->>'deduplicationKey'=$2 ORDER BY "CreatedAt" LIMIT 1`,
    [userId, deduplicationKey],
  );
  return r.rows[0]?.Id ?? null;
}
