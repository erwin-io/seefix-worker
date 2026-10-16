import Pusher from "pusher";
import { config } from "./config.js";

let client = null;

/** Publishes one event to Pusher channels. Rejects on HTTP/network failure or after OUTBOX_PUBLISH_TIMEOUT_MS. */
export async function publish(channels, event, data) {
  client ??= new Pusher({
    appId: config.pusherAppId,
    key: config.pusherKey,
    secret: config.pusherSecret,
    cluster: config.pusherCluster,
    useTLS: true,
    timeout: config.outboxPublishTimeoutMs,
  });
  await client.trigger(channels, event, data);
}
