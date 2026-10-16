/**
 * Which cache domains each role may hear about. Must equal seefix-api src/routes/sync.js DOMAINS_BY_ROLE (the filter
 * of GET /api/sync/versions and of /api/realtime/auth for role channels); `npm run sync:check` compares the two.
 */
export const DOMAINS_BY_ROLE = {
  ADMIN: ["reports", "work_orders", "procurement", "action_items", "users", "reference"],
  MAINTENANCE_SUPERVISOR: ["reports", "work_orders", "procurement", "action_items", "users", "reference"],
  MAINTENANCE_STAFF: ["reports", "work_orders", "procurement", "action_items", "users", "reference"],
  PROCUREMENT: ["procurement", "reports", "reference"],
  WORKER: ["work_orders", "reports", "reference"],
};

/** private-role-<ROLE>, authorized by seefix-api /api/realtime/auth for that role only. */
export const roleChannel = (role) => `private-role-${role}`;
