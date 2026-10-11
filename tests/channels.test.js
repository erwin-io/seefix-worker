import test from "node:test";
import assert from "node:assert/strict";
import { canSubscribe, parseChannel, resolveChannels } from "../src/jobs/realtime-dispatch/channels.js";

const R = "11111111-1111-4111-8111-111111111111";
const U = "22222222-2222-4222-8222-222222222222";
const W = "33333333-3333-4333-8333-333333333333";
const H = "44444444-4444-4444-8444-444444444444";

/** Fake query: answers "has access" lookups from a set of [sqlFragment, ...params] facts. */
function fakeQ(facts = []) {
  return async (sql, params) => {
    const hit = facts.find(([frag, ...p]) => sql.includes(frag) && p.every((v, i) => params[i] === v));
    if (sql.includes('FROM "dbo"."MaintenanceRequests" WHERE "Id"')) return { rows: hit ? [{ ReportId: R }] : [], rowCount: hit ? 1 : 0 };
    if (sql.includes('FROM "dbo"."ProcurementClarifications"')) return { rows: hit ? [{ ProcurementHandoffId: H }] : [], rowCount: hit ? 1 : 0 };
    return { rows: hit ? [{}] : [], rowCount: hit ? 1 : 0 };
  };
}

test("parseChannel accepts only the four private UUID channel types", () => {
  assert.deepEqual(parseChannel(`private-report-${R}`), { type: "report", id: R });
  assert.deepEqual(parseChannel(`private-work-order-${W}`), { type: "work-order", id: W });
  for (const bad of [`report-${R}`, `presence-user-${U}`, `private-user-${U}x`, `private-role-ADMIN`, `private-user-*`, "", null]) {
    assert.equal(parseChannel(bad), null, String(bad));
  }
});

test("resolveChannels routes outbox rows server-side and never broadcasts unknown rows", async () => {
  const q = fakeQ([['"MaintenanceRequests"', "mr"], ['"ProcurementClarifications"', "pc"]]);
  assert.deepEqual(await resolveChannels({ RecipientUserId: U, AggregateType: "NOTIFICATION" }, q), [`private-user-${U}`]);
  assert.deepEqual(await resolveChannels({ ChannelName: `report-${R}`, AggregateType: "MAINTENANCE_REQUEST", AggregateId: "mr" }, q), [`private-report-${R}`]);
  assert.deepEqual(await resolveChannels({ ChannelName: `work-order-${W}`, AggregateType: "WORK_ORDER", AggregateId: W }, q), [`private-work-order-${W}`]);
  assert.deepEqual(await resolveChannels({ AggregateType: "WORK_ORDER", AggregateId: W }, q), [`private-work-order-${W}`]);
  assert.deepEqual(await resolveChannels({ AggregateType: "MAINTENANCE_REQUEST", AggregateId: "mr" }, q), [`private-report-${R}`]);
  assert.deepEqual(await resolveChannels({ AggregateType: "PROCUREMENT_CLARIFICATION", AggregateId: "pc" }, q), [`private-handoff-${H}`]);
  assert.deepEqual(await resolveChannels({ AggregateType: "PROCUREMENT_CLARIFICATION", AggregateId: "missing" }, q), []);
  assert.deepEqual(await resolveChannels({ AggregateType: "SOMETHING_ELSE", AggregateId: R, ChannelName: "public-anything" }, q), []);
});

test("canSubscribe: user channel is self-only", async () => {
  const q = fakeQ();
  assert.equal(await canSubscribe({ id: U, role: "WORKER" }, `private-user-${U}`, q), true);
  assert.equal(await canSubscribe({ id: U, role: "ADMIN" }, `private-user-${W}`, q), false);
  assert.equal(await canSubscribe(null, `private-user-${U}`, q), false);
});

// ---- canSubscribe vs REST read rules, line by line -------------------------------------------
// In-memory records; the fake query answers exactly the lookups canSubscribe makes and throws on
// anything else, so an unexpected query (or a missing lookup) fails the test.
const id = (n) => `${String(n).repeat(8)}-0000-4000-8000-${String(n).repeat(12)}`;
const U_ADMIN = id(1), U_SUP = id(2), U_STAFF = id(3), U_PROC = id(4), U_WRK = id(5), U_WRK2 = id(6), U_REP = id(7), U_REP2 = id(8), U_GUEST = id(9);
const R_LINKED = id("a"), R_PLAIN = id("b"), WO = id("c"), HO = id("d"), MISSING = id("e");
const DB = {
  Reports: [{ Id: R_LINKED, ReporterId: U_REP }, { Id: R_PLAIN, ReporterId: U_REP2 }],
  WorkOrders: [{ Id: WO, ReportId: R_LINKED, ResponsibleLeadUserId: U_WRK }],
  ProcurementHandoffs: [{ Id: HO, ReportId: R_LINKED }],
};
async function dbQ(sql, [a, b]) {
  const rows = (list) => ({ rows: list, rowCount: list.length });
  let m;
  if ((m = /^SELECT 1 FROM "dbo"\."(\w+)" WHERE "Id"=\$1$/.exec(sql))) return rows(DB[m[1]].filter((x) => x.Id === a));
  if (sql.includes('"Reports" WHERE "Id"=$1 AND "ReporterId"=$2')) return rows(DB.Reports.filter((x) => x.Id === a && x.ReporterId === b));
  if (sql.includes('"WorkOrders" WHERE "Id"=$1 AND "ResponsibleLeadUserId"=$2')) return rows(DB.WorkOrders.filter((x) => x.Id === a && x.ResponsibleLeadUserId === b));
  if (sql.includes('"WorkOrders" WHERE "ReportId"=$1 AND "ResponsibleLeadUserId"=$2')) return rows(DB.WorkOrders.filter((x) => x.ReportId === a && x.ResponsibleLeadUserId === b));
  if (sql.includes('"ProcurementHandoffs" ph JOIN "dbo"."MaintenanceRequests"')) return rows(DB.ProcurementHandoffs.filter((x) => x.ReportId === a));
  throw new Error(`unexpected query: ${sql}`);
}
const USERS = {
  ADMIN: { id: U_ADMIN, role: "ADMIN" },
  SUPERVISOR: { id: U_SUP, role: "MAINTENANCE_SUPERVISOR" },
  STAFF: { id: U_STAFF, role: "MAINTENANCE_STAFF" },
  PROCUREMENT: { id: U_PROC, role: "PROCUREMENT" },
  "WORKER (lead)": { id: U_WRK, role: "WORKER" },
  "WORKER (unrelated)": { id: U_WRK2, role: "WORKER" },
  "REPORTER (owner)": { id: U_REP, role: "REPORTER" },
  "REPORTER (other)": { id: U_REP2, role: "REPORTER" },
  "unknown role": { id: U_GUEST, role: "GUEST" },
};
// Expected = REST GET result (200 -> 1, 403/404 -> 0), written from the route code:
//   reports/:id   getReportDetail: 404 missing; staff roles; REPORTER owner; PROCUREMENT linked handoff; WORKER lead of linked WO
//   work-orders/:id  WORK_ROLES + assertWorkAccess: 404 missing; WORKER only as responsible lead
//   procurement/handoffs/:id  ACCESS = PROCUREMENT + staff roles; 404 missing
// Columns: report linked | report plain | report missing | work-order | wo missing | handoff | handoff missing
const EXPECTED = {
  ADMIN:                  [1, 1, 0, 1, 0, 1, 0],
  SUPERVISOR:             [1, 1, 0, 1, 0, 1, 0],
  STAFF:                  [1, 1, 0, 1, 0, 1, 0],
  PROCUREMENT:            [1, 0, 0, 0, 0, 1, 0],
  "WORKER (lead)":        [1, 0, 0, 1, 0, 0, 0],
  "WORKER (unrelated)":   [0, 0, 0, 0, 0, 0, 0],
  "REPORTER (owner)":     [1, 0, 0, 0, 0, 0, 0],
  "REPORTER (other)":     [0, 1, 0, 0, 0, 0, 0],
  "unknown role":         [0, 0, 0, 0, 0, 0, 0],
};
const CHANNELS = [`private-report-${R_LINKED}`, `private-report-${R_PLAIN}`, `private-report-${MISSING}`, `private-work-order-${WO}`, `private-work-order-${MISSING}`, `private-handoff-${HO}`, `private-handoff-${MISSING}`];

test("canSubscribe matches the REST read rules for every role x channel (incl. nonexistent records)", async () => {
  const actual = {};
  for (const [name, user] of Object.entries(USERS)) {
    actual[name] = [];
    for (const ch of CHANNELS) actual[name].push((await canSubscribe(user, ch, dbQ)) ? 1 : 0);
  }
  assert.deepEqual(actual, EXPECTED);
});

test("canSubscribe: user channels are self-only for every role; malformed names are rejected without a query", async () => {
  for (const user of Object.values(USERS)) {
    assert.equal(await canSubscribe(user, `private-user-${user.id}`, dbQ), true, `${user.role} own`);
    assert.equal(await canSubscribe(user, `private-user-${user.id.toUpperCase()}`, dbQ), true, "case-insensitive uuid");
    assert.equal(await canSubscribe(user, `private-user-${U_REP2 === user.id ? U_REP : U_REP2}`, dbQ), false, `${user.role} other`);
  }
  const noQuery = async () => assert.fail("must not query for a malformed channel");
  const admin = USERS.ADMIN;
  for (const bad of [`report-${R_LINKED}`, `presence-report-${R_LINKED}`, `private-report-${R_LINKED}-x`, `private-report-not-a-uuid`, `private-role-ADMIN`, `private-encrypted-user-${U_ADMIN}`, `private-report-${R_LINKED};drop`, "", null, undefined, 42]) {
    assert.equal(await canSubscribe(admin, bad, noQuery), false, String(bad));
  }
  assert.equal(await canSubscribe(null, `private-user-${U_ADMIN}`, noQuery), false, "no user");
  assert.equal(await canSubscribe({ role: "ADMIN" }, `private-report-${R_LINKED}`, noQuery), false, "user without id");
});

test("every routable outbox row is published only to private channels", async () => {
  const q = async (sql) => ({ rows: sql.includes("MaintenanceRequests") ? [{ ReportId: R_LINKED }] : [{ ProcurementHandoffId: HO }], rowCount: 1 });
  const rows = [
    { RecipientUserId: U_REP, AggregateType: "NOTIFICATION", ChannelName: "public-feed" },
    { AggregateType: "REPORT", AggregateId: R_LINKED },
    { AggregateType: "WORK_ORDER", AggregateId: WO },
    { AggregateType: "PROCUREMENT_HANDOFF", AggregateId: HO },
    { AggregateType: "MAINTENANCE_REQUEST", AggregateId: id("f") },
    { AggregateType: "PROCUREMENT_CLARIFICATION", AggregateId: id("f") },
    { ChannelName: `report-${R_LINKED}`, AggregateType: "REPORT", AggregateId: R_LINKED },
    { ChannelName: `work-order-${WO}`, AggregateType: "WORK_ORDER", AggregateId: WO },
  ];
  for (const row of rows) {
    const channels = await resolveChannels(row, q);
    assert.equal(channels.length, 1, JSON.stringify(row));
    assert.ok(parseChannel(channels[0]), `${channels[0]} must be a private channel`);
  }
});

test("legacy ChannelName never overrides the aggregate identity: mismatches fail closed", async () => {
  // MaintenanceRequest "mr" belongs to report R (database parent).
  const q = fakeQ([['"MaintenanceRequests"', "mr"], ['"ProcurementClarifications"', "pc"]]);
  const OTHER = "55555555-5555-4555-8555-555555555555";
  const cases = [
    // [row, expected]
    [{ AggregateType: "WORK_ORDER", AggregateId: W, ChannelName: `report-${OTHER}` }, []], // work order payload to another report
    [{ AggregateType: "WORK_ORDER", AggregateId: W, ChannelName: `work-order-${OTHER}` }, []], // other work order
    [{ AggregateType: "REPORT", AggregateId: R, ChannelName: `report-${OTHER}` }, []], // other report
    [{ AggregateType: "REPORT", AggregateId: R, ChannelName: `work-order-${R}` }, []], // wrong channel type
    [{ AggregateType: "MAINTENANCE_REQUEST", AggregateId: "mr", ChannelName: `report-${OTHER}` }, []], // child with wrong parent
    [{ AggregateType: "MAINTENANCE_REQUEST", AggregateId: "missing", ChannelName: `report-${R}` }, []], // parent cannot be verified
    [{ AggregateType: "SOMETHING_ELSE", AggregateId: R, ChannelName: `report-${R}` }, []], // legacy name alone is never enough
    [{ AggregateType: "REPORT", AggregateId: R, ChannelName: "public-feed" }, []], // unrecognised name: fail closed
    // Valid seefix-agents legacy rows keep working.
    [{ AggregateType: "REPORT", AggregateId: R, ChannelName: `report-${R}` }, [`private-report-${R}`]],
    [{ AggregateType: "REPORT", AggregateId: R, ChannelName: `REPORT-${R.toUpperCase()}` }, [`private-report-${R}`]],
    [{ AggregateType: "WORK_ORDER", AggregateId: W, ChannelName: `work-order-${W}` }, [`private-work-order-${W}`]],
    [{ AggregateType: "MAINTENANCE_REQUEST", AggregateId: "mr", ChannelName: `report-${R}` }, [`private-report-${R}`]],
    [{ AggregateType: "WORK_ORDER", AggregateId: W, ChannelName: null }, [`private-work-order-${W}`]],
  ];
  for (const [row, expected] of cases) assert.deepEqual(await resolveChannels(row, q), expected, JSON.stringify(row));
});
