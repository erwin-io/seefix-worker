/**
 * Runs pass() now and then every intervalMs until stopped. Never throws into the caller: a failed pass is logged
 * (the first, then at 2, 4, 8… in a row, e.g. while the database is unreachable) and retried next interval.
 * Returns stop().
 */
export function repeat(name, intervalMs, pass, log = console) {
  let stopped = false;
  let timer = null;
  let failures = 0;
  const tick = async () => {
    if (stopped) return;
    try {
      await pass();
      if (failures) log.log?.(`[${name}] Recovered after ${failures} failed pass(es).`);
      failures = 0;
    } catch (error) {
      failures += 1;
      if ((failures & (failures - 1)) === 0) log.warn?.(`[${name}] Pass failed (${failures} in a row): ${error?.message || error}`);
    }
    if (!stopped) timer = setTimeout(tick, intervalMs);
  };
  timer = setTimeout(tick, 0);
  return () => {
    stopped = true;
    clearTimeout(timer);
  };
}
