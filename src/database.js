import fs from "node:fs";
import pg from "pg";
import { config } from "./config.js";

// Same SSL handling as seefix-api; an sslmode inside DATABASE_URL takes precedence (node-postgres behaviour).
function sslOptions() {
  const mode = config.databaseSslMode;
  if (["disable", "allow", "prefer"].includes(mode)) return false;
  if (mode === "require") return { rejectUnauthorized: false };
  const ssl = { rejectUnauthorized: true };
  if (config.databaseSslRootCert) ssl.ca = fs.readFileSync(config.databaseSslRootCert, "utf8");
  return ssl;
}

export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: config.databasePoolMax,
  connectionTimeoutMillis: config.databaseConnectTimeoutMs,
  application_name: "seefix-worker",
  ssl: sslOptions(),
});

pool.on("error", (error) => console.error("[DB POOL] Unexpected idle-client error:", error.message));

export const query = (text, params = []) => pool.query(text, params);
