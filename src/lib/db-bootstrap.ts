/**
 * Runtime database bootstrap.
 *
 * Runs once at server startup (from `instrumentation.ts`) so operators can
 * point the app at an empty PostgreSQL instance and have everything —
 * database, schema, indexes — be created automatically, and kept in sync
 * after upgrades.
 *
 * Steps:
 *   1. Ensure the target database exists (connect to the cluster's
 *      management DB and CREATE DATABASE if needed).
 *   2. Run `prisma migrate deploy` to apply all pending migrations.
 *
 * Controlled by env vars:
 *   - AUTO_MIGRATE=false      → skip everything.
 *   - AUTO_CREATE_DATABASE=false → skip step 1 (assume DB already exists).
 */

import { spawn } from "node:child_process"
import path from "node:path"
import { Client } from "pg"

let bootstrapped = false

function parseBool(value: string | undefined, defaultValue: boolean): boolean {
  if (value === undefined) return defaultValue
  const v = value.trim().toLowerCase()
  if (v === "false" || v === "0" || v === "no" || v === "off") return false
  if (v === "true" || v === "1" || v === "yes" || v === "on") return true
  return defaultValue
}

async function ensureDatabaseExists(databaseUrl: string): Promise<void> {
  let url: URL
  try {
    url = new URL(databaseUrl)
  } catch {
    console.warn("[db-bootstrap] DATABASE_URL is not a valid URL; skipping database-exists check.")
    return
  }

  const targetDb = decodeURIComponent((url.pathname || "/").replace(/^\//, ""))
  if (!targetDb) {
    console.warn("[db-bootstrap] DATABASE_URL has no database name; skipping.")
    return
  }

  // First try to connect directly to the target DB. If it works, we're done.
  const probe = new Client({ connectionString: databaseUrl })
  try {
    await probe.connect()
    await probe.end().catch(() => undefined)
    return
  } catch (error) {
    const code = (error as { code?: string } | undefined)?.code
    await probe.end().catch(() => undefined)
    // 3D000 = invalid_catalog_name (database does not exist). Anything else
    // is a real connection / auth problem that we cannot fix here.
    if (code !== "3D000") {
      throw error
    }
  }

  // Reconnect to the cluster management DB and CREATE DATABASE.
  const managementUrl = new URL(databaseUrl)
  managementUrl.pathname = "/postgres"
  const admin = new Client({ connectionString: managementUrl.toString() })
  try {
    await admin.connect()
    // pg does not allow parameterized CREATE DATABASE; identifier must be quoted.
    const quoted = `"${targetDb.replace(/"/g, '""')}"`
    console.log(`[db-bootstrap] Creating database ${targetDb}…`)
    await admin.query(`CREATE DATABASE ${quoted}`)
  } catch (error) {
    const code = (error as { code?: string } | undefined)?.code
    // 42P04 = duplicate_database (race with another starting instance). Ignore.
    if (code !== "42P04") {
      throw error
    }
  } finally {
    await admin.end().catch(() => undefined)
  }
}

function runPrismaMigrateDeploy(): Promise<void> {
  return new Promise((resolve, reject) => {
    // Resolve prisma CLI from node_modules/.bin so it works inside the
    // standalone Next.js server bundle as long as prisma is installed.
    const prismaBin = path.join(process.cwd(), "node_modules", ".bin", "prisma")
    const child = spawn(prismaBin, ["migrate", "deploy"], {
      env: process.env,
      stdio: "inherit",
    })
    child.once("error", reject)
    child.once("exit", (code) => {
      if (code === 0) resolve()
      else reject(new Error(`prisma migrate deploy exited with code ${code}`))
    })
  })
}

// A stable 64-bit integer used as the key for pg_advisory_lock. Any constant
// works — two high random ints folded together. Having ALL replicas use the
// same pair guarantees that only one instance runs migrations at a time.
const MIGRATION_LOCK_KEY_1 = 8127491
const MIGRATION_LOCK_KEY_2 = 554123

async function withMigrationLock(
  databaseUrl: string,
  fn: () => Promise<void>
): Promise<void> {
  const client = new Client({ connectionString: databaseUrl })
  await client.connect()
  try {
    console.log("[db-bootstrap] Acquiring migration advisory lock…")
    await client.query("SELECT pg_advisory_lock($1, $2)", [
      MIGRATION_LOCK_KEY_1,
      MIGRATION_LOCK_KEY_2,
    ])
    try {
      await fn()
    } finally {
      await client
        .query("SELECT pg_advisory_unlock($1, $2)", [
          MIGRATION_LOCK_KEY_1,
          MIGRATION_LOCK_KEY_2,
        ])
        .catch(() => undefined)
    }
  } finally {
    await client.end().catch(() => undefined)
  }
}

export async function bootstrapDatabase(): Promise<void> {
  if (bootstrapped) return
  bootstrapped = true

  if (!parseBool(process.env.AUTO_MIGRATE, true)) {
    console.log("[db-bootstrap] AUTO_MIGRATE=false, skipping automatic database setup.")
    return
  }

  const databaseUrl = process.env.DATABASE_URL
  if (!databaseUrl) {
    console.warn("[db-bootstrap] DATABASE_URL not set; skipping bootstrap.")
    return
  }

  try {
    if (parseBool(process.env.AUTO_CREATE_DATABASE, true)) {
      await ensureDatabaseExists(databaseUrl)
    }
    // Serialize migrations across all replicas via a Postgres advisory lock.
    // The first instance to boot applies pending migrations; others block on
    // the lock, then see an up-to-date schema and no-op through deploy.
    await withMigrationLock(databaseUrl, async () => {
      console.log("[db-bootstrap] Applying Prisma migrations…")
      await runPrismaMigrateDeploy()
    })
    console.log("[db-bootstrap] Database is up to date.")
  } catch (error) {
    // Log prominently but do not crash the process — operator may want the
    // app to start and inspect logs. They can set AUTO_MIGRATE=false and
    // run migrations manually.
    console.error("[db-bootstrap] Failed to bootstrap database:", error)
  }
}
