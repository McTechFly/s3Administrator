export async function register() {
  if (process.env.NEXT_RUNTIME === "edge") return

  const { setupServerErrorLogging } = await import("./lib/system-logger")
  const { startServerMetricsCollector } = await import("./lib/server-metrics")
  const { ensureBackupTaskScheduled } = await import("./lib/backup-scheduler")
  setupServerErrorLogging()
  startServerMetricsCollector()
  ensureBackupTaskScheduled().catch((err) => {
    console.error("[backup-scheduler] failed to schedule backup task", err)
  })
}
