export async function register() {
  if (process.env.NEXT_RUNTIME === "edge") return

  // Ensure the target database exists and is migrated before anything else
  // tries to connect to it. Safe to call on every boot: it's idempotent.
  const { bootstrapDatabase } = await import("./src/lib/db-bootstrap")
  await bootstrapDatabase()

  const { setupServerErrorLogging } = await import("./src/lib/system-logger")
  const serverMetricsModule = await import("./src/lib/server-metrics")
  setupServerErrorLogging()
  if (
    "startServerMetricsCollector" in serverMetricsModule &&
    typeof serverMetricsModule.startServerMetricsCollector === "function"
  ) {
    serverMetricsModule.startServerMetricsCollector()
  }

  const { isTaskEngineV2Enabled } = await import("./src/lib/task-engine-config")
  // In CLUSTER_MODE we rely exclusively on the external `worker` service,
  // so the embedded in-process worker must stay off — otherwise every app
  // replica would compete for tasks and multiply side effects.
  const clusterMode = /^(true|1|yes|on)$/i.test(process.env.CLUSTER_MODE ?? "")
  if (!isTaskEngineV2Enabled() && !clusterMode) {
    const { startEmbeddedTaskWorker } = await import("./src/lib/embedded-task-worker")
    startEmbeddedTaskWorker()
  }
}
