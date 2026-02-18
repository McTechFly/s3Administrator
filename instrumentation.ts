export async function register() {
  if (process.env.NEXT_RUNTIME === "edge") return

  const { setupServerErrorLogging } = await import("./src/lib/system-logger")
  const { startServerMetricsCollector } = await import("./src/lib/server-metrics")
  setupServerErrorLogging()
  startServerMetricsCollector()

  const { isTaskEngineV2Enabled } = await import("./src/lib/task-engine-config")
  if (!isTaskEngineV2Enabled()) {
    const { startEmbeddedTaskWorker } = await import("./src/lib/embedded-task-worker")
    startEmbeddedTaskWorker()
  }
}
