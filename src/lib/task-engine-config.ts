import { isCommunityEdition } from "@/lib/edition"

const community = isCommunityEdition()

function parseBooleanEnv(value: string | undefined, defaultValue: boolean): boolean {
  if (value === undefined) return defaultValue
  const normalized = value.trim().toLowerCase()
  if (normalized === "1" || normalized === "true" || normalized === "yes" || normalized === "on") {
    return true
  }
  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "off") {
    return false
  }
  return defaultValue
}

function parseIntegerEnv(
  value: string | undefined,
  defaultValue: number,
  min: number,
  max: number
): number {
  if (!value) return defaultValue
  const parsed = Number.parseInt(value, 10)
  if (!Number.isFinite(parsed)) return defaultValue
  return Math.min(max, Math.max(min, parsed))
}

export function isTaskEngineV2Enabled(): boolean {
  return parseBooleanEnv(process.env.TASK_ENGINE_V2, false)
}

export function getTaskWorkerConcurrency(): number {
  return parseIntegerEnv(process.env.TASK_WORKER_CONCURRENCY, community ? 24 : 12, 1, 128)
}

export function getTaskMaxActivePerUser(): number {
  return parseIntegerEnv(process.env.TASK_MAX_ACTIVE_PER_USER, community ? 8 : 4, 1, 32)
}

export function getTaskWorkerUserBurst(): number {
  return parseIntegerEnv(process.env.TASK_WORKER_USER_BURST, community ? 16 : 8, 1, 64)
}

export function getTaskWorkerUserBudgetMs(): number {
  return parseIntegerEnv(process.env.TASK_WORKER_USER_BUDGET_MS, community ? 20_000 : 10_000, 1_000, 120_000)
}

export function getTaskWorkerScanIntervalSeconds(): number {
  return parseIntegerEnv(process.env.TASK_WORKER_SCAN_INTERVAL_SECONDS, community ? 5 : 10, 2, 300)
}

export function getTaskMissedScheduleGraceSeconds(): number {
  return parseIntegerEnv(process.env.TASK_MISSED_SCHEDULE_GRACE_SECONDS, 120, 5, 86_400)
}

export function getTaskEventRetentionDays(): number {
  return parseIntegerEnv(process.env.TASK_EVENT_RETENTION_DAYS, 90, 7, 3650)
}

export function getTaskEngineInternalToken(): string {
  return (process.env.TASK_ENGINE_INTERNAL_TOKEN ?? "").trim()
}

export function getTaskWorkerAppUrl(): string {
  return (process.env.TASK_ENGINE_APP_URL ?? "http://app:3000").trim()
}
