import parser from "cron-parser"

export const MIN_TASK_SCHEDULE_INTERVAL_SECONDS = 60 * 60
const MAX_TASK_CRON_LENGTH = 120

export interface TaskSchedulePayload {
  cron: string
}

export interface ResolvedTaskSchedule {
  enabled: boolean
  cron: string | null
  legacyIntervalSeconds: number | null
}

export class TaskScheduleValidationError extends Error {}

function formatIntervalLabel(seconds: number): string {
  const normalized = Math.max(1, Math.floor(seconds))
  if (normalized % 3600 === 0) {
    const hours = normalized / 3600
    return hours === 1 ? "1 hour" : `${hours} hours`
  }
  if (normalized % 60 === 0) {
    const minutes = normalized / 60
    return minutes === 1 ? "1 minute" : `${minutes} minutes`
  }
  return normalized === 1 ? "1 second" : `${normalized} seconds`
}

function normalizeWhitespace(value: string): string {
  return value.trim().replace(/\s+/g, " ")
}

function parseCronExpression(cron: string, currentDate: Date) {
  return parser.parseExpression(cron, {
    currentDate,
    tz: "UTC",
  })
}

export function normalizeTaskScheduleCron(raw: string): string {
  const normalized = normalizeWhitespace(raw)
  if (!normalized) {
    throw new TaskScheduleValidationError("Schedule cron is required")
  }
  if (normalized.length > MAX_TASK_CRON_LENGTH) {
    throw new TaskScheduleValidationError("Schedule cron is too long")
  }
  return normalized
}

export function assertValidTaskScheduleCron(
  raw: string,
  minIntervalSeconds = MIN_TASK_SCHEDULE_INTERVAL_SECONDS
): string {
  const cron = normalizeTaskScheduleCron(raw)

  try {
    const probe = parseCronExpression(cron, new Date("2026-01-01T00:00:00.000Z"))
    let previous = probe.next().toDate()
    for (let i = 0; i < 32; i++) {
      const next = probe.next().toDate()
      const diffSeconds = Math.floor((next.getTime() - previous.getTime()) / 1000)
      if (diffSeconds < minIntervalSeconds) {
        throw new TaskScheduleValidationError(
          `Schedule is too frequent. Minimum interval is ${formatIntervalLabel(minIntervalSeconds)}`
        )
      }
      previous = next
    }
  } catch (error) {
    if (error instanceof TaskScheduleValidationError) {
      throw error
    }
    throw new TaskScheduleValidationError("Invalid cron expression")
  }

  return cron
}

export function nextRunAtFromCron(cron: string, from: Date): Date {
  const expression = parseCronExpression(cron, from)
  return expression.next().toDate()
}

export function getUpcomingRunDatesFromCron(
  cron: string,
  nextRunAt: Date,
  count = 3
): string[] {
  if (count <= 0) return []

  try {
    const result: string[] = [nextRunAt.toISOString()]
    let cursor = nextRunAt
    for (let i = 1; i < count; i++) {
      cursor = nextRunAtFromCron(cron, cursor)
      result.push(cursor.toISOString())
    }
    return result
  } catch {
    return []
  }
}

export function resolveTaskSchedule(schedule: {
  isRecurring: boolean
  scheduleCron?: string | null
  scheduleIntervalSeconds?: number | null
}): ResolvedTaskSchedule {
  if (!schedule.isRecurring) {
    return {
      enabled: false,
      cron: null,
      legacyIntervalSeconds: null,
    }
  }

  const cronCandidate =
    typeof schedule.scheduleCron === "string" && schedule.scheduleCron.trim().length > 0
      ? normalizeWhitespace(schedule.scheduleCron)
      : null
  if (cronCandidate) {
    try {
      const cron = assertValidTaskScheduleCron(cronCandidate)
      return {
        enabled: true,
        cron,
        legacyIntervalSeconds: null,
      }
    } catch {
      return {
        enabled: false,
        cron: null,
        legacyIntervalSeconds: null,
      }
    }
  }

  const legacyIntervalSeconds =
    typeof schedule.scheduleIntervalSeconds === "number" &&
    Number.isFinite(schedule.scheduleIntervalSeconds) &&
    schedule.scheduleIntervalSeconds >= MIN_TASK_SCHEDULE_INTERVAL_SECONDS
      ? Math.floor(schedule.scheduleIntervalSeconds)
      : null

  return {
    enabled: legacyIntervalSeconds !== null,
    cron: null,
    legacyIntervalSeconds,
  }
}

export function nextRunAtForTaskSchedule(
  schedule: ResolvedTaskSchedule,
  from: Date
): Date | null {
  if (!schedule.enabled) return null
  if (schedule.cron) {
    try {
      return nextRunAtFromCron(schedule.cron, from)
    } catch {
      return null
    }
  }
  if (schedule.legacyIntervalSeconds && schedule.legacyIntervalSeconds > 0) {
    return new Date(from.getTime() + schedule.legacyIntervalSeconds * 1000)
  }
  return null
}

export function getEffectiveNextRunAtForTask(params: {
  isRecurring: boolean
  scheduleCron?: string | null
  scheduleIntervalSeconds?: number | null
  nextRunAt: Date
}, now: Date = new Date()): Date {
  const schedule = resolveTaskSchedule(params)
  if (!schedule.enabled) {
    return params.nextRunAt
  }
  // Keep UI/server responses aligned to current server time for recurring tasks,
  // even if persisted nextRunAt drifted due to downtime/older state transitions.
  return nextRunAtForTaskSchedule(schedule, now) ?? params.nextRunAt
}
