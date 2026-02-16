import { Prisma } from "@prisma/client"
import { prisma } from "@/lib/db"
import { deleteExpiredTaskEvents } from "@/lib/task-run-history"
import {
  getTaskEventRetentionDays,
  getTaskMaxActivePerUser,
  getTaskWorkerConcurrency,
  getTaskWorkerScanIntervalSeconds,
  getTaskWorkerUserBudgetMs,
  getTaskWorkerUserBurst,
} from "@/lib/task-engine-config"

type BossJob<T = unknown> = {
  data?: T
}

type BossLike = {
  start: () => Promise<void>
  stop: () => Promise<void>
  createQueue: (name: string) => Promise<void>
  schedule: (name: string, cron: string, data?: unknown, options?: Record<string, unknown>) => Promise<unknown>
  send: (name: string, data?: unknown, options?: Record<string, unknown>) => Promise<unknown>
  work: (
    name: string,
    options: Record<string, unknown>,
    handler: (job: BossJob) => Promise<void>
  ) => Promise<unknown>
}

type BossConstructor = new (options: { connectionString: string; schema?: string }) => BossLike

export const TASK_DISPATCH_SCAN_QUEUE = "task-dispatch-scan"
export const TASK_USER_DISPATCH_QUEUE = "task-user-dispatch"
export const TASK_MAINTENANCE_QUEUE = "task-maintenance"

export interface ProcessUserResult {
  processed: boolean
  taskId?: string
}

export interface TaskQueueWorkerOptions {
  workerId: string
  processUserOnce: (userId: string) => Promise<ProcessUserResult>
  enabled: boolean
}

function extractUserIdFromJobData(data: unknown): string | null {
  const parseStructured = (value: unknown): unknown => {
    if (typeof value === "string") {
      try {
        return JSON.parse(value)
      } catch {
        return null
      }
    }
    if (value && typeof Buffer !== "undefined" && Buffer.isBuffer(value)) {
      try {
        return JSON.parse(value.toString("utf8"))
      } catch {
        return null
      }
    }
    return value
  }

  const readUserId = (value: unknown): string | null => {
    const payload = parseStructured(value)
    if (!payload || typeof payload !== "object") {
      return null
    }

    const directUserId = (payload as { userId?: unknown }).userId
    if (typeof directUserId === "string" && directUserId.trim()) {
      return directUserId.trim()
    }

    if ("data" in payload) {
      return readUserId((payload as { data?: unknown }).data)
    }

    return null
  }

  return readUserId(data)
}

function createCronEveryNSeconds(seconds: number): string {
  const clamped = Math.min(59, Math.max(2, Math.floor(seconds)))
  return `*/${clamped} * * * * *`
}

async function loadPgBossConstructor(): Promise<BossConstructor | null> {
  try {
    const dynamicImport = new Function(
      "specifier",
      "return import(specifier)"
    ) as (specifier: string) => Promise<unknown>
    const importedModule = await dynamicImport("pg-boss") as {
      default?: BossConstructor
    }
    return importedModule.default ?? null
  } catch {
    return null
  }
}

async function findDueUsers(limitUsers: number): Promise<string[]> {
  const maxSlots = getTaskMaxActivePerUser()
  const rows = await prisma.$queryRaw<Array<{ userId: string; availableSlots: number }>>(Prisma.sql`
    WITH due AS (
      SELECT
        t."userId" AS "userId",
        COUNT(*)::int AS "dueCount",
        MIN(t."createdAt") AS "oldestCreatedAt",
        MIN(t."nextRunAt") AS "oldestNextRunAt"
      FROM "BackgroundTask" t
      WHERE t."lifecycleState" = 'active'
        AND t."status" IN ('pending', 'in_progress')
        AND t."type" IN ('bulk_delete', 'thumbnail_generate', 'object_transfer')
        AND t."nextRunAt" <= NOW()
      GROUP BY t."userId"
    ),
    locked AS (
      SELECT
        t."userId" AS "userId",
        COUNT(*)::int AS "lockedCount"
      FROM "BackgroundTask" t
      WHERE t."lifecycleState" = 'active'
        AND t."status" = 'in_progress'
        AND t."nextRunAt" > NOW()
      GROUP BY t."userId"
    )
    SELECT
      d."userId" AS "userId",
      GREATEST(
        0,
        LEAST(${maxSlots}, d."dueCount") - COALESCE(l."lockedCount", 0)
      )::int AS "availableSlots"
    FROM due d
    LEFT JOIN locked l
      ON l."userId" = d."userId"
    ORDER BY d."oldestCreatedAt" ASC, d."oldestNextRunAt" ASC
    LIMIT ${limitUsers}
  `)

  const users: string[] = []
  for (const row of rows) {
    if (typeof row.userId !== "string" || !row.userId.trim()) {
      continue
    }
    const slots = Number.isFinite(row.availableSlots)
      ? Math.max(0, Math.floor(row.availableSlots))
      : 0
    for (let i = 0; i < slots; i++) {
      users.push(row.userId)
    }
  }

  return users
}

async function processUserBurst(
  userId: string,
  processUserOnce: (userId: string) => Promise<ProcessUserResult>
): Promise<number> {
  const budgetMs = getTaskWorkerUserBudgetMs()
  const maxBurst = getTaskWorkerUserBurst()
  const startedAt = Date.now()
  let processed = 0

  for (let i = 0; i < maxBurst; i++) {
    if (Date.now() - startedAt >= budgetMs) {
      break
    }

    const result = await processUserOnce(userId)
    if (!result.processed) {
      break
    }
    processed += 1
  }

  return processed
}

async function runMaintenance(workerId: string): Promise<void> {
  const deleted = await deleteExpiredTaskEvents(getTaskEventRetentionDays())
  if (deleted > 0) {
    console.info(`[task-worker:${workerId}] pruned ${deleted} expired task events`)
  }
}

async function runFallbackPollingWorker(options: TaskQueueWorkerOptions) {
  const scanIntervalMs = getTaskWorkerScanIntervalSeconds() * 1000
  const concurrency = getTaskWorkerConcurrency()

  let stopped = false
  let nextMaintenanceAt = Date.now() + 24 * 60 * 60 * 1000

  const tick = async () => {
    if (stopped) return

    try {
      if (Date.now() >= nextMaintenanceAt) {
        nextMaintenanceAt = Date.now() + 24 * 60 * 60 * 1000
        await runMaintenance(options.workerId)
      }

      const dueUsers = await findDueUsers(Math.max(64, concurrency * 8))
      if (dueUsers.length === 0) {
        return
      }

      let cursor = 0
      const workers = Array.from({ length: concurrency }, async () => {
        while (!stopped) {
          const current = dueUsers[cursor]
          cursor += 1
          if (!current) break
          try {
            await processUserBurst(current, options.processUserOnce)
          } catch (error) {
            console.error(`[task-worker:${options.workerId}] user burst failed`, {
              userId: current,
              error,
            })
          }
        }
      })
      await Promise.all(workers)
    } catch (error) {
      console.error(`[task-worker:${options.workerId}] fallback scan failed`, error)
    }
  }

  const interval = setInterval(() => {
    void tick()
  }, scanIntervalMs)
  // Keep the process alive in fallback mode; otherwise Docker restart loops.
  await tick()

  return async () => {
    stopped = true
    clearInterval(interval)
  }
}

async function runPgBossWorker(
  options: TaskQueueWorkerOptions,
  Boss: BossConstructor
) {
  const databaseUrl = process.env.DATABASE_URL
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for task queue worker")
  }

  const boss = new Boss({
    connectionString: databaseUrl,
    schema: "public",
  })
  await boss.start()
  await boss.createQueue(TASK_DISPATCH_SCAN_QUEUE)
  await boss.createQueue(TASK_USER_DISPATCH_QUEUE)
  await boss.createQueue(TASK_MAINTENANCE_QUEUE)

  const cron = createCronEveryNSeconds(getTaskWorkerScanIntervalSeconds())
  await boss.schedule(
    TASK_DISPATCH_SCAN_QUEUE,
    cron,
    {},
    { singletonKey: "global-dispatch-scan" }
  )
  await boss.schedule(
    TASK_MAINTENANCE_QUEUE,
    "0 0 3 * * *",
    {},
    { singletonKey: "task-maintenance-daily" }
  )

  await boss.work(TASK_DISPATCH_SCAN_QUEUE, { teamSize: 1 }, async () => {
    const dueUsers = await findDueUsers(Math.max(64, getTaskWorkerConcurrency() * 8))
    const slotByUser = new Map<string, number>()
    for (const userId of dueUsers) {
      const slotIndex = slotByUser.get(userId) ?? 0
      slotByUser.set(userId, slotIndex + 1)
      await boss.send(
        TASK_USER_DISPATCH_QUEUE,
        { userId, slotIndex },
        {
          singletonKey: `user:${userId}:slot:${slotIndex}`,
          singletonSeconds: 30,
          retryLimit: 0,
        }
      )
    }
  })

  await boss.work(
    TASK_USER_DISPATCH_QUEUE,
    {
      teamSize: getTaskWorkerConcurrency(),
      retryLimit: 0,
    },
    async (job) => {
      const jobs = Array.isArray(job) ? job : [job]
      for (const current of jobs) {
        const rawData =
          current && typeof current === "object" && "data" in current
            ? (current as { data?: unknown }).data
            : current
        const userId = extractUserIdFromJobData(rawData)
        if (!userId) {
          console.warn(`[task-worker:${options.workerId}] skipped dispatch job with invalid payload`)
          continue
        }
        await processUserBurst(userId, options.processUserOnce)
      }
    }
  )

  await boss.work(TASK_MAINTENANCE_QUEUE, { teamSize: 1 }, async () => {
    await runMaintenance(options.workerId)
  })

  return async () => {
    await boss.stop()
  }
}

export async function startTaskQueueWorker(options: TaskQueueWorkerOptions) {
  if (!options.enabled) {
    return async () => {}
  }

  const Boss = await loadPgBossConstructor()
  if (!Boss) {
    console.warn("[task-worker] pg-boss unavailable; using fallback DB polling scheduler")
    return runFallbackPollingWorker(options)
  }

  try {
    return await runPgBossWorker(options, Boss)
  } catch (error) {
    console.error("[task-worker] pg-boss scheduler failed, falling back to DB polling", error)
    return runFallbackPollingWorker(options)
  }
}
