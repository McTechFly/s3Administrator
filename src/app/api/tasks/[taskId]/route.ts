import { NextRequest, NextResponse } from "next/server"
import { Prisma } from "@prisma/client"
import { z } from "zod/v4"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { buildFileSearchSqlWhereClause, parseScopes } from "@/lib/file-search"
import { appendExecutionHistory } from "@/lib/task-plans"
import {
  assertValidTaskScheduleCron,
  TaskScheduleValidationError,
} from "@/lib/task-schedule"

const controlSchema = z.object({
  action: z.enum(["pause", "resume", "restart", "retry_failed", "update_schedule", "cancel"]),
  schedule: z
    .object({
      cron: z.string().trim().min(1).max(120),
    })
    .nullable()
    .optional(),
  confirmDestructiveSchedule: z.boolean().optional(),
})

const SYNC_POLL_INTERVAL_SECONDS = 60
const PAUSE_HOLD_MS = 365 * 24 * 60 * 60 * 1000

interface ObjectTransferTaskPayload {
  scope: "folder" | "bucket"
  operation: "sync" | "copy" | "move" | "migrate"
  sourceCredentialId: string
  sourceBucket: string
  sourcePrefix: string | null
  destinationCredentialId: string
  destinationBucket: string
  destinationPrefix: string | null
  pollIntervalSeconds: number | null
}

interface BulkDeleteTaskPayload {
  query: string
  selectedType: string
  selectedCredentialIds: string[]
  selectedBucketScopes: string[]
}

interface CountRow {
  total: bigint
}

function resolveTaskPlanPayload(executionPlan: unknown, fallbackPayload: unknown): unknown {
  if (!executionPlan || typeof executionPlan !== "object") {
    return fallbackPayload
  }
  const candidate = executionPlan as { payload?: unknown }
  return candidate.payload ?? fallbackPayload
}

function parseObjectTransferPayload(raw: unknown): ObjectTransferTaskPayload | null {
  if (!raw || typeof raw !== "object") return null
  const payload = raw as {
    scope?: unknown
    operation?: unknown
    sourceCredentialId?: unknown
    sourceBucket?: unknown
    sourcePrefix?: unknown
    destinationCredentialId?: unknown
    destinationBucket?: unknown
    destinationPrefix?: unknown
    pollIntervalSeconds?: unknown
  }

  if (payload.scope !== "folder" && payload.scope !== "bucket") return null
  if (
    payload.operation !== "sync" &&
    payload.operation !== "copy" &&
    payload.operation !== "move" &&
    payload.operation !== "migrate"
  ) {
    return null
  }
  if (typeof payload.sourceCredentialId !== "string" || !payload.sourceCredentialId.trim()) return null
  if (typeof payload.sourceBucket !== "string" || !payload.sourceBucket.trim()) return null
  if (typeof payload.destinationCredentialId !== "string" || !payload.destinationCredentialId.trim()) return null
  if (typeof payload.destinationBucket !== "string" || !payload.destinationBucket.trim()) return null

  const sourcePrefix =
    payload.sourcePrefix === null
      ? null
      : typeof payload.sourcePrefix === "string"
        ? payload.sourcePrefix
        : null
  const destinationPrefix =
    payload.destinationPrefix === null
      ? null
      : typeof payload.destinationPrefix === "string"
        ? payload.destinationPrefix
        : null

  if (payload.scope === "folder" && (!sourcePrefix || !destinationPrefix)) {
    return null
  }

  const pollIntervalSeconds =
    typeof payload.pollIntervalSeconds === "number" &&
    Number.isFinite(payload.pollIntervalSeconds) &&
    payload.pollIntervalSeconds >= SYNC_POLL_INTERVAL_SECONDS
      ? Math.floor(payload.pollIntervalSeconds)
      : null

  return {
    scope: payload.scope,
    operation: payload.operation,
    sourceCredentialId: payload.sourceCredentialId.trim(),
    sourceBucket: payload.sourceBucket.trim(),
    sourcePrefix,
    destinationCredentialId: payload.destinationCredentialId.trim(),
    destinationBucket: payload.destinationBucket.trim(),
    destinationPrefix,
    pollIntervalSeconds,
  }
}

function parseBulkDeletePayload(raw: unknown): BulkDeleteTaskPayload | null {
  if (!raw || typeof raw !== "object") return null
  const payload = raw as {
    query?: unknown
    selectedType?: unknown
    selectedCredentialIds?: unknown
    selectedBucketScopes?: unknown
  }

  if (typeof payload.query !== "string" || payload.query.trim().length < 2) {
    return null
  }

  return {
    query: payload.query.trim(),
    selectedType: typeof payload.selectedType === "string" ? payload.selectedType : "all",
    selectedCredentialIds: Array.isArray(payload.selectedCredentialIds)
      ? payload.selectedCredentialIds.filter((value): value is string => typeof value === "string")
      : [],
    selectedBucketScopes: Array.isArray(payload.selectedBucketScopes)
      ? payload.selectedBucketScopes.filter((value): value is string => typeof value === "string")
      : [],
  }
}

function pushHistory(
  current: unknown,
  status: "succeeded" | "failed" | "skipped" | "paused" | "resumed" | "restarted",
  message: string,
  metadata?: Record<string, unknown>
): Prisma.InputJsonValue {
  return appendExecutionHistory(current, {
    at: new Date().toISOString(),
    status,
    message,
    metadata,
  }) as unknown as Prisma.InputJsonValue
}

function getObjectTransferFailedCount(progress: unknown): number {
  if (!progress || typeof progress !== "object") return 0
  const value = (progress as { failed?: unknown }).failed
  if (typeof value !== "number" || !Number.isFinite(value)) return 0
  return Math.max(0, Math.floor(value))
}

function isDestructiveTransferOperation(operation: ObjectTransferTaskPayload["operation"]): boolean {
  return operation === "sync" || operation === "move" || operation === "migrate"
}

export async function PATCH(
  request: NextRequest,
  context: { params: Promise<{ taskId: string }> }
) {
  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const { taskId } = await context.params
    const body = await request.json()
    const parsed = controlSchema.safeParse(body)
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Invalid control payload", details: parsed.error.flatten() },
        { status: 400 }
      )
    }

    const task = await prisma.backgroundTask.findFirst({
      where: {
        id: taskId,
        userId: session.user.id,
      },
      select: {
        id: true,
        userId: true,
        type: true,
        status: true,
        lifecycleState: true,
        payload: true,
        executionPlan: true,
        executionHistory: true,
        progress: true,
        isRecurring: true,
        scheduleCron: true,
        scheduleIntervalSeconds: true,
        runCount: true,
        nextRunAt: true,
      },
    })

    if (!task) {
      return NextResponse.json({ error: "Task not found" }, { status: 404 })
    }

    if (parsed.data.action === "pause") {
      if (task.lifecycleState === "paused") {
        return NextResponse.json({ task })
      }

      const now = new Date()
      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          lifecycleState: "paused",
          pausedAt: now,
          status: task.status === "in_progress" ? "pending" : task.status,
          nextRunAt: new Date(now.getTime() + PAUSE_HOLD_MS),
          executionHistory: pushHistory(task.executionHistory, "paused", "Task paused by user"),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    if (parsed.data.action === "resume") {
      if (task.lifecycleState === "active" && !(task.isRecurring && task.status === "completed")) {
        return NextResponse.json({ task })
      }

      const now = new Date()
      const shouldRequeueRecurring = task.isRecurring && task.status === "completed"

      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          lifecycleState: "active",
          pausedAt: null,
          status: shouldRequeueRecurring ? "pending" : task.status,
          completedAt: shouldRequeueRecurring ? null : undefined,
          nextRunAt: now,
          executionHistory: pushHistory(task.executionHistory, "resumed", "Task resumed by user"),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    const retryFailedOnly = parsed.data.action === "retry_failed"
    if (retryFailedOnly && task.status !== "failed") {
      return NextResponse.json(
        { error: "Retry failed is only available for failed tasks" },
        { status: 400 }
      )
    }
    if (retryFailedOnly && task.type !== "object_transfer") {
      return NextResponse.json(
        { error: "Retry failed is currently supported for transfer tasks only" },
        { status: 400 }
      )
    }

    const planPayload = resolveTaskPlanPayload(task.executionPlan, task.payload)
    const now = new Date()

    if (parsed.data.action === "cancel") {
      if (task.status === "in_progress") {
        return NextResponse.json(
          { error: "Cannot cancel a running task. Please wait for the current run to finish." },
          { status: 400 }
        )
      }

      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          lifecycleState: "active",
          pausedAt: null,
          status: "completed",
          attempts: 0,
          lastError: null,
          isRecurring: false,
          scheduleCron: null,
          scheduleIntervalSeconds: null,
          completedAt: now,
          nextRunAt: now,
          executionHistory: pushHistory(task.executionHistory, "skipped", "Task canceled by user"),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
          isRecurring: true,
          scheduleCron: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    if (parsed.data.action === "update_schedule") {
      if (task.type === "thumbnail_generate") {
        return NextResponse.json(
          { error: "Scheduling is not supported for thumbnail tasks" },
          { status: 400 }
        )
      }

      let scheduleCron: string | null = null
      if (parsed.data.schedule?.cron) {
        scheduleCron = assertValidTaskScheduleCron(parsed.data.schedule.cron)
      }

      if (scheduleCron) {
        let destructive = false
        if (task.type === "bulk_delete") {
          destructive = true
        } else if (task.type === "object_transfer") {
          const transferPayload = parseObjectTransferPayload(planPayload)
          if (!transferPayload) {
            return NextResponse.json({ error: "Invalid transfer execution plan" }, { status: 400 })
          }
          destructive = isDestructiveTransferOperation(transferPayload.operation)
        }

        if (destructive && !parsed.data.confirmDestructiveSchedule) {
          return NextResponse.json(
            { error: "Recurring destructive task requires explicit confirmation" },
            { status: 400 }
          )
        }
      }

      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          lifecycleState: "active",
          pausedAt: null,
          isRecurring: Boolean(scheduleCron),
          scheduleCron,
          scheduleIntervalSeconds: null,
          ...(task.status !== "in_progress"
            ? {
                status: "pending",
                attempts: 0,
                startedAt: null,
                completedAt: null,
                nextRunAt: now,
                lastError: null,
              }
            : {}),
          executionHistory: pushHistory(
            task.executionHistory,
            "resumed",
            scheduleCron ? "Task schedule updated" : "Task schedule disabled"
          ),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
          isRecurring: true,
          scheduleCron: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    if (task.type === "object_transfer") {
      const transferPayload = parseObjectTransferPayload(planPayload)
      if (!transferPayload) {
        return NextResponse.json({ error: "Invalid transfer execution plan" }, { status: 400 })
      }

      if (retryFailedOnly) {
        const failedCount = getObjectTransferFailedCount(task.progress)
        if (failedCount <= 0) {
          return NextResponse.json(
            { error: "No failed objects available to retry" },
            { status: 400 }
          )
        }
      }

      const sourceWhere = {
        userId: session.user.id,
        credentialId: transferPayload.sourceCredentialId,
        bucket: transferPayload.sourceBucket,
        isFolder: false,
        ...(transferPayload.scope === "folder" && transferPayload.sourcePrefix
          ? { key: { startsWith: transferPayload.sourcePrefix } }
          : {}),
      }
      const total = await prisma.fileMetadata.count({
        where: sourceWhere,
      })

      if (retryFailedOnly) {
        await prisma.$transaction([
          prisma.backgroundTaskEvent.deleteMany({
            where: { taskId: task.id },
          }),
          prisma.backgroundTaskRun.deleteMany({
            where: { taskId: task.id },
          }),
        ])
      }

      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          payload: transferPayload as unknown as Prisma.InputJsonValue,
          status: "pending",
          lifecycleState: "active",
          pausedAt: null,
          attempts: 0,
          lastError: null,
          startedAt: null,
          completedAt: null,
          nextRunAt: now,
          isRecurring: task.isRecurring,
          scheduleCron: task.scheduleCron,
          scheduleIntervalSeconds: task.scheduleIntervalSeconds,
          runCount: retryFailedOnly ? 0 : undefined,
          progress: {
            phase: "transfer",
            total,
            processed: 0,
            copied: 0,
            moved: 0,
            deleted: 0,
            skipped: 0,
            failed: 0,
            remaining: total,
            cursorKey: null,
          } as Prisma.InputJsonObject,
          executionHistory: retryFailedOnly
            ? ([] as Prisma.InputJsonArray)
            : pushHistory(task.executionHistory, "restarted", "Task restarted by user"),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
          progress: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    if (task.type === "bulk_delete") {
      const bulkPayload = parseBulkDeletePayload(planPayload)
      if (!bulkPayload) {
        return NextResponse.json({ error: "Invalid bulk-delete execution plan" }, { status: 400 })
      }

      const whereClause = buildFileSearchSqlWhereClause({
        userId: session.user.id,
        query: bulkPayload.query,
        credentialIds: bulkPayload.selectedCredentialIds,
        scopes: parseScopes(bulkPayload.selectedBucketScopes),
        type: bulkPayload.selectedType,
      })
      const [countResult] = await prisma.$queryRaw<CountRow[]>(Prisma.sql`
        SELECT COUNT(*)::bigint AS "total"
        FROM "FileMetadata" fm
        WHERE ${whereClause}
      `)
      const total = Number(countResult?.total ?? 0)

      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          payload: bulkPayload as unknown as Prisma.InputJsonValue,
          status: "pending",
          lifecycleState: "active",
          pausedAt: null,
          attempts: 0,
          lastError: null,
          startedAt: null,
          completedAt: null,
          nextRunAt: now,
          isRecurring: task.isRecurring,
          scheduleCron: task.scheduleCron,
          scheduleIntervalSeconds: task.scheduleIntervalSeconds,
          progress: {
            total,
            deleted: 0,
            remaining: total,
            cursorId: null,
          } as Prisma.InputJsonObject,
          executionHistory: pushHistory(
            task.executionHistory,
            "restarted",
            retryFailedOnly ? "Retry failed items requested by user" : "Task restarted by user"
          ),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
          progress: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    if (task.type === "thumbnail_generate") {
      const updated = await prisma.backgroundTask.update({
        where: { id: task.id },
        data: {
          status: "pending",
          lifecycleState: "active",
          pausedAt: null,
          attempts: 0,
          lastError: null,
          startedAt: null,
          completedAt: null,
          nextRunAt: now,
          isRecurring: false,
          scheduleCron: null,
          scheduleIntervalSeconds: null,
          executionHistory: pushHistory(task.executionHistory, "restarted", "Task restarted by user"),
        },
        select: {
          id: true,
          status: true,
          lifecycleState: true,
          nextRunAt: true,
        },
      })

      return NextResponse.json({ task: updated })
    }

    return NextResponse.json({ error: "Unsupported task type" }, { status: 400 })
  } catch (error) {
    console.error("Failed to control task:", error)
    if (error instanceof TaskScheduleValidationError) {
      return NextResponse.json({ error: error.message }, { status: 400 })
    }
    const message = error instanceof Error ? error.message : "Failed to control task"
    return NextResponse.json({ error: message }, { status: 500 })
  }
}

export async function DELETE(
  _request: NextRequest,
  context: { params: Promise<{ taskId: string }> }
) {
  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const { taskId } = await context.params
    const task = await prisma.backgroundTask.findFirst({
      where: {
        id: taskId,
        userId: session.user.id,
      },
      select: {
        id: true,
        status: true,
      },
    })

    if (!task) {
      return NextResponse.json({ error: "Task not found" }, { status: 404 })
    }

    if (task.status === "in_progress") {
      return NextResponse.json(
        { error: "Cannot delete a running task" },
        { status: 400 }
      )
    }

    await prisma.backgroundTask.delete({
      where: { id: task.id },
    })

    return NextResponse.json({ deleted: true })
  } catch (error) {
    console.error("Failed to delete task:", error)
    const message = error instanceof Error ? error.message : "Failed to delete task"
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
