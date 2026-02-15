import { NextRequest, NextResponse } from "next/server"
import { Prisma } from "@prisma/client"
import { z } from "zod/v4"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { buildFileSearchSqlWhereClause, parseScopes } from "@/lib/file-search"
import { appendExecutionHistory } from "@/lib/task-plans"

const controlSchema = z.object({
  action: z.enum(["pause", "resume", "restart", "retry_failed"]),
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
          nextRunAt:
            task.nextRunAt.getTime() > now.getTime() && !shouldRequeueRecurring
              ? task.nextRunAt
              : now,
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
          isRecurring: transferPayload.operation === "sync",
          scheduleIntervalSeconds:
            transferPayload.operation === "sync"
              ? transferPayload.pollIntervalSeconds ?? SYNC_POLL_INTERVAL_SECONDS
              : null,
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
          isRecurring: false,
          scheduleIntervalSeconds: null,
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
    const message = error instanceof Error ? error.message : "Failed to control task"
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
