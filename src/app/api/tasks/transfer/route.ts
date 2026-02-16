import { NextRequest, NextResponse } from "next/server"
import { Prisma } from "@prisma/client"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { getS3Client } from "@/lib/s3"
import { getUserPlanEntitlements } from "@/lib/plan-entitlements"
import { getBucketLimitViolation } from "@/lib/plan-limits"
import {
  getObjectTransferDisabledMessage,
} from "@/lib/transfer-task-policy"
import { transferTaskSchema } from "@/lib/validations"
import { rateLimitByUser, rateLimitResponse } from "@/lib/rate-limit"
import { buildTaskDedupeKey, createTaskExecutionPlan } from "@/lib/task-plans"
import {
  assertValidTaskScheduleCron,
  TaskScheduleValidationError,
} from "@/lib/task-schedule"
import { isDestinationUpToDateForSync } from "@/lib/transfer-delta"
import { isBackgroundTaskSchemaOutdated, backgroundTaskSchemaOutdatedResponse } from "@/lib/task-errors"

type TransferScope = "folder" | "bucket"
type TransferOperation = "sync" | "copy" | "move" | "migrate"

function normalizeFolderPrefix(raw: string | undefined): string {
  const value = (raw ?? "").trim()
  if (!value) return ""
  return value.endsWith("/") ? value : `${value}/`
}

function isOperationAllowed(scope: TransferScope, operation: TransferOperation): boolean {
  if (scope === "folder") {
    return operation === "sync" || operation === "copy" || operation === "move"
  }
  return operation === "sync" || operation === "copy" || operation === "migrate"
}

function getOperationDisabledMessage(scope: TransferScope, operation: TransferOperation): string | null {
  if (operation === "sync") {
    return "Sync tasks are disabled for the current plan"
  }
  if (scope === "folder" && (operation === "copy" || operation === "move")) {
    return "Folder transfer tasks are disabled for the current plan"
  }
  if (scope === "bucket" && (operation === "copy" || operation === "migrate")) {
    return "Bucket transfer tasks are disabled for the current plan"
  }
  return null
}

function isOperationEnabledByPlan(
  entitlements: NonNullable<Awaited<ReturnType<typeof getUserPlanEntitlements>>>,
  scope: TransferScope,
  operation: TransferOperation
): boolean {
  if (operation === "sync") {
    return entitlements.syncTasks
  }
  if (scope === "folder" && (operation === "copy" || operation === "move")) {
    return entitlements.copyFolderToFolder
  }
  if (scope === "bucket" && (operation === "copy" || operation === "migrate")) {
    return entitlements.copyBucketToBucket
  }
  return false
}

interface TransferTaskPreview {
  type: "object_transfer"
  summary: string[]
  commands: string[]
  estimatedObjects: number
  sampleObjects: string[]
  warnings: string[]
  detailedPlan: TransferTaskDetailedPreviewPlan
}

type TransferPreviewPhase = "source" | "cleanup" | "done"

interface TransferTaskDetailedPreviewAction {
  phase: "transfer" | "sync_cleanup"
  operation: "copy" | "delete_source" | "delete_destination"
  command: string
  sourceKey: string | null
  destinationKey: string | null
}

interface TransferTaskDetailedPreviewCursor {
  phase: "source" | "cleanup"
  sourceKey: string | null
  cleanupKey: string | null
}

interface TransferTaskDetailedPreviewPlan {
  actions: TransferTaskDetailedPreviewAction[]
  hasMore: boolean
  nextCursor: TransferTaskDetailedPreviewCursor | null
  pageSize: number
  scannedSourceObjects: number
  scanLimitReached: boolean
}

interface DestinationMetadataPreviewRow {
  key: string
  size: bigint
  lastModified: Date
}

interface SyncDestinationDriftPreviewRow {
  key: string
}

const TRANSFER_PREVIEW_DEFAULT_LIMIT = 250
const TRANSFER_PREVIEW_MAX_LIMIT = 1000
const TRANSFER_PREVIEW_SOURCE_SCAN_MULTIPLIER = 4
const TRANSFER_PREVIEW_SOURCE_SCAN_MAX = 5000

function parsePreviewLimit(raw: unknown): number {
  if (typeof raw !== "number" || !Number.isFinite(raw)) {
    return TRANSFER_PREVIEW_DEFAULT_LIMIT
  }
  const normalized = Math.floor(raw)
  if (normalized <= 0) return TRANSFER_PREVIEW_DEFAULT_LIMIT
  return Math.min(TRANSFER_PREVIEW_MAX_LIMIT, normalized)
}

function parsePreviewCursor(raw: unknown): TransferTaskDetailedPreviewCursor {
  if (!raw || typeof raw !== "object") {
    return {
      phase: "source",
      sourceKey: null,
      cleanupKey: null,
    }
  }

  const candidate = raw as {
    phase?: unknown
    sourceKey?: unknown
    cleanupKey?: unknown
  }

  const phase = candidate.phase === "cleanup" ? "cleanup" : "source"
  const sourceKey =
    typeof candidate.sourceKey === "string" && candidate.sourceKey.trim().length > 0
      ? candidate.sourceKey
      : null
  const cleanupKey =
    typeof candidate.cleanupKey === "string" && candidate.cleanupKey.trim().length > 0
      ? candidate.cleanupKey
      : null

  return {
    phase,
    sourceKey,
    cleanupKey,
  }
}

function isInitialTransferPreviewRequest(cursor: TransferTaskDetailedPreviewCursor): boolean {
  return cursor.phase === "source" && !cursor.sourceKey && !cursor.cleanupKey
}

function mapTransferPreviewDestinationKey(params: {
  scope: TransferScope
  sourcePrefix: string
  destinationPrefix: string
  sourceKey: string
}): string {
  if (params.scope === "bucket") {
    return params.sourceKey
  }

  if (!params.sourcePrefix || !params.sourceKey.startsWith(params.sourcePrefix)) {
    return `${params.destinationPrefix}${params.sourceKey}`
  }
  return `${params.destinationPrefix}${params.sourceKey.slice(params.sourcePrefix.length)}`
}

function buildObjectPath(bucket: string, key: string): string {
  return `${bucket}/${key}`
}

function buildCopyAction(params: {
  sourceBucket: string
  sourceKey: string
  destinationBucket: string
  destinationKey: string
}): TransferTaskDetailedPreviewAction {
  return {
    phase: "transfer",
    operation: "copy",
    command: `COPY ${buildObjectPath(params.sourceBucket, params.sourceKey)} -> ${buildObjectPath(params.destinationBucket, params.destinationKey)}`,
    sourceKey: params.sourceKey,
    destinationKey: params.destinationKey,
  }
}

function buildDeleteSourceAction(params: {
  sourceBucket: string
  sourceKey: string
}): TransferTaskDetailedPreviewAction {
  return {
    phase: "transfer",
    operation: "delete_source",
    command: `DELETE ${buildObjectPath(params.sourceBucket, params.sourceKey)} (after successful copy)`,
    sourceKey: params.sourceKey,
    destinationKey: null,
  }
}

function buildDeleteDestinationAction(params: {
  destinationBucket: string
  destinationKey: string
}): TransferTaskDetailedPreviewAction {
  return {
    phase: "sync_cleanup",
    operation: "delete_destination",
    command: `DELETE ${buildObjectPath(params.destinationBucket, params.destinationKey)} (destination-only drift)`,
    sourceKey: null,
    destinationKey: params.destinationKey,
  }
}

async function findSyncDestinationDriftPreviewBatch(params: {
  userId: string
  scope: TransferScope
  sourceCredentialId: string
  sourceBucket: string
  sourcePrefix: string
  destinationCredentialId: string
  destinationBucket: string
  destinationPrefix: string
  cursorKey: string | null
  take: number
}): Promise<SyncDestinationDriftPreviewRow[]> {
  if (params.scope === "bucket") {
    return prisma.$queryRaw<SyncDestinationDriftPreviewRow[]>(Prisma.sql`
      SELECT d."key"
      FROM "FileMetadata" d
      WHERE d."userId" = ${params.userId}
        AND d."credentialId" = ${params.destinationCredentialId}
        AND d."bucket" = ${params.destinationBucket}
        AND d."isFolder" = false
        ${params.cursorKey ? Prisma.sql`AND d."key" > ${params.cursorKey}` : Prisma.empty}
        AND NOT EXISTS (
          SELECT 1
          FROM "FileMetadata" s
          WHERE s."userId" = ${params.userId}
            AND s."credentialId" = ${params.sourceCredentialId}
            AND s."bucket" = ${params.sourceBucket}
            AND s."isFolder" = false
            AND s."key" = d."key"
        )
      ORDER BY d."key" ASC
      LIMIT ${params.take}
    `)
  }

  const destinationPrefixLength = params.destinationPrefix.length
  const substringStart = destinationPrefixLength + 1

  return prisma.$queryRaw<SyncDestinationDriftPreviewRow[]>(Prisma.sql`
    SELECT d."key"
    FROM "FileMetadata" d
    WHERE d."userId" = ${params.userId}
      AND d."credentialId" = ${params.destinationCredentialId}
      AND d."bucket" = ${params.destinationBucket}
      AND d."isFolder" = false
      AND LEFT(d."key", ${destinationPrefixLength}) = ${params.destinationPrefix}
      ${params.cursorKey ? Prisma.sql`AND d."key" > ${params.cursorKey}` : Prisma.empty}
      AND NOT EXISTS (
        SELECT 1
        FROM "FileMetadata" s
        WHERE s."userId" = ${params.userId}
          AND s."credentialId" = ${params.sourceCredentialId}
          AND s."bucket" = ${params.sourceBucket}
          AND s."isFolder" = false
          AND s."key" = ${params.sourcePrefix} || substring(d."key" from ${substringStart})
      )
    ORDER BY d."key" ASC
    LIMIT ${params.take}
  `)
}

async function buildTransferDetailedPreviewPlan(params: {
  userId: string
  scope: TransferScope
  operation: TransferOperation
  sourceCredentialId: string
  sourceBucket: string
  sourcePrefix: string
  destinationCredentialId: string
  destinationBucket: string
  destinationPrefix: string
  pageSize: number
  cursor: TransferTaskDetailedPreviewCursor
}): Promise<TransferTaskDetailedPreviewPlan> {
  const actions: TransferTaskDetailedPreviewAction[] = []
  let scannedSourceObjects = 0
  let scanLimitReached = false
  let phase: TransferPreviewPhase = params.cursor.phase
  let sourceCursor = params.cursor.sourceKey
  let cleanupCursor = params.cursor.cleanupKey
  let sourceExhausted = false
  let cleanupExhausted = params.operation !== "sync"

  while (phase === "source" && actions.length < params.pageSize) {
    if (scannedSourceObjects >= TRANSFER_PREVIEW_SOURCE_SCAN_MAX) {
      scanLimitReached = true
      break
    }

    const remaining = params.pageSize - actions.length
    const scanTake = Math.min(
      1000,
      Math.max(remaining * TRANSFER_PREVIEW_SOURCE_SCAN_MULTIPLIER, remaining),
      TRANSFER_PREVIEW_SOURCE_SCAN_MAX - scannedSourceObjects
    )

    const sourceKeyFilter: { startsWith?: string; gt?: string } = {}
    if (params.scope === "folder" && params.sourcePrefix) {
      sourceKeyFilter.startsWith = params.sourcePrefix
    }
    if (sourceCursor) {
      sourceKeyFilter.gt = sourceCursor
    }

    const sourceRows = await prisma.fileMetadata.findMany({
      where: {
        userId: params.userId,
        credentialId: params.sourceCredentialId,
        bucket: params.sourceBucket,
        isFolder: false,
        ...(Object.keys(sourceKeyFilter).length > 0 ? { key: sourceKeyFilter } : {}),
      },
      orderBy: { key: "asc" },
      take: scanTake,
      select: {
        key: true,
        size: true,
        lastModified: true,
      },
    })

    if (sourceRows.length === 0) {
      sourceExhausted = true
      break
    }

    scannedSourceObjects += sourceRows.length

    const requiresDestinationComparison =
      params.operation === "copy" || params.operation === "sync"
    let destinationByKey = new Map<string, DestinationMetadataPreviewRow>()

    if (requiresDestinationComparison) {
      const destinationKeys = sourceRows.map((row) =>
        mapTransferPreviewDestinationKey({
          scope: params.scope,
          sourcePrefix: params.sourcePrefix,
          destinationPrefix: params.destinationPrefix,
          sourceKey: row.key,
        })
      )

      const destinationRows = await prisma.fileMetadata.findMany({
        where: {
          userId: params.userId,
          credentialId: params.destinationCredentialId,
          bucket: params.destinationBucket,
          isFolder: false,
          key: { in: destinationKeys },
        },
        select: {
          key: true,
          size: true,
          lastModified: true,
        },
      })
      destinationByKey = new Map(destinationRows.map((row) => [row.key, row]))
    }

    let consumedAllRows = true
    for (let i = 0; i < sourceRows.length; i++) {
      const sourceRow = sourceRows[i]
      const destinationKey = mapTransferPreviewDestinationKey({
        scope: params.scope,
        sourcePrefix: params.sourcePrefix,
        destinationPrefix: params.destinationPrefix,
        sourceKey: sourceRow.key,
      })
      const destinationExisting = requiresDestinationComparison
        ? destinationByKey.get(destinationKey)
        : undefined
      const rowActions: TransferTaskDetailedPreviewAction[] = []

      if (params.operation === "move" || params.operation === "migrate") {
        rowActions.push(
          buildCopyAction({
            sourceBucket: params.sourceBucket,
            sourceKey: sourceRow.key,
            destinationBucket: params.destinationBucket,
            destinationKey,
          })
        )
        rowActions.push(
          buildDeleteSourceAction({
            sourceBucket: params.sourceBucket,
            sourceKey: sourceRow.key,
          })
        )
      } else if (params.operation === "copy") {
        if (!destinationExisting) {
          rowActions.push(
            buildCopyAction({
              sourceBucket: params.sourceBucket,
              sourceKey: sourceRow.key,
              destinationBucket: params.destinationBucket,
              destinationKey,
            })
          )
        }
      } else {
        const shouldCopy =
          !destinationExisting ||
          !isDestinationUpToDateForSync(sourceRow, destinationExisting)
        if (shouldCopy) {
          rowActions.push(
            buildCopyAction({
              sourceBucket: params.sourceBucket,
              sourceKey: sourceRow.key,
              destinationBucket: params.destinationBucket,
              destinationKey,
            })
          )
        }
      }

      if (
        rowActions.length > 0 &&
        actions.length > 0 &&
        actions.length + rowActions.length > params.pageSize
      ) {
        consumedAllRows = false
        break
      }

      if (rowActions.length > 0) {
        actions.push(...rowActions)
      }
      sourceCursor = sourceRow.key

      if (actions.length >= params.pageSize && i < sourceRows.length - 1) {
        consumedAllRows = false
        break
      }
      if (actions.length >= params.pageSize) {
        break
      }
    }

    if (!consumedAllRows || actions.length >= params.pageSize) {
      break
    }

    if (sourceRows.length < scanTake) {
      sourceExhausted = true
      break
    }
  }

  if (phase === "source" && sourceExhausted) {
    phase = params.operation === "sync" ? "cleanup" : "done"
    cleanupCursor = null
  }

  while (phase === "cleanup" && actions.length < params.pageSize) {
    const remaining = params.pageSize - actions.length
    const cleanupRows = await findSyncDestinationDriftPreviewBatch({
      userId: params.userId,
      scope: params.scope,
      sourceCredentialId: params.sourceCredentialId,
      sourceBucket: params.sourceBucket,
      sourcePrefix: params.sourcePrefix,
      destinationCredentialId: params.destinationCredentialId,
      destinationBucket: params.destinationBucket,
      destinationPrefix: params.destinationPrefix,
      cursorKey: cleanupCursor,
      take: remaining,
    })

    if (cleanupRows.length === 0) {
      cleanupExhausted = true
      phase = "done"
      break
    }

    for (const row of cleanupRows) {
      actions.push(
        buildDeleteDestinationAction({
          destinationBucket: params.destinationBucket,
          destinationKey: row.key,
        })
      )
      cleanupCursor = row.key
    }

    if (cleanupRows.length < remaining) {
      cleanupExhausted = true
      phase = "done"
      break
    }

    break
  }

  if (phase === "source") {
    return {
      actions,
      hasMore: true,
      nextCursor: {
        phase: "source",
        sourceKey: sourceCursor,
        cleanupKey: null,
      },
      pageSize: params.pageSize,
      scannedSourceObjects,
      scanLimitReached,
    }
  }

  if (phase === "cleanup") {
    return {
      actions,
      hasMore: true,
      nextCursor: {
        phase: "cleanup",
        sourceKey: sourceCursor,
        cleanupKey: cleanupCursor,
      },
      pageSize: params.pageSize,
      scannedSourceObjects,
      scanLimitReached,
    }
  }

  return {
    actions,
    hasMore: !cleanupExhausted && params.operation === "sync",
    nextCursor:
      !cleanupExhausted && params.operation === "sync"
        ? {
          phase: "cleanup",
          sourceKey: sourceCursor,
          cleanupKey: cleanupCursor,
        }
        : null,
    pageSize: params.pageSize,
    scannedSourceObjects,
    scanLimitReached,
  }
}

function isDestructiveTransferOperation(operation: TransferOperation): boolean {
  return operation === "sync" || operation === "move" || operation === "migrate"
}

function buildTransferPreview(params: {
  scope: TransferScope
  operation: TransferOperation
  sourceBucket: string
  sourcePrefix: string
  destinationBucket: string
  destinationPrefix: string
  sourceCachedFileCount: number
  sampleObjects: string[]
  duplicateQueued: boolean
  scheduleCron: string | null
  detailedPlan: TransferTaskDetailedPreviewPlan
}): TransferTaskPreview {
  const isSync = params.operation === "sync"
  const isMoveLike = params.operation === "move" || params.operation === "migrate"

  const summary = [
    `Operation: ${params.operation.toUpperCase()} (${params.scope === "folder" ? "folder-to-folder" : "bucket-to-bucket"})`,
    `Source: ${params.scope === "folder" ? `${params.sourceBucket}/${params.sourcePrefix}` : params.sourceBucket}`,
    `Destination: ${params.scope === "folder" ? `${params.destinationBucket}/${params.destinationPrefix}` : params.destinationBucket}`,
    `Estimated source objects from cache: ${params.sourceCachedFileCount.toLocaleString()}`,
    params.scheduleCron
      ? `Schedule: CRON (${params.scheduleCron}) UTC`
      : "Schedule: one-time run",
  ]

  const commands = [
    "Read source objects from local metadata cache in chunks.",
    "Copy each source object to destination (CopyObject with stream fallback).",
    "Upsert destination metadata and persist checkpoint progress after each chunk.",
  ]

  if (isMoveLike) {
    commands.splice(2, 0, "Delete source object after successful copy.")
  }
  if (isSync) {
    commands.push("Delete destination-only objects inside selected destination scope.")
  }

  const warnings: string[] = []
  if (isSync) {
    warnings.push("Sync can delete destination-only objects in the selected scope.")
  }
  if (isMoveLike) {
    warnings.push("This operation deletes source objects after successful copy.")
  }
  if (params.duplicateQueued) {
    warnings.push("An equivalent transfer task is already queued or running.")
  }
  if (params.scheduleCron && isDestructiveTransferOperation(params.operation)) {
    warnings.push("Recurring destructive transfer is enabled. Each run can delete objects.")
  }
  if (params.sourceCachedFileCount === 0 && isSync) {
    warnings.push(
      "No source objects are currently cached; this cycle may primarily perform destination cleanup."
    )
  }

  return {
    type: "object_transfer",
    summary,
    commands,
    estimatedObjects: params.sourceCachedFileCount,
    sampleObjects: params.sampleObjects,
    warnings,
    detailedPlan: params.detailedPlan,
  }
}

export async function POST(request: NextRequest) {
  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const limitResult = rateLimitByUser(session.user.id, "task-transfer-create", 20, 60_000)
    if (!limitResult.success) {
      return rateLimitResponse(limitResult.retryAfterSeconds)
    }

    const entitlements = await getUserPlanEntitlements(session.user.id)
    if (!entitlements || !entitlements.transferTasks) {
      return NextResponse.json(
        {
          error: getObjectTransferDisabledMessage(),
          details: {
            plan: entitlements?.slug ?? "free",
            planSource: entitlements?.source ?? "default",
          },
        },
        { status: 403 }
      )
    }

    const body = await request.json()
    const previewOnly =
      typeof (body as { previewOnly?: unknown })?.previewOnly === "boolean"
        ? Boolean((body as { previewOnly?: unknown }).previewOnly)
        : false
    const previewLimit = parsePreviewLimit((body as { previewLimit?: unknown })?.previewLimit)
    const previewCursor = parsePreviewCursor((body as { previewCursor?: unknown })?.previewCursor)
    const parsed = transferTaskSchema.safeParse(body)
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Invalid parameters", details: parsed.error.flatten() },
        { status: 400 }
      )
    }

    const {
      scope,
      operation,
      sourceBucket,
      sourceCredentialId,
      sourcePrefix,
      destinationBucket,
      destinationCredentialId,
      destinationPrefix,
      schedule,
      confirmDestructiveSchedule,
    } = parsed.data

    let scheduleCron: string | null = null
    if (schedule?.cron) {
      scheduleCron = assertValidTaskScheduleCron(schedule.cron)
    }

    if (!isOperationAllowed(scope, operation)) {
      return NextResponse.json(
        { error: `Operation '${operation}' is not allowed for scope '${scope}'` },
        { status: 400 }
      )
    }

    if (!isOperationEnabledByPlan(entitlements, scope, operation)) {
      return NextResponse.json(
        {
          error: getOperationDisabledMessage(scope, operation) ?? "Transfer operation is disabled for the current plan",
          details: {
            plan: entitlements.slug,
            planSource: entitlements.source,
            scope,
            operation,
          },
        },
        { status: 403 }
      )
    }

    const normalizedSourcePrefix = scope === "folder" ? normalizeFolderPrefix(sourcePrefix) : ""
    const normalizedDestinationPrefix = scope === "folder" ? normalizeFolderPrefix(destinationPrefix) : ""

    if (scope === "folder") {
      if (!normalizedSourcePrefix || !normalizedDestinationPrefix) {
        return NextResponse.json(
          { error: "sourcePrefix and destinationPrefix are required for folder tasks" },
          { status: 400 }
        )
      }
    }

    const { credential: sourceCredential } = await getS3Client(session.user.id, sourceCredentialId)
    const { credential: destinationCredential } = await getS3Client(
      session.user.id,
      destinationCredentialId
    )

    const sameSourceAndDestination =
      sourceCredential.id === destinationCredential.id &&
      sourceBucket === destinationBucket &&
      (scope === "bucket" || normalizedSourcePrefix === normalizedDestinationPrefix)

    if (sameSourceAndDestination) {
      return NextResponse.json(
        { error: "Source and destination cannot be identical" },
        { status: 400 }
      )
    }

    const destinationContextChanged =
      sourceCredential.id !== destinationCredential.id ||
      sourceBucket !== destinationBucket
    if (destinationContextChanged) {
      const bucketLimitViolation = await getBucketLimitViolation({
        userId: session.user.id,
        credentialId: destinationCredential.id,
        bucket: destinationBucket,
        entitlements,
      })
      if (bucketLimitViolation) {
        return NextResponse.json(
          {
            error: "Bucket limit reached for current plan",
            details: bucketLimitViolation,
          },
          { status: 400 }
        )
      }
    }

    const sourceWhere = {
      userId: session.user.id,
      credentialId: sourceCredential.id,
      bucket: sourceBucket,
      isFolder: false,
      ...(scope === "folder" ? { key: { startsWith: normalizedSourcePrefix } } : {}),
    }

    const sourceCachedFileCount = await prisma.fileMetadata.count({
      where: sourceWhere,
    })
    const sampleObjects = previewOnly && isInitialTransferPreviewRequest(previewCursor)
      ? (
        await prisma.fileMetadata.findMany({
          where: sourceWhere,
          orderBy: { key: "asc" },
          take: 12,
          select: { key: true },
        })
      ).map((item) => item.key)
      : []

    if (sourceCachedFileCount === 0 && operation !== "sync") {
      return NextResponse.json(
        { error: "No cached source files matched this task" },
        { status: 400 }
      )
    }

    if (operation === "copy" || operation === "sync") {
      if (Number.isFinite(entitlements.fileLimit)) {
        const currentCachedFiles = await prisma.fileMetadata.count({
          where: {
            userId: session.user.id,
            isFolder: false,
          },
        })

        const projectedUpperBound = currentCachedFiles + sourceCachedFileCount
        if (projectedUpperBound > entitlements.fileLimit) {
          return NextResponse.json(
            {
              error:
                "Task could exceed your cached file limit. Upgrade or reduce source scope.",
              details: {
                currentCachedFiles,
                sourceCachedFileCount,
                fileLimit: entitlements.fileLimit,
                projectedUpperBound,
              },
            },
            { status: 400 }
          )
        }
      }
    }

    const title =
      scope === "folder"
        ? `${operation.toUpperCase()} folder ${sourceBucket}/${normalizedSourcePrefix} -> ${destinationBucket}/${normalizedDestinationPrefix}`
        : `${operation.toUpperCase()} bucket ${sourceBucket} -> ${destinationBucket}`

    const taskPayload = {
      scope,
      operation,
      sourceCredentialId: sourceCredential.id,
      sourceBucket,
      sourcePrefix: normalizedSourcePrefix || null,
      destinationCredentialId: destinationCredential.id,
      destinationBucket,
      destinationPrefix: normalizedDestinationPrefix || null,
    }

    const dedupeKey = buildTaskDedupeKey("object_transfer", {
      payload: taskPayload,
      scheduleCron,
    })
    const existingTask = await prisma.backgroundTask.findFirst({
      where: {
        userId: session.user.id,
        type: "object_transfer",
        dedupeKey,
        lifecycleState: {
          in: ["active", "paused"],
        },
        status: {
          in: ["pending", "in_progress"],
        },
      },
      select: {
        id: true,
        type: true,
        title: true,
        status: true,
        progress: true,
      },
    })

    if (previewOnly) {
      const detailedPlan = await buildTransferDetailedPreviewPlan({
        userId: session.user.id,
        scope,
        operation,
        sourceCredentialId: sourceCredential.id,
        sourceBucket,
        sourcePrefix: normalizedSourcePrefix,
        destinationCredentialId: destinationCredential.id,
        destinationBucket,
        destinationPrefix: normalizedDestinationPrefix,
        pageSize: previewLimit,
        cursor: previewCursor,
      })

      return NextResponse.json({
        preview: buildTransferPreview({
          scope,
          operation,
          sourceBucket,
          sourcePrefix: normalizedSourcePrefix,
          destinationBucket,
          destinationPrefix: normalizedDestinationPrefix,
          sourceCachedFileCount,
          sampleObjects,
          duplicateQueued: Boolean(existingTask),
          scheduleCron,
          detailedPlan,
        }),
        duplicate: Boolean(existingTask),
        task: existingTask ?? null,
      })
    }

    if (
      scheduleCron &&
      isDestructiveTransferOperation(operation) &&
      !confirmDestructiveSchedule
    ) {
      return NextResponse.json(
        {
          error:
            "Recurring destructive transfer requires explicit confirmation",
        },
        { status: 400 }
      )
    }

    if (existingTask) {
      return NextResponse.json({
        task: existingTask,
        duplicate: true,
        sourceCachedFileCount,
        note: "An equivalent transfer task is already queued or running.",
      })
    }

    const task = await prisma.backgroundTask.create({
      data: {
        userId: session.user.id,
        type: "object_transfer",
        title,
        status: "pending",
        lifecycleState: "active",
        payload: taskPayload,
        executionPlan: createTaskExecutionPlan("object_transfer", taskPayload),
        dedupeKey,
        isRecurring: Boolean(scheduleCron),
        scheduleCron,
        scheduleIntervalSeconds: null,
        progress: {
          phase: "transfer",
          total: sourceCachedFileCount,
          processed: 0,
          copied: 0,
          moved: 0,
          deleted: 0,
          skipped: 0,
          failed: 0,
          remaining: sourceCachedFileCount,
          cursorKey: null,
        },
      },
      select: {
        id: true,
        type: true,
        title: true,
        status: true,
        progress: true,
      },
    })

    return NextResponse.json({
      task,
      sourceCachedFileCount,
      note:
        operation === "sync"
          ? "Sync mirrors cached source files and deletes destination-only cached files in the selected scope."
          : "Only cached source files are processed for transfer tasks.",
    })
  } catch (error) {
    console.error("Failed to create transfer task:", error)
    if (error instanceof TaskScheduleValidationError) {
      return NextResponse.json({ error: error.message }, { status: 400 })
    }
    if (isBackgroundTaskSchemaOutdated(error)) {
      return backgroundTaskSchemaOutdatedResponse()
    }
    const message = error instanceof Error ? error.message : "Failed to create transfer task"
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
