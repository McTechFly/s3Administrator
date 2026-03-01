import { NextResponse } from "next/server"
import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  type S3Client,
  UploadPartCopyCommand,
} from "@aws-sdk/client-s3"
import { Upload } from "@aws-sdk/lib-storage"
import { Prisma } from "@prisma/client"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { getS3Client } from "@/lib/s3"
import { applyUserExtensionStatsDelta, rebuildUserExtensionStats } from "@/lib/file-stats"
import { buildFileSearchSqlWhereClause, parseScopes } from "@/lib/file-search"
import { getUserPlanEntitlements } from "@/lib/plan-entitlements"
import { getBucketLimitViolation } from "@/lib/plan-limits"
import { logUserAuditAction } from "@/lib/audit-logger"
import {
  getTaskBulkDeleteBatchSize,
  getTaskEngineInternalToken,
  getTaskMaxActivePerUser,
  getTaskMissedScheduleGraceSeconds,
  getTaskTransferBatchSize,
  getTaskTransferItemConcurrency,
  getTaskWorkerUserBudgetMs,
} from "@/lib/task-engine-config"
import {
  appendExecutionHistory,
  normalizeExecutionHistory,
  type TaskExecutionHistoryEntry,
} from "@/lib/task-plans"
import {
  nextRunAtForTaskSchedule,
  resolveTaskSchedule,
  type ResolvedTaskSchedule,
} from "@/lib/task-schedule"
import { isDestinationUpToDateForSync } from "@/lib/transfer-delta"

export const runtime = "nodejs"
export const maxDuration = 60

const LOCK_SECONDS = 45
const SYNC_POLL_INTERVAL_SECONDS = 60
const MAX_STALE_SCHEDULE_SKIPS_PER_CALL = 32
const PAUSE_HOLD_MS = 365 * 24 * 60 * 60 * 1000
const ONE_MEBIBYTE_BYTES = 1024 * 1024
const ONE_MEBIBYTE_BIGINT = BigInt(ONE_MEBIBYTE_BYTES)
const DEFAULT_MULTIPART_PART_SIZE_BYTES = 64 * ONE_MEBIBYTE_BYTES
const DEFAULT_MULTIPART_PART_SIZE_BIGINT = BigInt(DEFAULT_MULTIPART_PART_SIZE_BYTES)
const MAX_MULTIPART_PARTS = 10_000
const RELAY_UPLOAD_QUEUE_SIZE = 4
const SINGLE_REQUEST_COPY_MAX_BYTES = BigInt(5 * 1024 * 1024 * 1024)

interface BulkDeleteTaskPayload {
  query: string
  selectedType: string
  selectedCredentialIds: string[]
  selectedBucketScopes: string[]
}

interface BulkDeleteTaskProgress {
  total: number
  deleted: number
  remaining: number
  cursorId: string | null
}

type TransferScope = "folder" | "bucket"
type TransferOperation = "sync" | "copy" | "move" | "migrate"

interface ObjectTransferTaskPayload {
  scope: TransferScope
  operation: TransferOperation
  sourceCredentialId: string
  sourceBucket: string
  sourcePrefix: string | null
  destinationCredentialId: string
  destinationBucket: string
  destinationPrefix: string | null
  pollIntervalSeconds: number | null
}

interface ObjectTransferTaskProgress {
  phase: "transfer"
  total: number
  processed: number
  copied: number
  moved: number
  deleted: number
  skipped: number
  failed: number
  remaining: number
  cursorKey: string | null
}

interface WorkerTaskSnapshot {
  taskId: string
  taskType: string
  taskStatus: string
  runCount: number
  attempts: number
  lastError: string | null
  taskUserId: string
}

interface TransferSourceRow {
  id: string
  key: string
  extension: string
  size: bigint
  lastModified: Date
}

interface TransferDestinationSnapshot {
  size: bigint
  lastModified: Date
}

interface TransferMetadataUpsertRow {
  userId: string
  credentialId: string
  bucket: string
  key: string
  extension: string
  size: bigint
  lastModified: Date
}

interface PreparedTransferItem {
  sourceFile: TransferSourceRow
  destinationKey: string
  createsNewDestination: boolean
  skip: boolean
}

interface TransferItemResult {
  status: "copied" | "moved" | "skipped" | "failed" | "missing_source"
  sourceId: string
  sourceKey: string
  destinationKey: string
  extension: string
  size: bigint
  lastModified: Date
  createsNewDestination: boolean
  sourceDeleteRequired: boolean
  errorMessage: string | null
}

type TransferStrategy =
  | "single_request_server_copy"
  | "multipart_server_copy"
  | "multipart_relay_upload"

interface ClaimedTaskControlState {
  status: string
  lifecycleState: string
  pausedAt: Date | null
}

interface PersistClaimedTaskCheckpointParams {
  taskId: string
  userId: string
  claimedRunCount: number
  normalUpdate: Prisma.BackgroundTaskUpdateManyMutationInput
  pauseUpdate?: Prisma.BackgroundTaskUpdateManyMutationInput
  cancelUpdate?: Prisma.BackgroundTaskUpdateManyMutationInput
  preferTerminal?: boolean
}

interface PersistClaimedTaskCheckpointResult {
  applied: boolean
  appliedMode: "normal" | "paused" | "canceled"
  finalStatus: string
}

function parsePayload(raw: unknown): BulkDeleteTaskPayload | null {
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

function parseProgress(raw: unknown, totalFallback = 0): BulkDeleteTaskProgress {
  if (!raw || typeof raw !== "object") {
    return {
      total: totalFallback,
      deleted: 0,
      remaining: totalFallback,
      cursorId: null,
    }
  }

  const progress = raw as {
    total?: unknown
    deleted?: unknown
    remaining?: unknown
    cursorId?: unknown
  }

  const total = typeof progress.total === "number" ? Math.max(0, Math.floor(progress.total)) : totalFallback
  const deleted = typeof progress.deleted === "number" ? Math.max(0, Math.floor(progress.deleted)) : 0
  const remaining = typeof progress.remaining === "number"
    ? Math.max(0, Math.floor(progress.remaining))
    : Math.max(0, total - deleted)

  return {
    total,
    deleted,
    remaining,
    cursorId:
      typeof progress.cursorId === "string" && progress.cursorId.trim().length > 0
        ? progress.cursorId
        : null,
  }
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

  const scope = payload.scope
  const operation = payload.operation
  if (scope !== "folder" && scope !== "bucket") return null
  if (
    operation !== "sync" &&
    operation !== "copy" &&
    operation !== "move" &&
    operation !== "migrate"
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

  const pollIntervalSeconds =
    typeof payload.pollIntervalSeconds === "number" &&
    Number.isFinite(payload.pollIntervalSeconds) &&
    payload.pollIntervalSeconds >= SYNC_POLL_INTERVAL_SECONDS
      ? Math.floor(payload.pollIntervalSeconds)
      : null

  return {
    scope,
    operation,
    sourceCredentialId: payload.sourceCredentialId.trim(),
    sourceBucket: payload.sourceBucket.trim(),
    sourcePrefix,
    destinationCredentialId: payload.destinationCredentialId.trim(),
    destinationBucket: payload.destinationBucket.trim(),
    destinationPrefix,
    pollIntervalSeconds,
  }
}

function parseObjectTransferProgress(
  raw: unknown,
  totalFallback = 0
): ObjectTransferTaskProgress {
  if (!raw || typeof raw !== "object") {
    return {
      phase: "transfer",
      total: totalFallback,
      processed: 0,
      copied: 0,
      moved: 0,
      deleted: 0,
      skipped: 0,
      failed: 0,
      remaining: totalFallback,
      cursorKey: null,
    }
  }

  const progress = raw as {
    phase?: unknown
    total?: unknown
    processed?: unknown
    copied?: unknown
    moved?: unknown
    deleted?: unknown
    skipped?: unknown
    failed?: unknown
    remaining?: unknown
    cursorKey?: unknown
  }

  const total =
    typeof progress.total === "number" ? Math.max(0, Math.floor(progress.total)) : totalFallback
  const processed =
    typeof progress.processed === "number" ? Math.max(0, Math.floor(progress.processed)) : 0
  const copied = typeof progress.copied === "number" ? Math.max(0, Math.floor(progress.copied)) : 0
  const moved = typeof progress.moved === "number" ? Math.max(0, Math.floor(progress.moved)) : 0
  const deleted = typeof progress.deleted === "number" ? Math.max(0, Math.floor(progress.deleted)) : 0
  const skipped = typeof progress.skipped === "number" ? Math.max(0, Math.floor(progress.skipped)) : 0
  const failed = typeof progress.failed === "number" ? Math.max(0, Math.floor(progress.failed)) : 0

  const remaining =
    typeof progress.remaining === "number"
      ? Math.max(0, Math.floor(progress.remaining))
      : Math.max(0, total - processed)

  return {
    phase: progress.phase === "transfer" ? "transfer" : "transfer",
    total,
    processed,
    copied,
    moved,
    deleted,
    skipped,
    failed,
    remaining,
    cursorKey: typeof progress.cursorKey === "string" && progress.cursorKey.length > 0
      ? progress.cursorKey
      : null,
  }
}

function mapTransferDestinationKey(
  payload: ObjectTransferTaskPayload,
  sourceKey: string
): string {
  if (payload.scope === "bucket") {
    return sourceKey
  }

  const sourcePrefix = payload.sourcePrefix ?? ""
  const destinationPrefix = payload.destinationPrefix ?? ""
  if (!sourcePrefix || !sourceKey.startsWith(sourcePrefix)) {
    return `${destinationPrefix}${sourceKey}`
  }
  return `${destinationPrefix}${sourceKey.slice(sourcePrefix.length)}`
}

function buildCopySource(bucket: string, key: string): string {
  const encodedBucket = encodeURIComponent(bucket)
  const encodedKey = key
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/")
  return `${encodedBucket}/${encodedKey}`
}

function toValidContentLength(value: unknown): number | null {
  if (typeof value === "number") {
    if (!Number.isFinite(value) || value < 0) return null
    return Math.floor(value)
  }

  if (typeof value === "bigint") {
    const asNumber = Number(value)
    if (!Number.isSafeInteger(asNumber) || asNumber < 0) return null
    return asNumber
  }

  if (typeof value === "string" && value.trim()) {
    const parsed = Number.parseInt(value, 10)
    if (!Number.isFinite(parsed) || parsed < 0) return null
    return parsed
  }

  return null
}

function getS3ErrorStatus(error: unknown): number | null {
  if (!error || typeof error !== "object") return null
  const candidate = error as {
    $metadata?: {
      httpStatusCode?: unknown
    }
  }
  return typeof candidate.$metadata?.httpStatusCode === "number"
    ? candidate.$metadata.httpStatusCode
    : null
}

function getS3ErrorMessage(error: unknown): string {
  if (!error || typeof error !== "object") {
    return error instanceof Error ? error.message : ""
  }

  const candidate = error as {
    message?: unknown
    Message?: unknown
  }
  if (typeof candidate.message === "string") return candidate.message
  if (typeof candidate.Message === "string") return candidate.Message
  return error instanceof Error ? error.message : ""
}

function isEntityTooLargeError(error: unknown): boolean {
  const code = getS3ErrorCode(error)
  if (code.includes("EntityTooLarge")) return true
  return getS3ErrorMessage(error).includes("EntityTooLarge")
}

function isCopyCompatibilityFallbackError(error: unknown): boolean {
  const status = getS3ErrorStatus(error)
  if (status === 405 || status === 501) return true

  const code = getS3ErrorCode(error)
  return code.includes("NotImplemented") || code.includes("InvalidRequest")
}

function isSameS3Backend(params: {
  sourceEndpoint: string
  destinationEndpoint: string
  sourceRegion: string
  destinationRegion: string
  sourceProvider: string
  destinationProvider: string
}): boolean {
  return (
    params.sourceEndpoint.trim().toLowerCase() === params.destinationEndpoint.trim().toLowerCase() &&
    params.sourceRegion.trim() === params.destinationRegion.trim() &&
    params.sourceProvider.trim().toUpperCase() === params.destinationProvider.trim().toUpperCase()
  )
}

function selectTransferStrategy(params: {
  sameCredential: boolean
  sourceSizeBytes: bigint | null
  sourceEndpoint: string
  destinationEndpoint: string
  sourceRegion: string
  destinationRegion: string
  sourceProvider: string
  destinationProvider: string
}): TransferStrategy {
  if (
    params.sameCredential &&
    isSameS3Backend({
      sourceEndpoint: params.sourceEndpoint,
      destinationEndpoint: params.destinationEndpoint,
      sourceRegion: params.sourceRegion,
      destinationRegion: params.destinationRegion,
      sourceProvider: params.sourceProvider,
      destinationProvider: params.destinationProvider,
    })
  ) {
    if (params.sourceSizeBytes !== null && params.sourceSizeBytes > SINGLE_REQUEST_COPY_MAX_BYTES) {
      return "multipart_server_copy"
    }
    return "single_request_server_copy"
  }

  return "multipart_relay_upload"
}

function computeMultipartPartSizeBytes(sourceSizeBytes: bigint): bigint {
  const partCountFloor = (
    sourceSizeBytes + BigInt(MAX_MULTIPART_PARTS) - BigInt(1)
  ) / BigInt(MAX_MULTIPART_PARTS)
  const minimumSize = partCountFloor > DEFAULT_MULTIPART_PART_SIZE_BIGINT
    ? partCountFloor
    : DEFAULT_MULTIPART_PART_SIZE_BIGINT
  return (
    (minimumSize + ONE_MEBIBYTE_BIGINT - BigInt(1)) / ONE_MEBIBYTE_BIGINT
  ) * ONE_MEBIBYTE_BIGINT
}

async function readSourceObjectHeadDetails(params: {
  sourceClient: S3Client
  sourceBucket: string
  sourceKey: string
  expectedContentLength?: unknown
}): Promise<{
  sizeBytes: bigint | null
  contentType: string | undefined
  cacheControl: string | undefined
}> {
  const response = await params.sourceClient.send(
    new HeadObjectCommand({
      Bucket: params.sourceBucket,
      Key: params.sourceKey,
    })
  )

  const size =
    toValidContentLength(response.ContentLength) ??
    toValidContentLength(params.expectedContentLength)

  return {
    sizeBytes: size === null ? null : BigInt(size),
    contentType: typeof response.ContentType === "string" ? response.ContentType : undefined,
    cacheControl: typeof response.CacheControl === "string" ? response.CacheControl : undefined,
  }
}

interface RemoteObjectSnapshot {
  size: bigint | null
  lastModified: Date | null
}

function getS3ErrorCode(error: unknown): string {
  if (!error || typeof error !== "object") return ""
  const candidate = error as { Code?: unknown; code?: unknown; name?: unknown }

  if (typeof candidate.Code === "string") return candidate.Code
  if (typeof candidate.code === "string") return candidate.code
  if (typeof candidate.name === "string") return candidate.name
  return ""
}

function isS3MissingObjectError(error: unknown): boolean {
  const code = getS3ErrorCode(error)
  if (
    code === "NoSuchKey" ||
    code === "NotFound" ||
    code === "NoSuchObject" ||
    code === "404"
  ) {
    return true
  }

  if (!error || typeof error !== "object") return false
  const candidate = error as {
    $metadata?: {
      httpStatusCode?: unknown
    }
  }
  return candidate.$metadata?.httpStatusCode === 404
}

async function readRemoteObjectSnapshot(params: {
  client: S3Client
  bucket: string
  key: string
}): Promise<RemoteObjectSnapshot | null> {
  try {
    const response = await params.client.send(
      new HeadObjectCommand({
        Bucket: params.bucket,
        Key: params.key,
      })
    )

    const size = toValidContentLength(response.ContentLength)
    const lastModified = response.LastModified instanceof Date ? response.LastModified : null

    return {
      size: size === null ? null : BigInt(size),
      lastModified,
    }
  } catch (error) {
    if (isS3MissingObjectError(error)) {
      return null
    }
    throw error
  }
}

function formatTaskProcessingError(error: unknown): string {
  if (error instanceof Error && error.message && error.message !== "UnknownError") {
    return error.message
  }

  if (!error || typeof error !== "object") {
    if (error instanceof Error && error.message) return error.message
    return "Task processing failed"
  }

  const candidate = error as {
    name?: unknown
    code?: unknown
    Code?: unknown
    message?: unknown
    Message?: unknown
    $metadata?: unknown
  }

  const code =
    typeof candidate.Code === "string"
      ? candidate.Code
      : typeof candidate.code === "string"
        ? candidate.code
        : ""
  const name = typeof candidate.name === "string" ? candidate.name : ""
  const message =
    typeof candidate.message === "string"
      ? candidate.message
      : typeof candidate.Message === "string"
        ? candidate.Message
        : ""

  let status: number | null = null
  let requestId: string | null = null
  if (candidate.$metadata && typeof candidate.$metadata === "object") {
    const metadata = candidate.$metadata as {
      httpStatusCode?: unknown
      requestId?: unknown
    }
    if (typeof metadata.httpStatusCode === "number") {
      status = metadata.httpStatusCode
    }
    if (typeof metadata.requestId === "string" && metadata.requestId.trim()) {
      requestId = metadata.requestId.trim()
    }
  }

  const baseMessage =
    [message, code, name].find((value) => value && value !== "UnknownError") ??
    "UnknownError"

  const details: string[] = []
  if (status !== null) details.push(`status ${status}`)
  if (requestId) details.push(`request ${requestId}`)
  if (code && code !== baseMessage) details.push(`code ${code}`)
  if (name && name !== baseMessage && name !== code) details.push(`name ${name}`)

  return details.length > 0 ? `${baseMessage} (${details.join(", ")})` : baseMessage
}

function addTaskHistoryEntry(
  current: unknown,
  entry: Omit<TaskExecutionHistoryEntry, "at">
): Prisma.InputJsonValue {
  return appendExecutionHistory(current, {
    at: new Date().toISOString(),
    ...entry,
  }) as unknown as Prisma.InputJsonValue
}

function buildProcessedResponse(
  snapshot: WorkerTaskSnapshot,
  body: Record<string, unknown> = {}
) {
  return NextResponse.json({
    processed: true,
    taskId: snapshot.taskId,
    taskType: snapshot.taskType,
    taskStatus: snapshot.taskStatus,
    runCount: snapshot.runCount,
    attempts: snapshot.attempts,
    lastError: snapshot.lastError,
    taskUserId: snapshot.taskUserId,
    ...body,
  })
}

function getBackgroundTaskStringFieldValue(
  value: string | Prisma.StringFieldUpdateOperationsInput | undefined
): string | null {
  if (typeof value === "string") return value
  if (
    value &&
    typeof value === "object" &&
    "set" in value &&
    typeof value.set === "string"
  ) {
    return value.set
  }
  return null
}

async function loadClaimedTaskControlState(taskId: string): Promise<ClaimedTaskControlState | null> {
  return prisma.backgroundTask.findUnique({
    where: { id: taskId },
    select: {
      status: true,
      lifecycleState: true,
      pausedAt: true,
    },
  })
}

function buildPauseCheckpointUpdate(
  normalUpdate: Prisma.BackgroundTaskUpdateManyMutationInput,
  now: Date,
  pausedAt: Date | null
): Prisma.BackgroundTaskUpdateManyMutationInput {
  return {
    ...normalUpdate,
    status: "pending",
    lifecycleState: "paused",
    pausedAt: pausedAt ?? now,
    nextRunAt: new Date(now.getTime() + PAUSE_HOLD_MS),
    completedAt: null,
  }
}

function buildCancelCheckpointUpdate(
  normalUpdate: Prisma.BackgroundTaskUpdateManyMutationInput,
  now: Date
): Prisma.BackgroundTaskUpdateManyMutationInput {
  return {
    ...normalUpdate,
    status: "canceled",
    lifecycleState: "canceled",
    pausedAt: null,
    attempts: 0,
    lastError: null,
    completedAt: now,
    nextRunAt: now,
    isRecurring: false,
    scheduleCron: null,
    scheduleIntervalSeconds: null,
  }
}

async function persistClaimedTaskCheckpoint(
  params: PersistClaimedTaskCheckpointParams
): Promise<PersistClaimedTaskCheckpointResult> {
  const now = new Date()
  const normalStatus = getBackgroundTaskStringFieldValue(params.normalUpdate.status) ?? "in_progress"
  const normalIsTerminal = normalStatus === "completed" || normalStatus === "failed"

  for (let attempt = 0; attempt < 3; attempt++) {
    const controlState = await loadClaimedTaskControlState(params.taskId)
    if (controlState && controlState.status !== "in_progress") {
      return {
        applied: false,
        appliedMode: "normal",
        finalStatus: controlState.status,
      }
    }

    let appliedMode: PersistClaimedTaskCheckpointResult["appliedMode"] = "normal"
    let selectedUpdate = params.normalUpdate

    if (!(params.preferTerminal && normalIsTerminal)) {
      if (controlState?.lifecycleState === "canceled") {
        appliedMode = "canceled"
        selectedUpdate = params.cancelUpdate ?? buildCancelCheckpointUpdate(params.normalUpdate, now)
      } else if (controlState?.lifecycleState === "paused") {
        appliedMode = "paused"
        selectedUpdate =
          params.pauseUpdate ??
          buildPauseCheckpointUpdate(params.normalUpdate, now, controlState.pausedAt)
      }
    }

    const applied = await prisma.backgroundTask.updateMany({
      where: {
        id: params.taskId,
        userId: params.userId,
        runCount: params.claimedRunCount,
        status: "in_progress",
        ...(controlState ? { lifecycleState: controlState.lifecycleState } : {}),
      },
      data: selectedUpdate,
    })

    const selectedStatus =
      getBackgroundTaskStringFieldValue(selectedUpdate.status) ?? normalStatus

    if (applied.count > 0) {
      return {
        applied: true,
        appliedMode,
        finalStatus: selectedStatus,
      }
    }
  }

  const current = await prisma.backgroundTask.findFirst({
    where: {
      id: params.taskId,
      userId: params.userId,
    },
    select: {
      status: true,
      lifecycleState: true,
    },
  })

  return {
    applied: false,
    appliedMode: current?.lifecycleState === "canceled"
      ? "canceled"
      : current?.lifecycleState === "paused"
        ? "paused"
        : "normal",
    finalStatus: current?.status ?? normalStatus,
  }
}

async function upsertFileMetadataBatch(rows: TransferMetadataUpsertRow[]): Promise<void> {
  if (rows.length === 0) return

  const dedupedRows = Array.from(
    rows.reduce((map, row) => {
      map.set(`${row.credentialId}::${row.bucket}::${row.key}`, row)
      return map
    }, new Map<string, TransferMetadataUpsertRow>()).values()
  )
  if (dedupedRows.length === 0) return

  const values = Prisma.join(
    dedupedRows.map((row) => Prisma.sql`(
      ${crypto.randomUUID()},
      ${row.userId},
      ${row.credentialId},
      ${row.bucket},
      ${row.key},
      ${row.extension},
      ${row.size},
      ${row.lastModified},
      false
    )`)
  )

  await prisma.$executeRaw(Prisma.sql`
    INSERT INTO "FileMetadata" (
      "id",
      "userId",
      "credentialId",
      "bucket",
      "key",
      "extension",
      "size",
      "lastModified",
      "isFolder"
    )
    VALUES ${values}
    ON CONFLICT ("credentialId", "bucket", "key")
    DO UPDATE SET
      "userId" = EXCLUDED."userId",
      "extension" = EXCLUDED."extension",
      "size" = EXCLUDED."size",
      "lastModified" = EXCLUDED."lastModified",
      "isFolder" = EXCLUDED."isFolder"
  `)
}

async function realignFutureRecurringRun(params: {
  userId: string
  now: Date
  graceMs: number
}): Promise<boolean> {
  const { userId, now, graceMs } = params
  const candidate = await prisma.backgroundTask.findFirst({
    where: {
      userId,
      lifecycleState: "active",
      status: "pending",
      isRecurring: true,
      type: {
        in: ["bulk_delete", "object_transfer", "database_backup"],
      },
      nextRunAt: {
        gt: now,
      },
    },
    orderBy: {
      createdAt: "asc",
    },
    select: {
      id: true,
      nextRunAt: true,
      scheduleCron: true,
      scheduleIntervalSeconds: true,
      executionHistory: true,
    },
  })

  if (!candidate) return false

  const schedule = resolveTaskSchedule({
    isRecurring: true,
    scheduleCron: candidate.scheduleCron,
    scheduleIntervalSeconds: candidate.scheduleIntervalSeconds,
  })
  if (!schedule.enabled) return false

  const expectedNextRunAt = nextRunAtForTaskSchedule(schedule, now)
  if (!expectedNextRunAt) return false

  if (candidate.nextRunAt.getTime() - expectedNextRunAt.getTime() <= graceMs) {
    return false
  }

  const updated = await prisma.backgroundTask.updateMany({
    where: {
      id: candidate.id,
      userId,
      lifecycleState: "active",
      status: "pending",
      nextRunAt: candidate.nextRunAt,
    },
    data: {
      nextRunAt: expectedNextRunAt,
      lastError: null,
      executionHistory: addTaskHistoryEntry(
        normalizeExecutionHistory(candidate.executionHistory),
        {
          status: "skipped",
          message: "Realigned scheduled run to server time",
          metadata: {
            previousNextRunAt: candidate.nextRunAt.toISOString(),
            nextRunAt: expectedNextRunAt.toISOString(),
            realignedAt: now.toISOString(),
          },
        }
      ),
    },
  })

  return updated.count > 0
}

function resolveTaskPlanPayload(executionPlan: unknown, fallbackPayload: unknown): unknown {
  if (!executionPlan || typeof executionPlan !== "object") {
    return fallbackPayload
  }

  const candidate = executionPlan as { payload?: unknown }
  return candidate.payload ?? fallbackPayload
}

async function copyObjectAcrossLocations(params: {
  sourceClient: S3Client
  destinationClient: S3Client
  sameCredential: boolean
  sourceEndpoint: string
  destinationEndpoint: string
  sourceRegion: string
  destinationRegion: string
  sourceProvider: string
  destinationProvider: string
  sourceBucket: string
  sourceKey: string
  destinationBucket: string
  destinationKey: string
  expectedContentLength?: unknown
}) {
  async function multipartRelayObjectAcrossLocations(): Promise<void> {
    const sourceObject = await params.sourceClient.send(
      new GetObjectCommand({
        Bucket: params.sourceBucket,
        Key: params.sourceKey,
      })
    )

    if (!sourceObject.Body) {
      throw new Error(`Missing source object body for key '${params.sourceKey}'`)
    }

    const contentLength =
      toValidContentLength(sourceObject.ContentLength) ??
      toValidContentLength(params.expectedContentLength)

    const upload = new Upload({
      client: params.destinationClient,
      params: {
        Bucket: params.destinationBucket,
        Key: params.destinationKey,
        Body: sourceObject.Body,
        ContentType: sourceObject.ContentType,
        CacheControl: sourceObject.CacheControl,
        ...(contentLength !== null ? { ContentLength: contentLength } : {}),
      },
      queueSize: RELAY_UPLOAD_QUEUE_SIZE,
      partSize: DEFAULT_MULTIPART_PART_SIZE_BYTES,
      leavePartsOnError: false,
    })

    await upload.done()
  }

  async function multipartCopyObjectWithinBackend(): Promise<boolean> {
    const headDetails = await readSourceObjectHeadDetails({
      sourceClient: params.sourceClient,
      sourceBucket: params.sourceBucket,
      sourceKey: params.sourceKey,
      expectedContentLength: params.expectedContentLength,
    })

    if (!headDetails.sizeBytes || headDetails.sizeBytes <= BigInt(0)) {
      return false
    }

    const createResponse = await params.destinationClient.send(
      new CreateMultipartUploadCommand({
        Bucket: params.destinationBucket,
        Key: params.destinationKey,
        ...(headDetails.contentType ? { ContentType: headDetails.contentType } : {}),
        ...(headDetails.cacheControl ? { CacheControl: headDetails.cacheControl } : {}),
      })
    )

    const uploadId = createResponse.UploadId
    if (!uploadId) {
      throw new Error(`Failed to start multipart copy for key '${params.destinationKey}'`)
    }

    const completedParts: Array<{ ETag: string; PartNumber: number }> = []
    const partSizeBytes = computeMultipartPartSizeBytes(headDetails.sizeBytes)
    let offset = BigInt(0)
    let partNumber = 1

    try {
      while (offset < headDetails.sizeBytes) {
        const nextOffset = offset + partSizeBytes < headDetails.sizeBytes
          ? offset + partSizeBytes
          : headDetails.sizeBytes
        const rangeEnd = nextOffset - BigInt(1)

        const partResponse = await params.destinationClient.send(
          new UploadPartCopyCommand({
            Bucket: params.destinationBucket,
            Key: params.destinationKey,
            UploadId: uploadId,
            PartNumber: partNumber,
            CopySource: buildCopySource(params.sourceBucket, params.sourceKey),
            CopySourceRange: `bytes=${offset.toString()}-${rangeEnd.toString()}`,
          })
        )

        const etag = partResponse.CopyPartResult?.ETag
        if (!etag) {
          throw new Error(
            `Multipart copy part ${partNumber} did not return an ETag for key '${params.destinationKey}'`
          )
        }

        completedParts.push({
          ETag: etag,
          PartNumber: partNumber,
        })

        offset = nextOffset
        partNumber += 1
      }

      await params.destinationClient.send(
        new CompleteMultipartUploadCommand({
          Bucket: params.destinationBucket,
          Key: params.destinationKey,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: completedParts,
          },
        })
      )

      return true
    } catch (error) {
      await params.destinationClient.send(
        new AbortMultipartUploadCommand({
          Bucket: params.destinationBucket,
          Key: params.destinationKey,
          UploadId: uploadId,
        })
      ).catch(() => {})
      throw error
    }
  }

  const sourceSizeBytes = (() => {
    const contentLength = toValidContentLength(params.expectedContentLength)
    return contentLength === null ? null : BigInt(contentLength)
  })()

  const strategy = selectTransferStrategy({
    sameCredential: params.sameCredential,
    sourceSizeBytes,
    sourceEndpoint: params.sourceEndpoint,
    destinationEndpoint: params.destinationEndpoint,
    sourceRegion: params.sourceRegion,
    destinationRegion: params.destinationRegion,
    sourceProvider: params.sourceProvider,
    destinationProvider: params.destinationProvider,
  })

  if (strategy === "multipart_server_copy") {
    const copied = await multipartCopyObjectWithinBackend()
    if (copied) return
    await multipartRelayObjectAcrossLocations()
    return
  }

  if (strategy === "single_request_server_copy") {
    try {
      await params.destinationClient.send(
        new CopyObjectCommand({
          Bucket: params.destinationBucket,
          CopySource: buildCopySource(params.sourceBucket, params.sourceKey),
          Key: params.destinationKey,
        })
      )
      return
    } catch (error) {
      if (isEntityTooLargeError(error)) {
        const copied = await multipartCopyObjectWithinBackend()
        if (copied) return
        await multipartRelayObjectAcrossLocations()
        return
      }

      if (isCopyCompatibilityFallbackError(error)) {
        await multipartRelayObjectAcrossLocations()
        return
      }

      throw error
    }
  }

  await multipartRelayObjectAcrossLocations()
}

async function deleteKeysFromBucket(
  client: InstanceType<typeof import("@aws-sdk/client-s3").S3Client>,
  bucket: string,
  keys: string[]
): Promise<Set<string>> {
  const deletedKeys = new Set<string>()

  for (let i = 0; i < keys.length; i += 1000) {
    const batch = keys.slice(i, i + 1000)

    const response = await client.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
          Objects: batch.map((Key) => ({ Key })),
          Quiet: false,
        },
      })
    )

    const deletedInResponse = (response.Deleted ?? [])
      .map((item) => item.Key)
      .filter((key): key is string => Boolean(key))

    if (deletedInResponse.length > 0) {
      for (const key of deletedInResponse) {
        deletedKeys.add(key)
      }
      continue
    }

    const failedKeys = new Set(
      (response.Errors ?? [])
        .map((item) => item.Key)
        .filter((key): key is string => Boolean(key))
    )

    for (const key of batch) {
      if (!failedKeys.has(key)) {
        deletedKeys.add(key)
      }
    }
  }

  return deletedKeys
}

interface SyncDestinationDriftRow {
  key: string
}

async function findSyncDestinationDriftBatch(params: {
  userId: string
  payload: ObjectTransferTaskPayload
}): Promise<SyncDestinationDriftRow[]> {
  const { userId, payload } = params
  const limit = getTaskTransferBatchSize()

  if (payload.scope === "bucket") {
    return prisma.$queryRaw<SyncDestinationDriftRow[]>(Prisma.sql`
      SELECT d."key"
      FROM "FileMetadata" d
      WHERE d."userId" = ${userId}
        AND d."credentialId" = ${payload.destinationCredentialId}
        AND d."bucket" = ${payload.destinationBucket}
        AND d."isFolder" = false
        AND NOT EXISTS (
          SELECT 1
          FROM "FileMetadata" s
          WHERE s."userId" = ${userId}
            AND s."credentialId" = ${payload.sourceCredentialId}
            AND s."bucket" = ${payload.sourceBucket}
            AND s."isFolder" = false
            AND s."key" = d."key"
        )
      ORDER BY d."key" ASC
      LIMIT ${limit}
    `)
  }

  const sourcePrefix = payload.sourcePrefix ?? ""
  const destinationPrefix = payload.destinationPrefix ?? ""
  const destinationPrefixLength = destinationPrefix.length
  const substringStart = destinationPrefixLength + 1

  return prisma.$queryRaw<SyncDestinationDriftRow[]>(Prisma.sql`
    SELECT d."key"
    FROM "FileMetadata" d
    WHERE d."userId" = ${userId}
      AND d."credentialId" = ${payload.destinationCredentialId}
      AND d."bucket" = ${payload.destinationBucket}
      AND d."isFolder" = false
      AND LEFT(d."key", ${destinationPrefixLength}) = ${destinationPrefix}
      AND NOT EXISTS (
        SELECT 1
        FROM "FileMetadata" s
        WHERE s."userId" = ${userId}
          AND s."credentialId" = ${payload.sourceCredentialId}
          AND s."bucket" = ${payload.sourceBucket}
          AND s."isFolder" = false
          AND s."key" = ${sourcePrefix} || substring(d."key" from ${substringStart})
        )
    ORDER BY d."key" ASC
    LIMIT ${limit}
  `)
}

async function cleanupSyncDestinationDrift(params: {
  userId: string
  payload: ObjectTransferTaskPayload
  destinationClient: S3Client
}): Promise<{ deleted: number; failed: number }> {
  const { userId, payload, destinationClient } = params
  let deleted = 0
  let failed = 0

  while (true) {
    const driftRows = await findSyncDestinationDriftBatch({ userId, payload })
    if (driftRows.length === 0) {
      break
    }

    const driftKeys = driftRows.map((row) => row.key)
    const deletedKeys = await deleteKeysFromBucket(
      destinationClient,
      payload.destinationBucket,
      driftKeys
    )
    if (deletedKeys.size === 0) {
      failed += driftKeys.length
      break
    }

    const deletedKeyList = Array.from(deletedKeys)
    await prisma.fileMetadata.deleteMany({
      where: {
        userId,
        credentialId: payload.destinationCredentialId,
        bucket: payload.destinationBucket,
        key: { in: deletedKeyList },
      },
    })
    deleted += deletedKeyList.length
    failed += Math.max(0, driftKeys.length - deletedKeys.size)
  }

  return { deleted, failed }
}

export async function POST(request: Request) {
  let claimedTask:
    | {
      id: string
      type: string
      runCount: number
      attempts: number
      maxAttempts: number
    }
    | null = null
  let userId: string | null = null
  let transferPayload: ObjectTransferTaskPayload | null = null
  let taskExecutionHistory: TaskExecutionHistoryEntry[] = []
  let claimedTaskSchedule: ResolvedTaskSchedule | null = null

  try {
    const internalToken = getTaskEngineInternalToken()
    const requestToken = (request.headers.get("x-task-engine-token") ?? "").trim()
    const requestedUserId = (new URL(request.url).searchParams.get("userId") ?? "").trim()

    if (internalToken && requestToken === internalToken && requestedUserId) {
      userId = requestedUserId
    } else {
      const session = await auth()
      if (!session?.user?.id) {
        return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
      }
      userId = session.user.id
    }

    if (!userId) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }
    const actorUserId = userId

    const TASK_TYPES = ["bulk_delete", "object_transfer", "database_backup"] as const
    const requestedType = (new URL(request.url).searchParams.get("type") ?? "").trim()
    const typeFilter = requestedType && TASK_TYPES.includes(requestedType as typeof TASK_TYPES[number])
      ? [requestedType]
      : [...TASK_TYPES]

    const now = new Date()
    const maxActive = getTaskMaxActivePerUser()

    // Per-type concurrency: each type gets a reserved share of the total slots
    // so one type (e.g. bulk_delete) can never starve another (e.g. object_transfer).
    // Reserved slots = floor(maxActive / number_of_types), minimum 1.
    // Remaining slots are available to any type on a first-come basis.
    const typeCount = TASK_TYPES.length
    const reservedPerType = Math.max(1, Math.floor(maxActive / typeCount))

    const lockedByType = await prisma.backgroundTask.groupBy({
      by: ["type"],
      where: {
        userId: actorUserId,
        lifecycleState: "active",
        status: "in_progress",
        nextRunAt: { gt: now },
      },
      _count: { _all: true },
    })

    const lockedCounts = new Map(lockedByType.map((r) => [r.type, r._count._all]))
    const totalLocked = lockedByType.reduce((sum, r) => sum + r._count._all, 0)
    const requestedTypeName = typeFilter.length === 1 ? typeFilter[0] : null
    const lockedForRequestedType = requestedTypeName ? (lockedCounts.get(requestedTypeName) ?? 0) : totalLocked

    // Block if: this type already used its reserved slots AND overall limit is reached
    if (totalLocked >= maxActive && lockedForRequestedType >= reservedPerType) {
      return NextResponse.json({
        processed: false,
        message: "Task concurrency limit reached for user",
      })
    }

    const staleScheduleGraceMs = getTaskMissedScheduleGraceSeconds() * 1000
    const realignedFutureSchedule = await realignFutureRecurringRun({
      userId: actorUserId,
      now,
      graceMs: staleScheduleGraceMs,
    })
    let skippedStaleSchedules = 0
    let candidate: Awaited<ReturnType<typeof prisma.backgroundTask.findFirst>> = null

    for (let index = 0; index < MAX_STALE_SCHEDULE_SKIPS_PER_CALL; index++) {
      const nextCandidate = await prisma.backgroundTask.findFirst({
        where: {
          userId: actorUserId,
          lifecycleState: "active",
          type: {
            in: typeFilter,
          },
          status: {
            in: ["pending", "in_progress"],
          },
          nextRunAt: {
            lte: now,
          },
        },
        orderBy: {
          createdAt: "asc",
        },
      })

      if (!nextCandidate) {
        break
      }

      const scheduled = resolveTaskSchedule(nextCandidate)
      if (nextCandidate.status === "pending" && nextCandidate.isRecurring && !scheduled.enabled) {
        const disabled = await prisma.backgroundTask.updateMany({
          where: {
            id: nextCandidate.id,
            userId: actorUserId,
            lifecycleState: "active",
            status: "pending",
            isRecurring: true,
            nextRunAt: {
              lte: now,
            },
          },
          data: {
            status: "completed",
            completedAt: now,
            nextRunAt: now,
            isRecurring: false,
            scheduleCron: null,
            scheduleIntervalSeconds: null,
            lastError: null,
            executionHistory: addTaskHistoryEntry(
              normalizeExecutionHistory(nextCandidate.executionHistory),
              {
                status: "skipped",
                message: "Disabled scheduled task after cron support was removed",
                metadata: {
                  disabledAt: now.toISOString(),
                },
              }
            ),
          },
        })
        if (disabled.count > 0) {
          skippedStaleSchedules += 1
        }
        continue
      }

      const shouldSkipStaleRun =
        nextCandidate.status === "pending" &&
        scheduled.enabled &&
        now.getTime() - nextCandidate.nextRunAt.getTime() > staleScheduleGraceMs

      if (!shouldSkipStaleRun) {
        candidate = nextCandidate
        break
      }

      const nextRunAt =
        nextRunAtForTaskSchedule(scheduled, now) ??
        new Date(now.getTime() + SYNC_POLL_INTERVAL_SECONDS * 1000)
      const moved = await prisma.backgroundTask.updateMany({
        where: {
          id: nextCandidate.id,
          userId: actorUserId,
          lifecycleState: "active",
          status: "pending",
          nextRunAt: {
            lte: now,
          },
        },
        data: {
          nextRunAt,
          lastError: null,
          executionHistory: addTaskHistoryEntry(
            normalizeExecutionHistory(nextCandidate.executionHistory),
            {
              status: "skipped",
              message: "Skipped stale scheduled run after downtime",
              metadata: {
                previousNextRunAt: nextCandidate.nextRunAt.toISOString(),
                nextRunAt: nextRunAt.toISOString(),
                skippedAt: now.toISOString(),
              },
            }
          ),
        },
      })
      if (moved.count > 0) {
        skippedStaleSchedules += 1
      }
    }

    if (!candidate) {
      if (skippedStaleSchedules > 0) {
        return NextResponse.json({
          processed: false,
          message: "Skipped stale scheduled runs",
          skippedStaleSchedules,
        })
      }
      if (realignedFutureSchedule) {
        return NextResponse.json({
          processed: false,
          message: "Realigned future scheduled run to server time",
        })
      }
      return NextResponse.json({ processed: false, message: "No pending tasks" })
    }

    const lockUntil = new Date(Date.now() + LOCK_SECONDS * 1000)
    const claimed = await prisma.backgroundTask.updateMany({
      where: {
        id: candidate.id,
        userId: actorUserId,
        lifecycleState: "active",
        status: {
          in: ["pending", "in_progress"],
        },
        nextRunAt: {
          lte: now,
        },
      },
      data: {
        status: "in_progress",
        startedAt: candidate.startedAt ?? now,
        runCount: {
          increment: 1,
        },
        lastRunAt: now,
        nextRunAt: lockUntil,
      },
    })

    if (claimed.count === 0) {
      return NextResponse.json({ processed: false, message: "Task is already being processed" })
    }
    claimedTask = {
      id: candidate.id,
      type: candidate.type,
      runCount: candidate.runCount + 1,
      attempts: candidate.attempts,
      maxAttempts: candidate.maxAttempts,
    }
    claimedTaskSchedule = resolveTaskSchedule(candidate)
    taskExecutionHistory = normalizeExecutionHistory(candidate.executionHistory)

    if (candidate.type === "database_backup") {
      try {
        const { runBackup } = await import("@/lib/backup")
        await runBackup()
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err)
        const nextAttempts = candidate.attempts + 1
        const willRetry = nextAttempts < candidate.maxAttempts
        const nextScheduledRunAt = claimedTaskSchedule?.enabled
          ? nextRunAtForTaskSchedule(claimedTaskSchedule, new Date()) ?? new Date()
          : null
        const nextRunAt = willRetry
          ? new Date(Date.now() + Math.min(nextAttempts * 60_000, 30 * 60_000))
          : nextScheduledRunAt ?? new Date()
        const failureCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          normalUpdate: {
            status: willRetry ? "pending" : "failed",
            attempts: nextAttempts,
            lastError: message.slice(0, 500),
            nextRunAt,
            ...(willRetry || nextScheduledRunAt
              ? {}
              : {
                  isRecurring: false,
                  scheduleCron: null,
                  scheduleIntervalSeconds: null,
                }),
            executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
              status: "failed",
              message,
            }),
          },
        })
        return buildProcessedResponse(
          {
            taskId: candidate.id,
            taskType: candidate.type,
            taskStatus: failureCheckpoint.finalStatus,
            runCount: candidate.runCount + 1,
            attempts: failureCheckpoint.appliedMode === "canceled" ? 0 : nextAttempts,
            lastError: failureCheckpoint.appliedMode === "canceled" ? null : message.slice(0, 500),
            taskUserId: actorUserId,
          },
          {
            done: failureCheckpoint.appliedMode === "canceled",
            error: message,
          }
        )
      }

      const completedAt = new Date()
      const nextScheduledRunAt = claimedTaskSchedule?.enabled
        ? nextRunAtForTaskSchedule(claimedTaskSchedule, completedAt) ?? completedAt
        : null
      const successCheckpoint = await persistClaimedTaskCheckpoint({
        taskId: candidate.id,
        userId: actorUserId,
        claimedRunCount: candidate.runCount + 1,
        preferTerminal: true,
        normalUpdate: {
          status: nextScheduledRunAt ? "pending" : "completed",
          lifecycleState: "active",
          attempts: 0,
          lastError: null,
          lastRunAt: completedAt,
          completedAt: nextScheduledRunAt ? null : completedAt,
          nextRunAt: nextScheduledRunAt ?? completedAt,
          ...(nextScheduledRunAt
            ? {}
            : {
                isRecurring: false,
                scheduleCron: null,
                scheduleIntervalSeconds: null,
              }),
          executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
            status: "succeeded",
            message: "Backup completed",
            metadata: nextScheduledRunAt
              ? { nextRunAt: nextScheduledRunAt.toISOString() }
              : undefined,
          }),
        },
      })
      return buildProcessedResponse(
        {
          taskId: candidate.id,
          taskType: candidate.type,
          taskStatus: successCheckpoint.finalStatus,
          runCount: candidate.runCount + 1,
          attempts: 0,
          lastError: null,
          taskUserId: actorUserId,
        },
        {
          done: !nextScheduledRunAt,
          type: "database_backup",
        }
      )
    }

    if (candidate.type === "object_transfer") {
      const planPayload = resolveTaskPlanPayload(candidate.executionPlan, candidate.payload)
      transferPayload = parseObjectTransferPayload(planPayload)
      if (!transferPayload) {
        const invalidPayloadCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          preferTerminal: true,
          normalUpdate: {
            status: "failed",
            lifecycleState: "active",
            attempts: candidate.attempts + 1,
            lastError: "Invalid object transfer payload",
            completedAt: new Date(),
            nextRunAt: new Date(),
            executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
              status: "failed",
              message: "Invalid object transfer payload",
            }),
          },
        })
        return buildProcessedResponse(
          {
            taskId: candidate.id,
            taskType: candidate.type,
            taskStatus: invalidPayloadCheckpoint.finalStatus,
            runCount: candidate.runCount + 1,
            attempts: invalidPayloadCheckpoint.appliedMode === "canceled" ? 0 : candidate.attempts + 1,
            lastError:
              invalidPayloadCheckpoint.appliedMode === "canceled"
                ? null
                : "Invalid object transfer payload",
            taskUserId: actorUserId,
          },
          {
            done: true,
            type: "object_transfer",
            error: "Invalid object transfer payload",
          }
        )
      }

      const entitlements = await getUserPlanEntitlements(actorUserId)
      if (!entitlements) {
        const entitlementCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          preferTerminal: true,
          normalUpdate: {
            status: "failed",
            lifecycleState: "active",
            attempts: candidate.attempts + 1,
            completedAt: new Date(),
            nextRunAt: new Date(),
            lastError: "Failed to resolve plan entitlements",
            executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
              status: "failed",
              message: "Failed to resolve plan entitlements",
            }),
          },
        })
        return buildProcessedResponse(
          {
            taskId: candidate.id,
            taskType: candidate.type,
            taskStatus: entitlementCheckpoint.finalStatus,
            runCount: candidate.runCount + 1,
            attempts: entitlementCheckpoint.appliedMode === "canceled" ? 0 : candidate.attempts + 1,
            lastError:
              entitlementCheckpoint.appliedMode === "canceled"
                ? null
                : "Failed to resolve plan entitlements",
            taskUserId: actorUserId,
          },
          {
            done: true,
            type: "object_transfer",
            error: "Failed to resolve plan entitlements",
          }
        )
      }

      const activeTransferPayload = transferPayload
      const destinationContextChanged =
        activeTransferPayload.sourceCredentialId !== activeTransferPayload.destinationCredentialId ||
        activeTransferPayload.sourceBucket !== activeTransferPayload.destinationBucket
      if (destinationContextChanged) {
        const bucketLimitViolation = await getBucketLimitViolation({
          userId: actorUserId,
          credentialId: activeTransferPayload.destinationCredentialId,
          bucket: activeTransferPayload.destinationBucket,
          entitlements,
        })
        if (bucketLimitViolation) {
          const bucketLimitCheckpoint = await persistClaimedTaskCheckpoint({
            taskId: candidate.id,
            userId: actorUserId,
            claimedRunCount: candidate.runCount + 1,
            preferTerminal: true,
            normalUpdate: {
              status: "failed",
              lifecycleState: "active",
              attempts: candidate.attempts + 1,
              completedAt: new Date(),
              nextRunAt: new Date(),
              lastError: "Bucket limit reached for current plan",
              executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
                status: "failed",
                message: "Bucket limit reached for current plan",
              }),
            },
          })

          return buildProcessedResponse(
            {
              taskId: candidate.id,
              taskType: candidate.type,
              taskStatus: bucketLimitCheckpoint.finalStatus,
              runCount: candidate.runCount + 1,
              attempts: bucketLimitCheckpoint.appliedMode === "canceled" ? 0 : candidate.attempts + 1,
              lastError:
                bucketLimitCheckpoint.appliedMode === "canceled"
                  ? null
                  : "Bucket limit reached for current plan",
              taskUserId: actorUserId,
            },
            {
              done: true,
              type: "object_transfer",
              skipped: "bucket_limit_reached",
              details: bucketLimitViolation,
            }
          )
        }
      }

      const progress = parseObjectTransferProgress(candidate.progress)
      const sourceKeyFilter: { startsWith?: string; gt?: string } = {}
      if (activeTransferPayload.scope === "folder" && activeTransferPayload.sourcePrefix) {
        sourceKeyFilter.startsWith = activeTransferPayload.sourcePrefix
      }
      if (progress.cursorKey) {
        sourceKeyFilter.gt = progress.cursorKey
      }

      const sourceBatch = await prisma.fileMetadata.findMany({
        where: {
          userId: actorUserId,
          credentialId: activeTransferPayload.sourceCredentialId,
          bucket: activeTransferPayload.sourceBucket,
          isFolder: false,
          ...(Object.keys(sourceKeyFilter).length > 0 ? { key: sourceKeyFilter } : {}),
        },
        orderBy: { key: "asc" },
        take: getTaskTransferBatchSize(),
        select: {
          id: true,
          key: true,
          extension: true,
          size: true,
          lastModified: true,
        },
      })

      const sourceTotal =
        progress.total > 0
          ? progress.total
          : progress.processed + await prisma.fileMetadata.count({
            where: {
              userId: actorUserId,
              credentialId: activeTransferPayload.sourceCredentialId,
              bucket: activeTransferPayload.sourceBucket,
              isFolder: false,
              ...(Object.keys(sourceKeyFilter).length > 0 ? { key: sourceKeyFilter } : {}),
            },
          })

      if (sourceBatch.length === 0) {
        const total = sourceTotal
        let syncCleanupDeleted = 0
        let syncCleanupFailed = 0

        if (activeTransferPayload.operation === "sync") {
          const { client: destinationClient } = await getS3Client(
            actorUserId,
            activeTransferPayload.destinationCredentialId
          )
          const cleanupResult = await cleanupSyncDestinationDrift({
            userId: actorUserId,
            payload: activeTransferPayload,
            destinationClient,
          })
          syncCleanupDeleted = cleanupResult.deleted
          syncCleanupFailed = cleanupResult.failed
        }

        await rebuildUserExtensionStats(actorUserId)

        const cycleProgress = {
          total,
          processed: progress.processed,
          copied: progress.copied,
          moved: progress.moved,
          deleted: progress.deleted + syncCleanupDeleted,
          skipped: progress.skipped,
          failed: progress.failed + syncCleanupFailed,
        }

        if (claimedTaskSchedule?.enabled) {
          const nextRunAt =
            nextRunAtForTaskSchedule(claimedTaskSchedule, new Date()) ??
            new Date(Date.now() + SYNC_POLL_INTERVAL_SECONDS * 1000)
          const scheduledCycleCheckpoint = await persistClaimedTaskCheckpoint({
            taskId: candidate.id,
            userId: actorUserId,
            claimedRunCount: candidate.runCount + 1,
            normalUpdate: {
              status: "pending",
              attempts: 0,
              completedAt: null,
              nextRunAt,
              lastRunAt: new Date(),
              progress: {
                phase: "transfer",
                total: 0,
                processed: 0,
                copied: 0,
                moved: 0,
                deleted: 0,
                skipped: 0,
                failed: 0,
                remaining: 0,
                cursorKey: null,
              } as Prisma.InputJsonObject,
              lastError: null,
              executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
                status: cycleProgress.failed > 0 ? "failed" : "succeeded",
                message:
                  cycleProgress.failed > 0
                    ? "Scheduled cycle completed with failures"
                    : "Scheduled cycle completed",
                metadata: {
                  nextRunAt: nextRunAt.toISOString(),
                  schedule: claimedTaskSchedule.cron ?? claimedTaskSchedule.legacyIntervalSeconds,
                  progress: cycleProgress,
                },
              }),
            },
          })

          await logUserAuditAction({
            userId: actorUserId,
            eventType: "s3_action",
            eventName: "object_transfer_scheduled_cycle_completed",
            path: "/api/tasks/process",
            method: "POST",
            target: `${activeTransferPayload.sourceBucket} -> ${activeTransferPayload.destinationBucket}`,
            metadata: {
              scope: activeTransferPayload.scope,
              operation: activeTransferPayload.operation,
              sourceCredentialId: activeTransferPayload.sourceCredentialId,
              sourceBucket: activeTransferPayload.sourceBucket,
              sourcePrefix: activeTransferPayload.sourcePrefix,
              destinationCredentialId: activeTransferPayload.destinationCredentialId,
              destinationBucket: activeTransferPayload.destinationBucket,
              destinationPrefix: activeTransferPayload.destinationPrefix,
              nextRunAt: nextRunAt.toISOString(),
              schedule: claimedTaskSchedule.cron ?? claimedTaskSchedule.legacyIntervalSeconds,
              progress: cycleProgress,
              cleanupDeleted: syncCleanupDeleted,
              cleanupFailed: syncCleanupFailed,
            },
          })

          return buildProcessedResponse(
            {
              taskId: candidate.id,
              taskType: candidate.type,
              taskStatus: scheduledCycleCheckpoint.finalStatus,
              runCount: candidate.runCount + 1,
              attempts: scheduledCycleCheckpoint.appliedMode === "canceled" ? 0 : 0,
              lastError: scheduledCycleCheckpoint.appliedMode === "canceled" ? null : null,
              taskUserId: actorUserId,
            },
            {
              done: scheduledCycleCheckpoint.appliedMode === "canceled",
              type: "object_transfer",
              recurring: scheduledCycleCheckpoint.appliedMode === "normal",
              nextRunAt:
                scheduledCycleCheckpoint.appliedMode === "normal"
                  ? nextRunAt.toISOString()
                  : undefined,
              deletedInCleanup: syncCleanupDeleted,
              failedInCleanup: syncCleanupFailed,
            }
          )
        }

        const hasTransferFailures = cycleProgress.failed > 0
        const finalTransferError = hasTransferFailures
          ? candidate.lastError ?? "One or more objects failed during transfer"
          : null

        const finalTransferCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          preferTerminal: true,
          normalUpdate: {
            status: hasTransferFailures ? "failed" : "completed",
            lifecycleState: "active",
            attempts: 0,
            completedAt: new Date(),
            nextRunAt: new Date(),
            progress: {
              ...cycleProgress,
              total,
              remaining: 0,
            } as Prisma.InputJsonObject,
            lastError: finalTransferError,
            executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
              status: hasTransferFailures ? "failed" : "succeeded",
              message: hasTransferFailures
                ? "Transfer completed with failures"
                : "Transfer completed",
              metadata: {
                total,
                processed: cycleProgress.processed,
                copied: cycleProgress.copied,
                moved: cycleProgress.moved,
                deleted: cycleProgress.deleted,
                skipped: cycleProgress.skipped,
                failed: cycleProgress.failed,
              },
            }),
          },
        })

        await logUserAuditAction({
          userId: actorUserId,
          eventType: "s3_action",
          eventName: hasTransferFailures
            ? "object_transfer_failed"
            : "object_transfer_completed",
          path: "/api/tasks/process",
          method: "POST",
          target: `${activeTransferPayload.sourceBucket} -> ${activeTransferPayload.destinationBucket}`,
          metadata: {
            scope: activeTransferPayload.scope,
            operation: activeTransferPayload.operation,
            sourceCredentialId: activeTransferPayload.sourceCredentialId,
            sourceBucket: activeTransferPayload.sourceBucket,
            sourcePrefix: activeTransferPayload.sourcePrefix,
            destinationCredentialId: activeTransferPayload.destinationCredentialId,
            destinationBucket: activeTransferPayload.destinationBucket,
            destinationPrefix: activeTransferPayload.destinationPrefix,
            progress: {
              total,
              processed: progress.processed,
              copied: progress.copied,
              moved: progress.moved,
              deleted: progress.deleted,
              skipped: progress.skipped,
              failed: progress.failed,
            },
          },
        })

        return buildProcessedResponse(
          {
            taskId: candidate.id,
            taskType: candidate.type,
            taskStatus: finalTransferCheckpoint.finalStatus,
            runCount: candidate.runCount + 1,
            attempts: 0,
            lastError: finalTransferError,
            taskUserId: actorUserId,
          },
          {
            done: true,
            type: "object_transfer",
            failed: hasTransferFailures,
          }
        )
      }

      const [sourceClientInfo, destinationClientInfo] = await Promise.all([
        getS3Client(actorUserId, activeTransferPayload.sourceCredentialId),
        getS3Client(actorUserId, activeTransferPayload.destinationCredentialId),
      ])
      const sourceClient = sourceClientInfo.client
      const destinationClient = destinationClientInfo.client

      const sameCredential =
        activeTransferPayload.sourceCredentialId === activeTransferPayload.destinationCredentialId
      const requiresDestinationComparison =
        activeTransferPayload.operation === "copy" || activeTransferPayload.operation === "sync"
      const mappedBatch = sourceBatch.map((sourceFile) => ({
        sourceFile,
        destinationKey: mapTransferDestinationKey(
          activeTransferPayload,
          sourceFile.key
        ),
      }))

      let destinationByKey = new Map<string, TransferDestinationSnapshot>()
      if (requiresDestinationComparison) {
        const destinationRows = await prisma.fileMetadata.findMany({
          where: {
            userId: actorUserId,
            credentialId: activeTransferPayload.destinationCredentialId,
            bucket: activeTransferPayload.destinationBucket,
            isFolder: false,
            key: { in: mappedBatch.map((item) => item.destinationKey) },
          },
          select: {
            key: true,
            size: true,
            lastModified: true,
          },
        })

        destinationByKey = new Map(
          destinationRows.map((row) => [
            row.key,
            {
              size: row.size,
              lastModified: row.lastModified,
            },
          ])
        )
      }

      let remainingCacheSlots: number | null = null
      if (
        activeTransferPayload.operation === "copy" ||
        activeTransferPayload.operation === "sync"
      ) {
        if (Number.isFinite(entitlements.fileLimit)) {
          const currentCachedFileCount = await prisma.fileMetadata.count({
            where: {
              userId: actorUserId,
              isFolder: false,
            },
          })
          remainingCacheSlots = Math.max(0, entitlements.fileLimit - currentCachedFileCount)
        }
      }

      let copiedInBatch = 0
      let movedInBatch = 0
      let deletedInBatch = 0
      let skippedInBatch = 0
      let failedInBatch = 0
      let processedInBatch = 0
      let lastProcessedCursorKey = progress.cursorKey
      let timeBudgetReached = false
      let batchLastError: string | null = null
      const batchStartedAt = Date.now()
      const transferItemConcurrency = getTaskTransferItemConcurrency()

      for (let index = 0; index < mappedBatch.length; index += transferItemConcurrency) {
        if (
          processedInBatch > 0 &&
          Date.now() - batchStartedAt >= getTaskWorkerUserBudgetMs()
        ) {
          timeBudgetReached = true
          break
        }

        const slice = mappedBatch.slice(index, index + transferItemConcurrency)
        const prepared = await Promise.all(
          slice.map(async ({ sourceFile, destinationKey }): Promise<PreparedTransferItem> => {
            if (
              sameCredential &&
              activeTransferPayload.sourceBucket === activeTransferPayload.destinationBucket &&
              sourceFile.key === destinationKey
            ) {
              return {
                sourceFile,
                destinationKey,
                createsNewDestination: false,
                skip: true,
              }
            }

            let destinationExisting = requiresDestinationComparison
              ? destinationByKey.get(destinationKey)
              : undefined
            let destinationExistsRemotely = false

            if (requiresDestinationComparison && !destinationExisting) {
              try {
                const remoteSnapshot = await readRemoteObjectSnapshot({
                  client: destinationClient,
                  bucket: activeTransferPayload.destinationBucket,
                  key: destinationKey,
                })
                if (remoteSnapshot) {
                  destinationExistsRemotely = true
                  if (remoteSnapshot.size !== null && remoteSnapshot.lastModified) {
                    destinationExisting = {
                      size: remoteSnapshot.size,
                      lastModified: remoteSnapshot.lastModified,
                    }
                    destinationByKey.set(destinationKey, destinationExisting)
                  }
                }
              } catch {
                // If destination verification fails, continue with normal transfer flow.
              }
            }

            const createsNewDestination = !destinationExisting && !destinationExistsRemotely
            const shouldSkipForExistingDestination =
              activeTransferPayload.operation === "copy" &&
              (destinationExisting || destinationExistsRemotely)
            const shouldSkipForUpToDateSync =
              activeTransferPayload.operation === "sync" &&
              destinationExisting &&
              isDestinationUpToDateForSync(
                {
                  size: sourceFile.size,
                  lastModified: sourceFile.lastModified,
                },
                destinationExisting
              )

            return {
              sourceFile,
              destinationKey,
              createsNewDestination,
              skip: Boolean(shouldSkipForExistingDestination || shouldSkipForUpToDateSync),
            }
          })
        )

        const actionable: PreparedTransferItem[] = []
        for (const item of prepared) {
          if (item.skip) {
            skippedInBatch++
            processedInBatch++
            continue
          }

          if (item.createsNewDestination && remainingCacheSlots !== null) {
            if (remainingCacheSlots <= 0) {
              skippedInBatch++
              processedInBatch++
              continue
            }
            remainingCacheSlots -= 1
          }

          actionable.push(item)
        }

        let results = await Promise.all(
          actionable.map(async (item): Promise<TransferItemResult> => {
            try {
              await copyObjectAcrossLocations({
                sourceClient,
                destinationClient,
                sameCredential,
                sourceEndpoint: sourceClientInfo.credential.endpoint,
                destinationEndpoint: destinationClientInfo.credential.endpoint,
                sourceRegion: sourceClientInfo.credential.region,
                destinationRegion: destinationClientInfo.credential.region,
                sourceProvider: sourceClientInfo.credential.provider,
                destinationProvider: destinationClientInfo.credential.provider,
                sourceBucket: activeTransferPayload.sourceBucket,
                sourceKey: item.sourceFile.key,
                destinationBucket: activeTransferPayload.destinationBucket,
                destinationKey: item.destinationKey,
                expectedContentLength: item.sourceFile.size,
              })

              return {
                status: "copied",
                sourceId: item.sourceFile.id,
                sourceKey: item.sourceFile.key,
                destinationKey: item.destinationKey,
                extension: item.sourceFile.extension,
                size: item.sourceFile.size,
                lastModified: item.sourceFile.lastModified,
                createsNewDestination: item.createsNewDestination,
                sourceDeleteRequired:
                  activeTransferPayload.operation === "move" ||
                  activeTransferPayload.operation === "migrate",
                errorMessage: null,
              }
            } catch (itemError) {
              const errorCode = getS3ErrorCode(itemError)
              const errorMessage = formatTaskProcessingError(itemError)
              return {
                status: errorCode === "NoSuchKey" ? "missing_source" : "failed",
                sourceId: item.sourceFile.id,
                sourceKey: item.sourceFile.key,
                destinationKey: item.destinationKey,
                extension: item.sourceFile.extension,
                size: item.sourceFile.size,
                lastModified: item.sourceFile.lastModified,
                createsNewDestination: item.createsNewDestination,
                sourceDeleteRequired: false,
                errorMessage,
              }
            }
          })
        )

        const missingSourceIds = results
          .filter((result) => result.status === "missing_source")
          .map((result) => result.sourceId)
        if (missingSourceIds.length > 0) {
          await prisma.fileMetadata.deleteMany({
            where: {
              id: {
                in: missingSourceIds,
              },
              userId: actorUserId,
            },
          })
        }

        const copiedRows = results.filter((result) => result.status === "copied")
        if (copiedRows.length > 0) {
          await upsertFileMetadataBatch(
            copiedRows.map((result) => ({
              userId: actorUserId,
              credentialId: activeTransferPayload.destinationCredentialId,
              bucket: activeTransferPayload.destinationBucket,
              key: result.destinationKey,
              extension: result.extension,
              size: result.size,
              lastModified: result.lastModified,
            }))
          )

          for (const result of copiedRows) {
            destinationByKey.set(result.destinationKey, {
              size: result.size,
              lastModified: result.lastModified,
            })
          }
        }

        if (
          (activeTransferPayload.operation === "move" || activeTransferPayload.operation === "migrate") &&
          copiedRows.length > 0
        ) {
          const deletedSourceKeys = await deleteKeysFromBucket(
            sourceClient,
            activeTransferPayload.sourceBucket,
            copiedRows.map((result) => result.sourceKey)
          )
          const movedSourceIds: string[] = []

          results = results.map((result) => {
            if (result.status !== "copied" || !result.sourceDeleteRequired) {
              return result
            }

            if (deletedSourceKeys.has(result.sourceKey)) {
              movedSourceIds.push(result.sourceId)
              return {
                ...result,
                status: "moved",
              }
            }

            return {
              ...result,
              status: "failed",
              errorMessage:
                result.errorMessage ??
                `Failed to delete source object '${result.sourceKey}' after transfer`,
            }
          })

          if (movedSourceIds.length > 0) {
            await prisma.fileMetadata.deleteMany({
              where: {
                id: {
                  in: movedSourceIds,
                },
                userId: actorUserId,
              },
            })
          }
        }

        for (const result of results) {
          const destinationPersisted =
            result.status === "copied" ||
            result.status === "moved" ||
            (result.status === "failed" && result.sourceDeleteRequired)
          if (
            result.createsNewDestination &&
            remainingCacheSlots !== null &&
            !destinationPersisted
          ) {
            remainingCacheSlots += 1
          }

          if (!batchLastError && result.errorMessage) {
            batchLastError = result.errorMessage
          }

          processedInBatch++
          if (result.status === "copied") {
            copiedInBatch++
            continue
          }
          if (result.status === "moved") {
            movedInBatch++
            deletedInBatch++
            continue
          }
          if (result.status === "skipped" || result.status === "missing_source") {
            skippedInBatch++
            continue
          }
          failedInBatch++
        }

        lastProcessedCursorKey = slice[slice.length - 1]?.sourceFile.key ?? lastProcessedCursorKey
      }

      const total = sourceTotal
      const nextProcessed = progress.processed + processedInBatch
      const nextProgress: ObjectTransferTaskProgress = {
        phase: "transfer",
        total,
        processed: nextProcessed,
        copied: progress.copied + copiedInBatch,
        moved: progress.moved + movedInBatch,
        deleted: progress.deleted + deletedInBatch,
        skipped: progress.skipped + skippedInBatch,
        failed: progress.failed + failedInBatch,
        remaining: Math.max(0, total - nextProcessed),
        cursorKey: lastProcessedCursorKey,
      }

      const transferCheckpoint = await persistClaimedTaskCheckpoint({
        taskId: candidate.id,
        userId: actorUserId,
        claimedRunCount: candidate.runCount + 1,
        normalUpdate: {
          status: "in_progress",
          attempts: 0,
          nextRunAt: new Date(),
          progress: nextProgress as unknown as Prisma.InputJsonObject,
          lastError:
            batchLastError ??
            (nextProgress.failed > 0
              ? candidate.lastError ?? "One or more objects failed during transfer"
              : null),
          completedAt: null,
        },
      })

      return buildProcessedResponse(
        {
          taskId: candidate.id,
          taskType: candidate.type,
          taskStatus: transferCheckpoint.finalStatus,
          runCount: candidate.runCount + 1,
          attempts: transferCheckpoint.appliedMode === "canceled" ? 0 : 0,
          lastError:
            transferCheckpoint.appliedMode === "canceled"
              ? null
              : batchLastError ??
                (nextProgress.failed > 0
                  ? candidate.lastError ?? "One or more objects failed during transfer"
                  : null),
          taskUserId: actorUserId,
        },
        {
          done: transferCheckpoint.appliedMode === "canceled",
          type: "object_transfer",
          processedInBatch,
          copiedInBatch,
          movedInBatch,
          skippedInBatch,
          failedInBatch,
          timeBudgetReached,
        }
      )
    }

    const bulkPlanPayload = resolveTaskPlanPayload(candidate.executionPlan, candidate.payload)
    const payload = parsePayload(bulkPlanPayload)
    if (!payload) {
      const nextAttempts = candidate.attempts + 1
      const invalidBulkCheckpoint = await persistClaimedTaskCheckpoint({
        taskId: candidate.id,
        userId: actorUserId,
        claimedRunCount: candidate.runCount + 1,
        preferTerminal: true,
        normalUpdate: {
          status: "failed",
          lifecycleState: "active",
          attempts: nextAttempts,
          lastError: "Invalid task payload",
          completedAt: new Date(),
          nextRunAt: new Date(),
          executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
            status: "failed",
            message: "Invalid task payload",
          }),
        },
      })
      return buildProcessedResponse(
        {
          taskId: candidate.id,
          taskType: candidate.type,
          taskStatus: invalidBulkCheckpoint.finalStatus,
          runCount: candidate.runCount + 1,
          attempts: invalidBulkCheckpoint.appliedMode === "canceled" ? 0 : nextAttempts,
          lastError: invalidBulkCheckpoint.appliedMode === "canceled" ? null : "Invalid task payload",
          taskUserId: actorUserId,
        },
        {
          done: true,
          error: "Invalid task payload",
        }
      )
    }

    const whereClause = buildFileSearchSqlWhereClause({
      userId: actorUserId,
      query: payload.query,
      credentialIds: payload.selectedCredentialIds,
      scopes: parseScopes(payload.selectedBucketScopes),
      type: payload.selectedType,
    })
    const progress = parseProgress(candidate.progress)
    const bulkDeleteTotal =
      progress.total > 0
        ? progress.total
        : progress.deleted + Number((
          await prisma.$queryRaw<Array<{ total: bigint }>>(Prisma.sql`
            SELECT COUNT(*)::bigint AS "total"
            FROM "FileMetadata" fm
            WHERE ${whereClause}
            ${progress.cursorId ? Prisma.sql`AND fm."id" > ${progress.cursorId}` : Prisma.empty}
          `)
        )[0]?.total ?? BigInt(0))

    const batch = await prisma.$queryRaw<Array<{
      id: string
      key: string
      bucket: string
      credentialId: string
      extension: string
      size: bigint
    }>>(Prisma.sql`
      SELECT
        fm."id",
        fm."key",
        fm."bucket",
        fm."credentialId",
        fm."extension",
        fm."size"
      FROM "FileMetadata" fm
      WHERE ${whereClause}
      ${progress.cursorId ? Prisma.sql`AND fm."id" > ${progress.cursorId}` : Prisma.empty}
      ORDER BY fm."id" ASC
      LIMIT ${getTaskBulkDeleteBatchSize()}
    `)

    if (batch.length === 0) {
      if (claimedTaskSchedule?.enabled) {
        const nextRunAt =
          nextRunAtForTaskSchedule(claimedTaskSchedule, new Date()) ??
          new Date(Date.now() + SYNC_POLL_INTERVAL_SECONDS * 1000)
        const scheduledEmptyCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          normalUpdate: {
            status: "pending",
            attempts: 0,
            completedAt: null,
            nextRunAt,
            progress: {
              total: 0,
              deleted: 0,
              remaining: 0,
              cursorId: null,
            },
            lastError: null,
            executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
              status: "succeeded",
              message: "Scheduled bulk delete cycle completed",
              metadata: {
                deleted: bulkDeleteTotal,
                nextRunAt: nextRunAt.toISOString(),
                schedule: claimedTaskSchedule.cron ?? claimedTaskSchedule.legacyIntervalSeconds,
              },
            }),
          },
        })

        return buildProcessedResponse(
          {
            taskId: candidate.id,
            taskType: candidate.type,
            taskStatus: scheduledEmptyCheckpoint.finalStatus,
            runCount: candidate.runCount + 1,
            attempts: 0,
            lastError: null,
            taskUserId: actorUserId,
          },
          {
            done: scheduledEmptyCheckpoint.appliedMode === "canceled",
            recurring: scheduledEmptyCheckpoint.appliedMode === "normal",
            nextRunAt:
              scheduledEmptyCheckpoint.appliedMode === "normal"
                ? nextRunAt.toISOString()
                : undefined,
          }
        )
      }

      const emptyCompletionCheckpoint = await persistClaimedTaskCheckpoint({
        taskId: candidate.id,
        userId: actorUserId,
        claimedRunCount: candidate.runCount + 1,
        preferTerminal: true,
        normalUpdate: {
          status: "completed",
          lifecycleState: "active",
          attempts: 0,
          completedAt: new Date(),
          nextRunAt: new Date(),
          progress: {
            total: bulkDeleteTotal,
            deleted: bulkDeleteTotal,
            remaining: 0,
            cursorId: null,
          },
          lastError: null,
          executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
            status: "succeeded",
            message: "Bulk delete completed",
            metadata: {
              deleted: bulkDeleteTotal,
            },
          }),
        },
      })

      return buildProcessedResponse(
        {
          taskId: candidate.id,
          taskType: candidate.type,
          taskStatus: emptyCompletionCheckpoint.finalStatus,
          runCount: candidate.runCount + 1,
          attempts: 0,
          lastError: null,
          taskUserId: actorUserId,
        },
        {
          done: true,
        }
      )
    }

    const grouped = new Map<string, { bucket: string; credentialId: string; rows: typeof batch }>()

    for (const row of batch) {
      const groupKey = `${row.credentialId}::${row.bucket}`
      const existing = grouped.get(groupKey)
      if (existing) {
        existing.rows.push(row)
      } else {
        grouped.set(groupKey, {
          bucket: row.bucket,
          credentialId: row.credentialId,
          rows: [row],
        })
      }
    }

    const clients = new Map<string, InstanceType<typeof import("@aws-sdk/client-s3").S3Client>>()
    const deletedIds = new Set<string>()

    for (const group of grouped.values()) {
      let client = clients.get(group.credentialId)
      if (!client) {
        const response = await getS3Client(actorUserId, group.credentialId)
        client = response.client
        clients.set(group.credentialId, client)
      }

      const keys = group.rows.map((row) => row.key)
      const deletedKeys = await deleteKeysFromBucket(client, group.bucket, keys)

      for (const row of group.rows) {
        if (deletedKeys.has(row.key)) {
          deletedIds.add(row.id)
        }
      }
    }

    if (deletedIds.size === 0) {
      throw new Error("No files could be deleted in this batch")
    }

    await prisma.fileMetadata.deleteMany({
      where: {
        id: {
          in: Array.from(deletedIds),
        },
      },
    })

    const deletedRows = batch.filter((row) => deletedIds.has(row.id))
    try {
      await applyUserExtensionStatsDelta(
        actorUserId,
        deletedRows.map((row) => ({
          extension: row.extension,
          size: row.size,
        }))
      )
    } catch {
      await rebuildUserExtensionStats(actorUserId)
    }

    const total = bulkDeleteTotal
    const deleted = Math.min(total, progress.deleted + deletedIds.size)
    const remaining = Math.max(0, total - deleted)
    let lastBatchCursorId = progress.cursorId
    let cursorBlocked = false
    for (const row of batch) {
      if (!cursorBlocked && deletedIds.has(row.id)) {
        lastBatchCursorId = row.id
      } else {
        cursorBlocked = true
      }
    }

    if (remaining === 0 && claimedTaskSchedule?.enabled) {
      const nextRunAt =
        nextRunAtForTaskSchedule(claimedTaskSchedule, new Date()) ??
        new Date(Date.now() + SYNC_POLL_INTERVAL_SECONDS * 1000)
      const scheduledRemainingCheckpoint = await persistClaimedTaskCheckpoint({
        taskId: candidate.id,
        userId: actorUserId,
        claimedRunCount: candidate.runCount + 1,
        normalUpdate: {
          status: "pending",
          attempts: 0,
          completedAt: null,
          nextRunAt,
          progress: {
            total: 0,
            deleted: 0,
            remaining: 0,
            cursorId: null,
          },
          lastError: null,
          executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
            status: "succeeded",
            message: "Scheduled bulk delete cycle completed",
            metadata: {
              total,
              deleted,
              nextRunAt: nextRunAt.toISOString(),
              schedule: claimedTaskSchedule.cron ?? claimedTaskSchedule.legacyIntervalSeconds,
            },
          }),
        },
      })

      return buildProcessedResponse(
        {
          taskId: candidate.id,
          taskType: candidate.type,
          taskStatus: scheduledRemainingCheckpoint.finalStatus,
          runCount: candidate.runCount + 1,
          attempts: 0,
          lastError: null,
          taskUserId: actorUserId,
        },
        {
          deletedInBatch: deletedIds.size,
          done: scheduledRemainingCheckpoint.appliedMode === "canceled",
          recurring: scheduledRemainingCheckpoint.appliedMode === "normal",
          nextRunAt:
            scheduledRemainingCheckpoint.appliedMode === "normal"
              ? nextRunAt.toISOString()
              : undefined,
        }
      )
    }

    const bulkCheckpoint = await persistClaimedTaskCheckpoint({
      taskId: candidate.id,
      userId: actorUserId,
      claimedRunCount: candidate.runCount + 1,
      preferTerminal: remaining === 0,
      normalUpdate: {
        status: remaining === 0 ? "completed" : "in_progress",
        lifecycleState: remaining === 0 ? "active" : undefined,
        attempts: 0,
        completedAt: remaining === 0 ? new Date() : null,
        nextRunAt: new Date(),
        progress: {
          total,
          deleted,
          remaining,
          cursorId: remaining === 0 ? null : lastBatchCursorId,
        },
        lastError: null,
        ...(remaining === 0
          ? {
              executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
                status: "succeeded",
                message: "Bulk delete completed",
                metadata: {
                  total,
                  deleted,
                },
              }),
            }
          : {}),
      },
    })

    return buildProcessedResponse(
      {
        taskId: candidate.id,
        taskType: candidate.type,
        taskStatus: bulkCheckpoint.finalStatus,
        runCount: candidate.runCount + 1,
        attempts: bulkCheckpoint.appliedMode === "canceled" ? 0 : 0,
        lastError: bulkCheckpoint.appliedMode === "canceled" ? null : null,
        taskUserId: actorUserId,
      },
      {
        deletedInBatch: deletedIds.size,
        done: remaining === 0 || bulkCheckpoint.appliedMode === "canceled",
      }
    )
  } catch (error) {
    console.error("Failed to process task:", error)

    const message = formatTaskProcessingError(error)
    const taskAttemptFailed = Boolean(userId && claimedTask)

    try {
      if (userId && claimedTask) {
        const now = new Date()
        const nextAttempts = claimedTask.attempts + 1
        const retryable = nextAttempts < claimedTask.maxAttempts
        const backoffSeconds = Math.min(300, Math.pow(2, nextAttempts))
        const nextScheduledRunAt =
          claimedTaskSchedule?.enabled
            ? nextRunAtForTaskSchedule(claimedTaskSchedule, now) ??
              new Date(now.getTime() + SYNC_POLL_INTERVAL_SECONDS * 1000)
            : null

        if (claimedTask.type === "object_transfer" && transferPayload) {
          await logUserAuditAction({
            userId,
            eventType: "s3_action",
            eventName: "object_transfer_failed",
            path: "/api/tasks/process",
            method: "POST",
            target: `${transferPayload.sourceBucket} -> ${transferPayload.destinationBucket}`,
            metadata: {
              scope: transferPayload.scope,
              operation: transferPayload.operation,
              sourceCredentialId: transferPayload.sourceCredentialId,
              sourceBucket: transferPayload.sourceBucket,
              sourcePrefix: transferPayload.sourcePrefix,
              destinationCredentialId: transferPayload.destinationCredentialId,
              destinationBucket: transferPayload.destinationBucket,
              destinationPrefix: transferPayload.destinationPrefix,
              error: message,
            },
          })
        }

        const failureUpdate: Prisma.BackgroundTaskUpdateManyMutationInput = (() => {
          if (claimedTaskSchedule?.enabled && !retryable) {
            return {
              attempts: 0,
              status: "pending",
              nextRunAt: nextScheduledRunAt ?? new Date(now.getTime() + backoffSeconds * 1000),
              lastError: message,
              completedAt: null,
              executionHistory: addTaskHistoryEntry(taskExecutionHistory, {
                status: "failed",
                message: "Scheduled run failed",
                metadata: {
                  error: message,
                  nextRunAt: nextScheduledRunAt?.toISOString() ?? null,
                },
              }),
            }
          }

          const base: Prisma.BackgroundTaskUpdateManyMutationInput = {
            attempts: nextAttempts,
            status: retryable ? "pending" : "failed",
            nextRunAt: retryable
              ? new Date(now.getTime() + backoffSeconds * 1000)
              : new Date(now.getTime() + 365 * 24 * 60 * 60 * 1000),
            lastError: message,
            completedAt: retryable ? null : now,
          }
          if (!retryable) {
            base.executionHistory = addTaskHistoryEntry(taskExecutionHistory, {
              status: "failed",
              message,
            })
          }
          return base
        })()

        await persistClaimedTaskCheckpoint({
          taskId: claimedTask.id,
          userId,
          claimedRunCount: claimedTask.runCount,
          normalUpdate: failureUpdate,
        })
      }
    } catch (updateError) {
      console.error("Failed to update task failure state:", updateError)
    }

    // A task-level failure was already persisted (retry scheduled or failed state set).
    // Return 200 to avoid noisy client-side 500s while the queue keeps progressing.
    if (taskAttemptFailed && claimedTask) {
      const nextAttempts = claimedTask.attempts + 1
      const retryable = nextAttempts < claimedTask.maxAttempts
      const scheduledRetry = Boolean(claimedTaskSchedule?.enabled && !retryable)
      const currentTask = await prisma.backgroundTask.findFirst({
        where: {
          id: claimedTask.id,
          userId: userId!,
        },
        select: {
          status: true,
          attempts: true,
          lastError: true,
        },
      })
      return buildProcessedResponse(
        {
          taskId: claimedTask.id,
          taskType: claimedTask.type,
          taskStatus: currentTask?.status ?? (retryable || scheduledRetry ? "pending" : "failed"),
          runCount: claimedTask.runCount,
          attempts: Math.max(0, currentTask?.attempts ?? (scheduledRetry ? 0 : nextAttempts)),
          lastError:
            typeof currentTask?.lastError === "string"
              ? currentTask.lastError
              : currentTask?.lastError === null
                ? null
                : message,
          taskUserId: userId!,
        },
        {
          done: currentTask?.status === "canceled",
          error: message,
          retryable,
        }
      )
    }

    return NextResponse.json({ processed: false, error: message }, { status: 500 })
  }
}
