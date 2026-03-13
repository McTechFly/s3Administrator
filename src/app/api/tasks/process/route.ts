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
  UploadPartCommand,
  UploadPartCopyCommand,
} from "@aws-sdk/client-s3"
import { Upload } from "@aws-sdk/lib-storage"
import { PassThrough, Transform, type TransformCallback } from "node:stream"
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
  getTaskTransferMultipartCopyPartConcurrency,
  getTaskTransferProgressMaxEventsPerFile,
  getTaskTransferProgressMinFileSizeMb,
  getTaskTransferProgressSampleDeltaMb,
  getTaskTransferProgressSampleIntervalMs,
  getTaskTransferPreferServerCopySameBackend,
  getTaskTransferRelayPartSizeMb,
  getTaskTransferRelayQueueSize,
  getTaskTransferItemRetryMaxAttempts,
  getTaskTransferItemRetryBaseDelayMs,
  getTaskTransferVerifyChecksum,
  getTaskTransferBandwidthLimitMbps,
  getTaskTransferParallelChunkedDownloadThresholdMb,
  getTaskTransferParallelDownloadStreams,
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
export const maxDuration = 300

const LOCK_SECONDS = 45
const SYNC_POLL_INTERVAL_SECONDS = 60
const MAX_STALE_SCHEDULE_SKIPS_PER_CALL = 32
const PAUSE_HOLD_MS = 365 * 24 * 60 * 60 * 1000
const ONE_MEBIBYTE_BYTES = 1024 * 1024
const ONE_MEBIBYTE_BIGINT = BigInt(ONE_MEBIBYTE_BYTES)
const DEFAULT_MULTIPART_PART_SIZE_BYTES = 64 * ONE_MEBIBYTE_BYTES
const DEFAULT_MULTIPART_PART_SIZE_BIGINT = BigInt(DEFAULT_MULTIPART_PART_SIZE_BYTES)
const MAX_MULTIPART_PARTS = 10_000
const MAX_RELAY_BUFFERED_BYTES = 512 * ONE_MEBIBYTE_BYTES
const SINGLE_REQUEST_COPY_MAX_BYTES = BigInt(5 * 1024 * 1024 * 1024)
const TRANSFER_PROGRESS_MILESTONES = [25, 50, 75, 90, 100] as const
const TRANSIENT_S3_ERROR_CODES = new Set([
  "SlowDown",
  "ServiceUnavailable",
  "InternalError",
  "RequestTimeout",
  "RequestTimeTooSkewed",
  "OperationAborted",
  "500",
  "502",
  "503",
  "504",
])

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
  currentFileKey: string | null
  currentFileSizeBytes: string | null
  currentFileTransferredBytes: string | null
  currentFileStage: TransferProgressStage | null
  transferStrategy: TransferStrategy | null
  fallbackReason: string | null
  bytesProcessedTotal: string | null
  bytesEstimatedTotal: string | null
  throughputBytesPerSec: number | null
  etaSeconds: number | null
  lastProgressAt: string | null
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
  skipReason: TransferSkipReason | null
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

type TransferProgressStage =
  | "queued"
  | "copying"
  | "deleting_source"
  | "finalizing"
  | "completed"
  | "failed"

type TransferProgressSampleReason =
  | "interval"
  | "delta"
  | "milestone"
  | "stage_change"

interface TransferTelemetryHooks {
  start?: (params: {
    sourceKey: string
    destinationKey: string
    strategy: TransferStrategy
    totalBytes: bigint | null
  }) => void | Promise<void>
  progress?: (params: {
    sourceKey: string
    destinationKey: string
    strategy: TransferStrategy
    transferredBytes: bigint
    totalBytes: bigint | null
    stage?: TransferProgressStage
  }) => void | Promise<void>
  stage?: (params: {
    sourceKey: string
    destinationKey: string
    strategy: TransferStrategy | null
    stage: TransferProgressStage
  }) => void | Promise<void>
  fallback?: (params: {
    sourceKey: string
    destinationKey: string
    reason: string
    nextStrategy: TransferStrategy
  }) => void | Promise<void>
  finish?: (params: {
    sourceKey: string
    destinationKey: string
    strategy: TransferStrategy | null
    status: "completed" | "failed"
  }) => void | Promise<void>
}

type TransferSkipReason =
  | "already_exists"
  | "up_to_date"
  | "same_source_and_destination"
  | "cache_limit_reached"

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
      currentFileKey: null,
      currentFileSizeBytes: null,
      currentFileTransferredBytes: null,
      currentFileStage: null,
      transferStrategy: null,
      fallbackReason: null,
      bytesProcessedTotal: null,
      bytesEstimatedTotal: null,
      throughputBytesPerSec: null,
      etaSeconds: null,
      lastProgressAt: null,
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
    currentFileKey?: unknown
    currentFileSizeBytes?: unknown
    currentFileTransferredBytes?: unknown
    currentFileStage?: unknown
    transferStrategy?: unknown
    fallbackReason?: unknown
    bytesProcessedTotal?: unknown
    bytesEstimatedTotal?: unknown
    throughputBytesPerSec?: unknown
    etaSeconds?: unknown
    lastProgressAt?: unknown
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
    currentFileKey:
      typeof progress.currentFileKey === "string" && progress.currentFileKey.length > 0
        ? progress.currentFileKey
        : null,
    currentFileSizeBytes:
      typeof progress.currentFileSizeBytes === "string" && progress.currentFileSizeBytes.length > 0
        ? progress.currentFileSizeBytes
        : null,
    currentFileTransferredBytes:
      typeof progress.currentFileTransferredBytes === "string" &&
      progress.currentFileTransferredBytes.length > 0
        ? progress.currentFileTransferredBytes
        : null,
    currentFileStage:
      progress.currentFileStage === "queued" ||
      progress.currentFileStage === "copying" ||
      progress.currentFileStage === "deleting_source" ||
      progress.currentFileStage === "finalizing" ||
      progress.currentFileStage === "completed" ||
      progress.currentFileStage === "failed"
        ? progress.currentFileStage
        : null,
    transferStrategy:
      progress.transferStrategy === "single_request_server_copy" ||
      progress.transferStrategy === "multipart_server_copy" ||
      progress.transferStrategy === "multipart_relay_upload"
        ? progress.transferStrategy
        : null,
    fallbackReason:
      typeof progress.fallbackReason === "string" && progress.fallbackReason.length > 0
        ? progress.fallbackReason
        : null,
    bytesProcessedTotal:
      typeof progress.bytesProcessedTotal === "string" && progress.bytesProcessedTotal.length > 0
        ? progress.bytesProcessedTotal
        : null,
    bytesEstimatedTotal:
      typeof progress.bytesEstimatedTotal === "string" && progress.bytesEstimatedTotal.length > 0
        ? progress.bytesEstimatedTotal
        : null,
    throughputBytesPerSec:
      typeof progress.throughputBytesPerSec === "number" && Number.isFinite(progress.throughputBytesPerSec)
        ? Math.max(0, progress.throughputBytesPerSec)
        : null,
    etaSeconds:
      typeof progress.etaSeconds === "number" && Number.isFinite(progress.etaSeconds)
        ? Math.max(0, Math.floor(progress.etaSeconds))
        : null,
    lastProgressAt:
      typeof progress.lastProgressAt === "string" && progress.lastProgressAt.length > 0
        ? progress.lastProgressAt
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
  // AWS SDK v3 does NOT correctly encode the x-amz-copy-source header
  // for keys with special characters (spaces, parentheses, non-ASCII).
  // See: https://github.com/aws/aws-sdk-js-v3/issues/6596
  //
  // encodeURI encodes spaces/special chars but preserves '/' separators,
  // unlike encodeURIComponent which also encodes '/' and breaks the format.
  return encodeURI(`${bucket}/${key}`)
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

function parseProgressBigint(value: unknown): bigint | null {
  if (typeof value === "bigint") {
    return value >= BigInt(0) ? value : null
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || value < 0) return null
    return BigInt(Math.floor(value))
  }
  if (typeof value === "string" && value.trim()) {
    try {
      const parsed = BigInt(value)
      return parsed >= BigInt(0) ? parsed : null
    } catch {
      return null
    }
  }
  return null
}

function bigintToNumberLossy(value: bigint): number {
  if (value <= BigInt(Number.MAX_SAFE_INTEGER)) {
    return Number(value)
  }
  return Number.MAX_SAFE_INTEGER
}

function buildTransferFallbackReason(prefix: string, error: unknown): string {
  const code = getS3ErrorCode(error)
  const status = getS3ErrorStatus(error)
  const message = getS3ErrorMessage(error).trim()
  const details = [
    code ? `code=${code}` : null,
    status !== null ? `status=${status}` : null,
    message ? `message=${message.slice(0, 180)}` : null,
  ].filter((value): value is string => Boolean(value))
  if (details.length === 0) return prefix
  return `${prefix} (${details.join(", ")})`
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

function isCopyAuthFallbackError(error: unknown): boolean {
  const status = getS3ErrorStatus(error)
  if (status === 401 || status === 403) return true

  const code = getS3ErrorCode(error)
  if (
    code.includes("AccessDenied") ||
    code.includes("Signature") ||
    code.includes("Authorization") ||
    code.includes("InvalidAccessKeyId") ||
    code.includes("ExpiredToken") ||
    code.includes("InvalidToken") ||
    code.includes("AuthFailure")
  ) {
    return true
  }

  const message = getS3ErrorMessage(error).toLowerCase()
  return (
    message.includes("access denied") ||
    message.includes("forbidden") ||
    message.includes("authorization") ||
    message.includes("signature")
  )
}

function isTransientS3Error(error: unknown): boolean {
  const code = getS3ErrorCode(error)
  if (TRANSIENT_S3_ERROR_CODES.has(code)) return true

  if (error && typeof error === "object") {
    const candidate = error as { $metadata?: { httpStatusCode?: unknown } }
    const status = candidate.$metadata?.httpStatusCode
    if (typeof status === "number" && (status === 429 || status >= 500)) return true
  }

  if (error instanceof Error) {
    const message = error.message.toLowerCase()
    if (
      message.includes("econnreset") ||
      message.includes("econnrefused") ||
      message.includes("etimedout") ||
      message.includes("epipe") ||
      message.includes("socket hang up") ||
      message.includes("network")
    ) {
      return true
    }
  }

  return false
}

function computeRetryDelayMs(attempt: number, baseDelayMs: number): number {
  const delay = baseDelayMs * Math.pow(2, attempt)
  const jitter = delay * 0.2 * Math.random()
  return Math.floor(delay + jitter)
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

class BandwidthThrottleTransform extends Transform {
  private readonly bytesPerSecond: number
  private tokenBucket: number
  private lastRefillTime: number

  constructor(bytesPerSecond: number) {
    super()
    this.bytesPerSecond = bytesPerSecond
    this.tokenBucket = bytesPerSecond
    this.lastRefillTime = Date.now()
  }

  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback): void {
    void this._throttledWrite(chunk, callback)
  }

  private async _throttledWrite(chunk: Buffer, callback: TransformCallback): Promise<void> {
    let offset = 0
    while (offset < chunk.length) {
      this.refillTokens()
      if (this.tokenBucket <= 0) {
        await sleep(50)
        continue
      }
      const bytesToSend = Math.min(chunk.length - offset, Math.floor(this.tokenBucket))
      this.tokenBucket -= bytesToSend
      this.push(chunk.subarray(offset, offset + bytesToSend))
      offset += bytesToSend
    }
    callback()
  }

  private refillTokens(): void {
    const now = Date.now()
    const elapsed = (now - this.lastRefillTime) / 1000
    this.lastRefillTime = now
    this.tokenBucket = Math.min(
      this.bytesPerSecond,
      this.tokenBucket + elapsed * this.bytesPerSecond
    )
  }
}

function createThrottledStream(
  source: NodeJS.ReadableStream,
  bandwidthLimitMbps: number
): NodeJS.ReadableStream {
  if (bandwidthLimitMbps <= 0) return source
  const bytesPerSecond = bandwidthLimitMbps * 1024 * 1024
  const throttle = new BandwidthThrottleTransform(bytesPerSecond)
  return (source as import("stream").Readable).pipe(throttle)
}

async function parallelChunkedDownload(params: {
  sourceClient: S3Client
  sourceBucket: string
  sourceKey: string
  totalBytes: bigint
  streams: number
  onProgress?: (downloadedBytes: bigint) => void
}): Promise<NodeJS.ReadableStream> {
  const { sourceClient, sourceBucket, sourceKey, totalBytes, streams } = params
  const chunkSize = totalBytes / BigInt(streams)
  const passThrough = new PassThrough()

  const ranges: Array<{ start: bigint; end: bigint }> = []
  for (let i = 0; i < streams; i++) {
    const start = chunkSize * BigInt(i)
    const end = i === streams - 1 ? totalBytes - BigInt(1) : start + chunkSize - BigInt(1)
    ranges.push({ start, end })
  }

  // Download chunks sequentially and pipe in order to preserve byte order,
  // but fetch the next chunk header concurrently with the current stream.
  void (async () => {
    let totalDownloaded = BigInt(0)
    try {
      for (const range of ranges) {
        const response = await sourceClient.send(
          new GetObjectCommand({
            Bucket: sourceBucket,
            Key: sourceKey,
            Range: `bytes=${range.start.toString()}-${range.end.toString()}`,
          })
        )
        if (!response.Body) {
          throw new Error(`Missing body for range ${range.start}-${range.end}`)
        }
        const readable = response.Body as import("stream").Readable
        await new Promise<void>((resolve, reject) => {
          readable.on("data", (chunk: Buffer) => {
            totalDownloaded += BigInt(chunk.length)
            params.onProgress?.(totalDownloaded)
            if (!passThrough.write(chunk)) {
              readable.pause()
              passThrough.once("drain", () => readable.resume())
            }
          })
          readable.on("end", resolve)
          readable.on("error", reject)
        })
      }
      passThrough.end()
    } catch (error) {
      passThrough.destroy(error instanceof Error ? error : new Error(String(error)))
    }
  })()

  return passThrough
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
  preferServerCopySameBackend: boolean
  sourceSizeBytes: bigint | null
  sourceEndpoint: string
  destinationEndpoint: string
  sourceRegion: string
  destinationRegion: string
  sourceProvider: string
  destinationProvider: string
}): TransferStrategy {
  const sameBackend = isSameS3Backend({
    sourceEndpoint: params.sourceEndpoint,
    destinationEndpoint: params.destinationEndpoint,
    sourceRegion: params.sourceRegion,
    destinationRegion: params.destinationRegion,
    sourceProvider: params.sourceProvider,
    destinationProvider: params.destinationProvider,
  })
  const canUseServerCopy =
    sameBackend && (params.sameCredential || params.preferServerCopySameBackend)

  if (
    canUseServerCopy
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

function formatTransferSkipReason(reason: TransferSkipReason): string {
  if (reason === "already_exists") return "already exists at destination"
  if (reason === "up_to_date") return "destination is up to date"
  if (reason === "same_source_and_destination") return "source and destination are identical"
  return "cache limit reached"
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
  telemetry?: TransferTelemetryHooks
}) {
  const relayPartSizeBytes = getTaskTransferRelayPartSizeMb() * ONE_MEBIBYTE_BYTES
  const relayQueueSizeConfigured = getTaskTransferRelayQueueSize()
  const relayQueueMemoryCap = Math.max(
    1,
    Math.floor(MAX_RELAY_BUFFERED_BYTES / relayPartSizeBytes)
  )
  const relayQueueSize = Math.max(
    1,
    Math.min(relayQueueSizeConfigured, relayQueueMemoryCap)
  )

  const sourceSizeBytes = (() => {
    const contentLength = toValidContentLength(params.expectedContentLength)
    return contentLength === null ? null : BigInt(contentLength)
  })()

  const initialStrategy = selectTransferStrategy({
    sameCredential: params.sameCredential,
    preferServerCopySameBackend: getTaskTransferPreferServerCopySameBackend(),
    sourceSizeBytes,
    sourceEndpoint: params.sourceEndpoint,
    destinationEndpoint: params.destinationEndpoint,
    sourceRegion: params.sourceRegion,
    destinationRegion: params.destinationRegion,
    sourceProvider: params.sourceProvider,
    destinationProvider: params.destinationProvider,
  })

  async function emitStart(strategy: TransferStrategy, totalBytes: bigint | null) {
    if (!params.telemetry?.start) return
    await params.telemetry.start({
      sourceKey: params.sourceKey,
      destinationKey: params.destinationKey,
      strategy,
      totalBytes,
    })
  }

  async function emitProgress(
    strategy: TransferStrategy,
    transferredBytes: bigint,
    totalBytes: bigint | null,
    stage?: TransferProgressStage
  ) {
    if (!params.telemetry?.progress) return
    await params.telemetry.progress({
      sourceKey: params.sourceKey,
      destinationKey: params.destinationKey,
      strategy,
      transferredBytes,
      totalBytes,
      stage,
    })
  }

  async function emitStage(strategy: TransferStrategy | null, stage: TransferProgressStage) {
    if (!params.telemetry?.stage) return
    await params.telemetry.stage({
      sourceKey: params.sourceKey,
      destinationKey: params.destinationKey,
      strategy,
      stage,
    })
  }

  async function emitFallback(reason: string, nextStrategy: TransferStrategy) {
    if (!params.telemetry?.fallback) return
    await params.telemetry.fallback({
      sourceKey: params.sourceKey,
      destinationKey: params.destinationKey,
      reason,
      nextStrategy,
    })
  }

  async function emitFinish(strategy: TransferStrategy | null, status: "completed" | "failed") {
    if (!params.telemetry?.finish) return
    await params.telemetry.finish({
      sourceKey: params.sourceKey,
      destinationKey: params.destinationKey,
      strategy,
      status,
    })
  }

  async function multipartRelayObjectAcrossLocations(
    strategy: TransferStrategy = "multipart_relay_upload"
  ): Promise<void> {
    await emitStage(strategy, "copying")

    const bandwidthLimitMbps = getTaskTransferBandwidthLimitMbps()
    const parallelDownloadThresholdBytes =
      BigInt(getTaskTransferParallelChunkedDownloadThresholdMb()) * ONE_MEBIBYTE_BIGINT
    const parallelDownloadStreams = getTaskTransferParallelDownloadStreams()

    // Determine source size from head or expected content length
    const headDetails = await readSourceObjectHeadDetails({
      sourceClient: params.sourceClient,
      sourceBucket: params.sourceBucket,
      sourceKey: params.sourceKey,
      expectedContentLength: params.expectedContentLength,
    })
    const totalBytes = headDetails.sizeBytes ?? sourceSizeBytes
    const contentLength = totalBytes !== null ? Number(totalBytes) : null

    // Choose download method: parallel chunked for large files, streaming for others
    const useParallelDownload =
      parallelDownloadThresholdBytes > BigInt(0) &&
      totalBytes !== null &&
      totalBytes >= parallelDownloadThresholdBytes &&
      parallelDownloadStreams > 1

    let sourceBody: NodeJS.ReadableStream

    if (useParallelDownload && totalBytes !== null) {
      sourceBody = await parallelChunkedDownload({
        sourceClient: params.sourceClient,
        sourceBucket: params.sourceBucket,
        sourceKey: params.sourceKey,
        totalBytes,
        streams: parallelDownloadStreams,
      })
    } else {
      const sourceObject = await params.sourceClient.send(
        new GetObjectCommand({
          Bucket: params.sourceBucket,
          Key: params.sourceKey,
        })
      )
      if (!sourceObject.Body) {
        throw new Error(`Missing source object body for key '${params.sourceKey}'`)
      }
      sourceBody = sourceObject.Body as unknown as NodeJS.ReadableStream
    }

    // Apply bandwidth throttling if configured
    const throttledBody = createThrottledStream(sourceBody, bandwidthLimitMbps)

    // Start a resumable multipart upload manually so we can track and resume
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
      throw new Error(`Failed to start multipart relay upload for key '${params.destinationKey}'`)
    }

    try {
      const partSize = relayPartSizeBytes
      const completedParts: Array<{ ETag: string; PartNumber: number }> = []
      let partNumber = 1
      let uploadedBytes = BigInt(0)

      // Read the source stream in partSize chunks and upload each part
      const readable = throttledBody as import("stream").Readable
      let currentBuffer = Buffer.alloc(0)

      const uploadPart = async (body: Buffer, partNum: number): Promise<void> => {
        const response = await params.destinationClient.send(
          new UploadPartCommand({
            Bucket: params.destinationBucket,
            Key: params.destinationKey,
            UploadId: uploadId,
            PartNumber: partNum,
            Body: body,
            ContentLength: body.length,
          })
        )
        const etag = response.ETag
        if (!etag) {
          throw new Error(`Relay upload part ${partNum} did not return an ETag`)
        }
        completedParts.push({ ETag: etag, PartNumber: partNum })
        uploadedBytes += BigInt(body.length)
        void emitProgress(strategy, uploadedBytes, totalBytes, "copying")
      }

      // Collect concurrent uploads up to relayQueueSize
      const uploadQueue: Promise<void>[] = []

      for await (const chunk of readable) {
        currentBuffer = Buffer.concat([currentBuffer, chunk as Buffer])

        while (currentBuffer.length >= partSize) {
          const partBody = currentBuffer.subarray(0, partSize)
          currentBuffer = currentBuffer.subarray(partSize)
          const currentPartNumber = partNumber++

          const uploadPromise = uploadPart(Buffer.from(partBody), currentPartNumber)
          uploadQueue.push(uploadPromise)

          if (uploadQueue.length >= relayQueueSize) {
            await Promise.all(uploadQueue)
            uploadQueue.length = 0
          }
        }
      }

      // Upload remaining data as the final part
      if (currentBuffer.length > 0) {
        const currentPartNumber = partNumber++
        const uploadPromise = uploadPart(Buffer.from(currentBuffer), currentPartNumber)
        uploadQueue.push(uploadPromise)
      }

      // Wait for remaining uploads
      if (uploadQueue.length > 0) {
        await Promise.all(uploadQueue)
      }

      if (completedParts.length === 0) {
        // Edge case: empty file — upload a single empty part
        await uploadPart(Buffer.alloc(0), 1)
      }

      completedParts.sort((a, b) => a.PartNumber - b.PartNumber)

      await emitStage(strategy, "finalizing")
      await params.destinationClient.send(
        new CompleteMultipartUploadCommand({
          Bucket: params.destinationBucket,
          Key: params.destinationKey,
          UploadId: uploadId,
          MultipartUpload: { Parts: completedParts },
        })
      )

      if (totalBytes !== null) {
        await emitProgress(strategy, totalBytes, totalBytes, "finalizing")
      }
    } catch (error) {
      // Abort the multipart upload on failure to avoid orphaned parts
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

  async function multipartCopyObjectWithinBackend(
    strategy: TransferStrategy = "multipart_server_copy"
  ): Promise<boolean> {
    await emitStage(strategy, "copying")

    const headDetails = await readSourceObjectHeadDetails({
      sourceClient: params.sourceClient,
      sourceBucket: params.sourceBucket,
      sourceKey: params.sourceKey,
      expectedContentLength: params.expectedContentLength,
    })

    if (!headDetails.sizeBytes || headDetails.sizeBytes <= BigInt(0)) {
      return false
    }
    const sourceSizeForCopy = headDetails.sizeBytes

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

    const partSizeBytes = computeMultipartPartSizeBytes(sourceSizeForCopy)
    const copySourceHeader = buildCopySource(params.sourceBucket, params.sourceKey)
    const partRanges: Array<{ partNumber: number; rangeStart: bigint; rangeEnd: bigint }> = []
    let offset = BigInt(0)
    let partNumber = 1
    while (offset < sourceSizeForCopy) {
      const nextOffset = offset + partSizeBytes < sourceSizeForCopy
        ? offset + partSizeBytes
        : sourceSizeForCopy
      const rangeEnd = nextOffset - BigInt(1)
      partRanges.push({
        partNumber,
        rangeStart: offset,
        rangeEnd,
      })
      offset = nextOffset
      partNumber += 1
    }

    if (partRanges.length === 0) {
      return false
    }

    try {
      const concurrency = Math.max(
        1,
        Math.min(getTaskTransferMultipartCopyPartConcurrency(), partRanges.length)
      )
      let copiedBytes = BigInt(0)
      const partResults: Array<{ ETag: string; PartNumber: number } | null> = new Array(
        partRanges.length
      ).fill(null)
      let nextPartIndex = 0
      let firstError: unknown = null

      const workers = Array.from({ length: concurrency }, async () => {
        while (true) {
          if (firstError) return

          const currentIndex = nextPartIndex
          nextPartIndex += 1
          if (currentIndex >= partRanges.length) {
            return
          }

          const currentPart = partRanges[currentIndex]
          try {
            const partResponse = await params.destinationClient.send(
              new UploadPartCopyCommand({
                Bucket: params.destinationBucket,
                Key: params.destinationKey,
                UploadId: uploadId,
                PartNumber: currentPart.partNumber,
                CopySource: copySourceHeader,
                CopySourceRange:
                  `bytes=${currentPart.rangeStart.toString()}-${currentPart.rangeEnd.toString()}`,
              })
            )

            const etag = partResponse.CopyPartResult?.ETag
            if (!etag) {
              throw new Error(
                `Multipart copy part ${currentPart.partNumber} did not return an ETag for key '${params.destinationKey}'`
              )
            }

            partResults[currentIndex] = {
              ETag: etag,
              PartNumber: currentPart.partNumber,
            }

            copiedBytes += currentPart.rangeEnd - currentPart.rangeStart + BigInt(1)
            await emitProgress(
              strategy,
              copiedBytes > sourceSizeForCopy ? sourceSizeForCopy : copiedBytes,
              sourceSizeForCopy,
              "copying"
            )
          } catch (error) {
            if (!firstError) {
              firstError = error
            }
            return
          }
        }
      })

      await Promise.all(workers)
      if (firstError) {
        throw firstError
      }

      const completedParts = partResults
        .filter((value): value is { ETag: string; PartNumber: number } => Boolean(value))
        .sort((a, b) => a.PartNumber - b.PartNumber)
      if (completedParts.length !== partRanges.length) {
        throw new Error(
          `Multipart copy did not produce all parts for key '${params.destinationKey}'`
        )
      }

      await emitStage(strategy, "finalizing")
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

  async function verifyPostCopyIntegrity(): Promise<void> {
    if (!getTaskTransferVerifyChecksum()) return
    if (sourceSizeBytes === null) return

    const destSnapshot = await readRemoteObjectSnapshot({
      client: params.destinationClient,
      bucket: params.destinationBucket,
      key: params.destinationKey,
    })

    if (!destSnapshot) {
      throw new Error(
        `Post-copy verification failed: destination object '${params.destinationKey}' not found`
      )
    }

    if (destSnapshot.size !== null && destSnapshot.size !== sourceSizeBytes) {
      throw new Error(
        `Post-copy verification failed: size mismatch for '${params.destinationKey}' ` +
        `(source=${sourceSizeBytes.toString()}, destination=${destSnapshot.size.toString()})`
      )
    }
  }

  const complete = async (strategy: TransferStrategy) => {
    await verifyPostCopyIntegrity()
    await emitStage(strategy, "completed")
    await emitFinish(strategy, "completed")
  }

  try {
    await emitStart(initialStrategy, sourceSizeBytes)
    await emitStage(initialStrategy, "queued")

    if (initialStrategy === "multipart_server_copy") {
      try {
        const copied = await multipartCopyObjectWithinBackend("multipart_server_copy")
        if (copied) {
          await complete("multipart_server_copy")
          return
        }

        await emitFallback(
          "multipart_server_copy produced no copyable byte ranges",
          "multipart_relay_upload"
        )
        await multipartRelayObjectAcrossLocations("multipart_relay_upload")
        await complete("multipart_relay_upload")
        return
      } catch (error) {
        if (
          isCopyCompatibilityFallbackError(error) ||
          isCopyAuthFallbackError(error) ||
          isS3MissingObjectError(error)
        ) {
          // Some S3-compatible providers can return NoSuchKey for CopySource
          // parsing issues even when the source exists. Relay upload avoids
          // CopySource and still preserves true missing-source behavior.
          await emitFallback(
            buildTransferFallbackReason(
              "multipart_server_copy failed; retrying via multipart_relay_upload",
              error
            ),
            "multipart_relay_upload"
          )
          await multipartRelayObjectAcrossLocations("multipart_relay_upload")
          await complete("multipart_relay_upload")
          return
        }
        throw error
      }
    }

    if (initialStrategy === "single_request_server_copy") {
      try {
        await emitStage("single_request_server_copy", "copying")
        await params.destinationClient.send(
          new CopyObjectCommand({
            Bucket: params.destinationBucket,
            CopySource: buildCopySource(params.sourceBucket, params.sourceKey),
            Key: params.destinationKey,
          })
        )
        if (sourceSizeBytes !== null) {
          await emitProgress(
            "single_request_server_copy",
            sourceSizeBytes,
            sourceSizeBytes,
            "finalizing"
          )
        }
        await complete("single_request_server_copy")
        return
      } catch (error) {
        if (isEntityTooLargeError(error)) {
          await emitFallback(
            buildTransferFallbackReason(
              "single_request_server_copy exceeded size limit; retrying multipart",
              error
            ),
            "multipart_server_copy"
          )
          try {
            const copied = await multipartCopyObjectWithinBackend("multipart_server_copy")
            if (copied) {
              await complete("multipart_server_copy")
              return
            }
          } catch (multipartError) {
            if (
              isCopyCompatibilityFallbackError(multipartError) ||
              isCopyAuthFallbackError(multipartError) ||
              isS3MissingObjectError(multipartError)
            ) {
              await emitFallback(
                buildTransferFallbackReason(
                  "multipart_server_copy failed after single_request_server_copy fallback; retrying relay",
                  multipartError
                ),
                "multipart_relay_upload"
              )
              await multipartRelayObjectAcrossLocations("multipart_relay_upload")
              await complete("multipart_relay_upload")
              return
            }
            throw multipartError
          }

          await emitFallback(
            "multipart_server_copy produced no copyable byte ranges after single-request fallback",
            "multipart_relay_upload"
          )
          await multipartRelayObjectAcrossLocations("multipart_relay_upload")
          await complete("multipart_relay_upload")
          return
        }

        if (isCopyCompatibilityFallbackError(error) || isCopyAuthFallbackError(error)) {
          await emitFallback(
            buildTransferFallbackReason(
              "single_request_server_copy rejected by backend; retrying relay",
              error
            ),
            "multipart_relay_upload"
          )
          await multipartRelayObjectAcrossLocations("multipart_relay_upload")
          await complete("multipart_relay_upload")
          return
        }

        if (isS3MissingObjectError(error)) {
          await emitFallback(
            buildTransferFallbackReason(
              "single_request_server_copy returned missing source; retrying relay verification",
              error
            ),
            "multipart_relay_upload"
          )
          // Same fallback rationale as multipart_server_copy above.
          await multipartRelayObjectAcrossLocations("multipart_relay_upload")
          await complete("multipart_relay_upload")
          return
        }

        throw error
      }
    }

    await multipartRelayObjectAcrossLocations("multipart_relay_upload")
    await complete("multipart_relay_upload")
  } catch (error) {
    await emitStage(null, "failed")
    await emitFinish(null, "failed")
    throw error
  }
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

    // Recover tasks stuck in cancel-transition: lifecycleState was set to
    // "canceled" while the task was in_progress, but the worker never
    // finalised the status (crash, timeout, etc.).  Once the lock
    // (nextRunAt) has expired we know no worker is actively processing
    // the task, so we can safely move it to its terminal state.
    await prisma.backgroundTask.updateMany({
      where: {
        userId: actorUserId,
        status: "in_progress",
        lifecycleState: "canceled",
        nextRunAt: { lte: now },
      },
      data: {
        status: "canceled",
        attempts: 0,
        lastError: null,
        completedAt: now,
        nextRunAt: now,
        isRecurring: false,
        scheduleCron: null,
        scheduleIntervalSeconds: null,
      },
    })

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
      const sourceScopeBaseWhere = {
        userId: actorUserId,
        credentialId: activeTransferPayload.sourceCredentialId,
        bucket: activeTransferPayload.sourceBucket,
        isFolder: false,
        ...(activeTransferPayload.scope === "folder" && activeTransferPayload.sourcePrefix
          ? { key: { startsWith: activeTransferPayload.sourcePrefix } }
          : {}),
      }
      const sourceKeyFilter: { startsWith?: string; gt?: string } = {}
      if (activeTransferPayload.scope === "folder" && activeTransferPayload.sourcePrefix) {
        sourceKeyFilter.startsWith = activeTransferPayload.sourcePrefix
      }
      if (progress.cursorKey) {
        sourceKeyFilter.gt = progress.cursorKey
      }

      const persistedEstimatedBytes = parseProgressBigint(progress.bytesEstimatedTotal)
      const sourceEstimatedBytesAggregate =
        persistedEstimatedBytes === null
          ? await prisma.fileMetadata.aggregate({
            where: sourceScopeBaseWhere,
            _sum: {
              size: true,
            },
          })
          : null
      const bytesEstimatedTotal =
        persistedEstimatedBytes ?? sourceEstimatedBytesAggregate?._sum.size ?? null
      let bytesProcessedCompleted = parseProgressBigint(progress.bytesProcessedTotal) ?? BigInt(0)
      if (bytesEstimatedTotal !== null && bytesProcessedCompleted > bytesEstimatedTotal) {
        bytesProcessedCompleted = bytesEstimatedTotal
      }

      const [sourceClientInfo, destinationClientInfo] = await Promise.all([
        getS3Client(actorUserId, activeTransferPayload.sourceCredentialId, {
          trafficClass: "background",
        }),
        getS3Client(actorUserId, activeTransferPayload.destinationCredentialId, {
          trafficClass: "background",
        }),
      ])
      const sourceClient = sourceClientInfo.client
      const destinationClient = destinationClientInfo.client

      const sameCredential =
        activeTransferPayload.sourceCredentialId === activeTransferPayload.destinationCredentialId
      const requiresDestinationComparison =
        activeTransferPayload.operation === "copy" || activeTransferPayload.operation === "sync"

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

      const batchSize = getTaskTransferBatchSize()
      let sourceBatch = await prisma.fileMetadata.findMany({
        where: {
          userId: actorUserId,
          credentialId: activeTransferPayload.sourceCredentialId,
          bucket: activeTransferPayload.sourceBucket,
          isFolder: false,
          ...(Object.keys(sourceKeyFilter).length > 0 ? { key: sourceKeyFilter } : {}),
        },
        orderBy: { key: "asc" },
        take: batchSize,
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
                currentFileKey: null,
                currentFileSizeBytes: null,
                currentFileTransferredBytes: null,
                currentFileStage: null,
                transferStrategy: null,
                fallbackReason: null,
                bytesProcessedTotal: null,
                bytesEstimatedTotal: null,
                throughputBytesPerSec: null,
                etaSeconds: null,
                lastProgressAt: null,
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
              cursorKey: null,
              currentFileKey: null,
              currentFileSizeBytes: null,
              currentFileTransferredBytes: null,
              currentFileStage: null,
              transferStrategy: null,
              fallbackReason: null,
              bytesProcessedTotal: bytesProcessedCompleted.toString(),
              bytesEstimatedTotal: bytesEstimatedTotal?.toString() ?? null,
              throughputBytesPerSec: null,
              etaSeconds: null,
              lastProgressAt: null,
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

      let mappedBatch = sourceBatch.map((sourceFile) => ({
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

      // Bulk-skip: split the batch into files skippable from cached metadata
      // vs files that need actual transfer processing. Skippable files are
      // counted immediately so we never iterate through them in the loop.
      let bulkSkipReasons: Record<string, number> = {}
      let actionableBatch: typeof mappedBatch = []
      for (const item of mappedBatch) {
        let skipReason: TransferSkipReason | null = null

        if (
          sameCredential &&
          activeTransferPayload.sourceBucket === activeTransferPayload.destinationBucket &&
          item.sourceFile.key === item.destinationKey
        ) {
          skipReason = "same_source_and_destination"
        } else if (requiresDestinationComparison) {
          const dest = destinationByKey.get(item.destinationKey)
          if (dest) {
            if (activeTransferPayload.operation === "copy") {
              skipReason = "already_exists"
            } else if (
              activeTransferPayload.operation === "sync" &&
              isDestinationUpToDateForSync(
                { size: item.sourceFile.size, lastModified: item.sourceFile.lastModified },
                dest
              )
            ) {
              skipReason = "up_to_date"
            }
          }
        }

        if (skipReason) {
          bulkSkipReasons[skipReason] = (bulkSkipReasons[skipReason] ?? 0) + 1
        } else {
          actionableBatch.push(item)
        }
      }

      let bulkSkippedCount = Object.values(bulkSkipReasons).reduce((a, b) => a + b, 0)

      // Emit a single summary event for all bulk-skipped files
      if (bulkSkippedCount > 0) {
        const reasonParts = Object.entries(bulkSkipReasons)
          .map(([reason, count]) => `${count} ${formatTransferSkipReason(reason as TransferSkipReason)}`)
          .join(", ")
        try {
          await prisma.backgroundTaskEvent.create({
            data: {
              taskId: candidate.id,
              userId: actorUserId,
              eventType: "batch_skipped",
              message: `Skipped ${bulkSkippedCount} files (${reasonParts})`,
              metadata: {
                count: bulkSkippedCount,
                reasons: bulkSkipReasons,
              },
            },
          })
        } catch {
          // Non-critical
        }
      }

      // Fast-forward: when the entire batch was bulk-skipped, load subsequent
      // batches immediately instead of returning to the worker poll loop.
      // This avoids wasting one HTTP round-trip per skip-only batch.
      while (
        actionableBatch.length === 0 &&
        sourceBatch.length >= batchSize
      ) {
        // Advance progress past the skipped batch
        const lastSkippedKey = mappedBatch[mappedBatch.length - 1]!.sourceFile.key
        progress.processed += bulkSkippedCount
        progress.skipped += bulkSkippedCount
        progress.cursorKey = lastSkippedKey
        progress.remaining = Math.max(0, sourceTotal - progress.processed)
        sourceKeyFilter.gt = lastSkippedKey

        // Persist checkpoint so the UI reflects progress and cancel/pause is honoured
        const ffCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          normalUpdate: {
            status: "in_progress",
            attempts: 0,
            nextRunAt: new Date(Date.now() + LOCK_SECONDS * 1000),
            progress: progress as unknown as Prisma.InputJsonObject,
            lastError: null,
            completedAt: null,
          },
        })
        if (ffCheckpoint.appliedMode !== "normal") {
          return buildProcessedResponse(
            {
              taskId: candidate.id,
              taskType: candidate.type,
              taskStatus: ffCheckpoint.finalStatus,
              runCount: candidate.runCount + 1,
              attempts: 0,
              lastError: null,
              taskUserId: actorUserId,
            },
            {
              done: true,
              type: "object_transfer",
              processedInBatch: bulkSkippedCount,
              copiedInBatch: 0,
              movedInBatch: 0,
              skippedInBatch: bulkSkippedCount,
              failedInBatch: 0,
              timeBudgetReached: false,
            }
          )
        }

        // Load next batch
        sourceBatch = await prisma.fileMetadata.findMany({
          where: {
            userId: actorUserId,
            credentialId: activeTransferPayload.sourceCredentialId,
            bucket: activeTransferPayload.sourceBucket,
            isFolder: false,
            ...(Object.keys(sourceKeyFilter).length > 0 ? { key: sourceKeyFilter } : {}),
          },
          orderBy: { key: "asc" },
          take: batchSize,
          select: {
            id: true,
            key: true,
            extension: true,
            size: true,
            lastModified: true,
          },
        })

        if (sourceBatch.length === 0) break

        // Rebuild mapped batch and destination metadata
        mappedBatch = sourceBatch.map((sourceFile) => ({
          sourceFile,
          destinationKey: mapTransferDestinationKey(activeTransferPayload, sourceFile.key),
        }))

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
              { size: row.size, lastModified: row.lastModified },
            ])
          )
        }

        // Re-run bulk-skip on the new batch
        bulkSkipReasons = {}
        actionableBatch = []
        for (const item of mappedBatch) {
          let skipReason: TransferSkipReason | null = null

          if (
            sameCredential &&
            activeTransferPayload.sourceBucket === activeTransferPayload.destinationBucket &&
            item.sourceFile.key === item.destinationKey
          ) {
            skipReason = "same_source_and_destination"
          } else if (requiresDestinationComparison) {
            const dest = destinationByKey.get(item.destinationKey)
            if (dest) {
              if (activeTransferPayload.operation === "copy") {
                skipReason = "already_exists"
              } else if (
                activeTransferPayload.operation === "sync" &&
                isDestinationUpToDateForSync(
                  { size: item.sourceFile.size, lastModified: item.sourceFile.lastModified },
                  dest
                )
              ) {
                skipReason = "up_to_date"
              }
            }
          }

          if (skipReason) {
            bulkSkipReasons[skipReason] = (bulkSkipReasons[skipReason] ?? 0) + 1
          } else {
            actionableBatch.push(item)
          }
        }

        bulkSkippedCount = Object.values(bulkSkipReasons).reduce((a, b) => a + b, 0)

        if (bulkSkippedCount > 0) {
          const reasonParts = Object.entries(bulkSkipReasons)
            .map(([reason, count]) => `${count} ${formatTransferSkipReason(reason as TransferSkipReason)}`)
            .join(", ")
          try {
            await prisma.backgroundTaskEvent.create({
              data: {
                taskId: candidate.id,
                userId: actorUserId,
                eventType: "batch_skipped",
                message: `Skipped ${bulkSkippedCount} files (${reasonParts})`,
                metadata: {
                  count: bulkSkippedCount,
                  reasons: bulkSkipReasons,
                },
              },
            })
          } catch {
            // Non-critical
          }
        }
      }

      // If fast-forward exhausted all source files, persist the final skip
      // progress and return. The next worker poll will see an empty sourceBatch
      // and run the original completion handler (sync cleanup, audit, etc.).
      if (sourceBatch.length === 0 || (sourceBatch.length < batchSize && actionableBatch.length === 0)) {
        if (bulkSkippedCount > 0 && actionableBatch.length === 0) {
          progress.processed += bulkSkippedCount
          progress.skipped += bulkSkippedCount
          progress.cursorKey = mappedBatch[mappedBatch.length - 1]?.sourceFile.key ?? progress.cursorKey
          progress.remaining = Math.max(0, sourceTotal - progress.processed)
        }

        const ffFinalCheckpoint = await persistClaimedTaskCheckpoint({
          taskId: candidate.id,
          userId: actorUserId,
          claimedRunCount: candidate.runCount + 1,
          normalUpdate: {
            status: "in_progress",
            attempts: 0,
            nextRunAt: new Date(),
            progress: progress as unknown as Prisma.InputJsonObject,
            lastError: null,
            completedAt: null,
          },
        })

        return buildProcessedResponse(
          {
            taskId: candidate.id,
            taskType: candidate.type,
            taskStatus: ffFinalCheckpoint.finalStatus,
            runCount: candidate.runCount + 1,
            attempts: 0,
            lastError: null,
            taskUserId: actorUserId,
          },
          {
            done: ffFinalCheckpoint.appliedMode === "canceled",
            type: "object_transfer",
            processedInBatch: progress.processed,
            copiedInBatch: 0,
            movedInBatch: 0,
            skippedInBatch: progress.skipped,
            failedInBatch: 0,
            timeBudgetReached: false,
          }
        )
      }

      let copiedInBatch = 0
      let movedInBatch = 0
      let deletedInBatch = 0
      let skippedInBatch = bulkSkippedCount
      let failedInBatch = 0
      let processedInBatch = bulkSkippedCount
      let lastProcessedCursorKey = progress.cursorKey
      let timeBudgetReached = false
      let batchLastError: string | null = null
      const staleDestinationKeys: string[] = []
      const batchStartedAt = Date.now()
      const transferItemConcurrency = getTaskTransferItemConcurrency()
      const claimedTaskId = candidate.id
      const claimedRunCount = candidate.runCount + 1
      const transferProgressMinFileSizeBytes =
        BigInt(getTaskTransferProgressMinFileSizeMb()) * ONE_MEBIBYTE_BIGINT
      const transferProgressSampleIntervalMs = getTaskTransferProgressSampleIntervalMs()
      const transferProgressSampleDeltaBytes =
        BigInt(getTaskTransferProgressSampleDeltaMb()) * ONE_MEBIBYTE_BIGINT
      const transferProgressMaxEventsPerFile = getTaskTransferProgressMaxEventsPerFile()

      interface LiveTransferTelemetryState {
        sourceKey: string
        destinationKey: string
        strategy: TransferStrategy | null
        stage: TransferProgressStage | null
        fallbackReason: string | null
        totalBytes: bigint | null
        transferredBytes: bigint
        throughputBytesPerSec: number | null
        etaSeconds: number | null
        lastProgressAtMs: number | null
        lastSpeedSampleAtMs: number | null
        lastSpeedSampleBytes: bigint
        lastSampleAtMs: number | null
        lastSampleBytes: bigint
        lastSampleStage: TransferProgressStage | null
        emittedMilestones: Set<number>
        sampledEvents: number
      }

      const transferTelemetryByFile = new Map<string, LiveTransferTelemetryState>()
      let activeTransferTelemetryKey: string | null = null
      let telemetryWriteQueue = Promise.resolve()
      let lastTelemetryProgressPersistAt = 0

      function getTelemetryStateKey(sourceKey: string, destinationKey: string): string {
        return `${sourceKey}::${destinationKey}`
      }

      function getOrCreateTelemetryState(
        sourceKey: string,
        destinationKey: string
      ): LiveTransferTelemetryState {
        const key = getTelemetryStateKey(sourceKey, destinationKey)
        const existing = transferTelemetryByFile.get(key)
        if (existing) return existing

        const created: LiveTransferTelemetryState = {
          sourceKey,
          destinationKey,
          strategy: null,
          stage: null,
          fallbackReason: null,
          totalBytes: null,
          transferredBytes: BigInt(0),
          throughputBytesPerSec: null,
          etaSeconds: null,
          lastProgressAtMs: null,
          lastSpeedSampleAtMs: null,
          lastSpeedSampleBytes: BigInt(0),
          lastSampleAtMs: null,
          lastSampleBytes: BigInt(0),
          lastSampleStage: null,
          emittedMilestones: new Set<number>(),
          sampledEvents: 0,
        }
        transferTelemetryByFile.set(key, created)
        return created
      }

      function getActiveTelemetryState(): LiveTransferTelemetryState | null {
        if (!activeTransferTelemetryKey) return null
        return transferTelemetryByFile.get(activeTransferTelemetryKey) ?? null
      }

      function buildLiveTransferProgressSnapshot(): ObjectTransferTaskProgress {
        const activeTelemetryState = getActiveTelemetryState()
        const activeTransferredBytes = activeTelemetryState?.transferredBytes ?? BigInt(0)
        const bytesProcessedWithCurrent = bytesProcessedCompleted + activeTransferredBytes
        const boundedBytesProcessed =
          bytesEstimatedTotal !== null && bytesProcessedWithCurrent > bytesEstimatedTotal
            ? bytesEstimatedTotal
            : bytesProcessedWithCurrent

        return {
          phase: "transfer",
          total: sourceTotal,
          processed: progress.processed + processedInBatch,
          copied: progress.copied + copiedInBatch,
          moved: progress.moved + movedInBatch,
          deleted: progress.deleted + deletedInBatch,
          skipped: progress.skipped + skippedInBatch,
          failed: progress.failed + failedInBatch,
          remaining: Math.max(0, sourceTotal - (progress.processed + processedInBatch)),
          cursorKey: lastProcessedCursorKey,
          currentFileKey: activeTelemetryState?.sourceKey ?? null,
          currentFileSizeBytes: activeTelemetryState?.totalBytes?.toString() ?? null,
          currentFileTransferredBytes: activeTelemetryState
            ? activeTelemetryState.transferredBytes.toString()
            : null,
          currentFileStage: activeTelemetryState?.stage ?? null,
          transferStrategy: activeTelemetryState?.strategy ?? null,
          fallbackReason: activeTelemetryState?.fallbackReason ?? null,
          bytesProcessedTotal: boundedBytesProcessed.toString(),
          bytesEstimatedTotal: bytesEstimatedTotal?.toString() ?? null,
          throughputBytesPerSec: activeTelemetryState?.throughputBytesPerSec ?? null,
          etaSeconds: activeTelemetryState?.etaSeconds ?? null,
          lastProgressAt: activeTelemetryState?.lastProgressAtMs
            ? new Date(activeTelemetryState.lastProgressAtMs).toISOString()
            : null,
        }
      }

      function queueTelemetryWrite(operation: () => Promise<void>) {
        telemetryWriteQueue = telemetryWriteQueue
          .then(operation)
          .catch(() => {
            // Telemetry writes are best effort and should not fail task processing.
          })
      }

      function persistLiveProgressSnapshot(force = false) {
        const nowMs = Date.now()
        if (!force && nowMs - lastTelemetryProgressPersistAt < transferProgressSampleIntervalMs) {
          return
        }
        lastTelemetryProgressPersistAt = nowMs
        const snapshot = buildLiveTransferProgressSnapshot()
        queueTelemetryWrite(async () => {
          await prisma.backgroundTask.updateMany({
            where: {
              id: claimedTaskId,
              userId: actorUserId,
              runCount: claimedRunCount,
              status: "in_progress",
            },
            data: {
              progress: snapshot as unknown as Prisma.InputJsonObject,
              nextRunAt: new Date(Date.now() + LOCK_SECONDS * 1000),
            },
          })
        })
      }

      function emitSampledProgressEvent(
        state: LiveTransferTelemetryState,
        sampleReason: TransferProgressSampleReason
      ) {
        if (state.sampledEvents >= transferProgressMaxEventsPerFile) return
        if (state.totalBytes === null || state.totalBytes < transferProgressMinFileSizeBytes) return

        const percent =
          state.totalBytes && state.totalBytes > BigInt(0)
            ? Math.min(100, Math.floor((bigintToNumberLossy(state.transferredBytes) * 100) / Math.max(1, bigintToNumberLossy(state.totalBytes))))
            : null
        const throughputLabel =
          state.throughputBytesPerSec !== null
            ? `${Math.max(0, Math.round(state.throughputBytesPerSec))} B/s`
            : "n/a"
        const etaLabel =
          state.etaSeconds !== null
            ? `${Math.max(0, Math.floor(state.etaSeconds))}s`
            : "n/a"
        const message = [
          `PROGRESS ${activeTransferPayload.sourceBucket}/${state.sourceKey} -> ${activeTransferPayload.destinationBucket}/${state.destinationKey}`,
          percent !== null ? `${percent}%` : "size unknown",
          `stage=${state.stage ?? "copying"}`,
          `speed=${throughputLabel}`,
          `eta=${etaLabel}`,
        ].join(" ")

        state.sampledEvents += 1
        queueTelemetryWrite(async () => {
          await prisma.backgroundTaskEvent.create({
            data: {
              taskId: claimedTaskId,
              userId: actorUserId,
              eventType: "file_progress",
              message,
              metadata: {
                sourceKey: state.sourceKey,
                destinationKey: state.destinationKey,
                stage: state.stage,
                strategy: state.strategy,
                transferredBytes: state.transferredBytes.toString(),
                totalBytes: state.totalBytes?.toString() ?? null,
                throughputBytesPerSec: state.throughputBytesPerSec,
                etaSeconds: state.etaSeconds,
                sampleReason,
              },
            },
          })
        })
      }

      function markReachedMilestones(state: LiveTransferTelemetryState) {
        if (!state.totalBytes || state.totalBytes <= BigInt(0)) return
        for (const milestone of TRANSFER_PROGRESS_MILESTONES) {
          const threshold = (state.totalBytes * BigInt(milestone)) / BigInt(100)
          if (state.transferredBytes >= threshold) {
            state.emittedMilestones.add(milestone)
          }
        }
      }

      function maybeEmitProgressSample(
        state: LiveTransferTelemetryState,
        nowMs: number,
        stageChanged: boolean
      ) {
        if (state.sampledEvents >= transferProgressMaxEventsPerFile) return
        if (state.totalBytes === null || state.totalBytes < transferProgressMinFileSizeBytes) return

        const reachedNewMilestone =
          state.totalBytes && state.totalBytes > BigInt(0)
            ? TRANSFER_PROGRESS_MILESTONES.some((milestone) => {
              if (state.emittedMilestones.has(milestone)) return false
              const threshold = (state.totalBytes! * BigInt(milestone)) / BigInt(100)
              return state.transferredBytes >= threshold
            })
            : false

        const intervalTriggered =
          state.lastSampleAtMs === null || nowMs - state.lastSampleAtMs >= transferProgressSampleIntervalMs
        const deltaTriggered =
          state.transferredBytes - state.lastSampleBytes >= transferProgressSampleDeltaBytes

        let reason: TransferProgressSampleReason | null = null
        if (stageChanged) {
          reason = "stage_change"
        } else if (reachedNewMilestone) {
          reason = "milestone"
        } else if (deltaTriggered) {
          reason = "delta"
        } else if (intervalTriggered) {
          reason = "interval"
        }

        if (!reason) return

        emitSampledProgressEvent(state, reason)
        state.lastSampleAtMs = nowMs
        state.lastSampleBytes = state.transferredBytes
        state.lastSampleStage = state.stage
        markReachedMilestones(state)
      }

      function updateTelemetryProgressSpeed(state: LiveTransferTelemetryState, nowMs: number) {
        if (state.lastSpeedSampleAtMs === null) {
          state.lastSpeedSampleAtMs = nowMs
          state.lastSpeedSampleBytes = state.transferredBytes
          return
        }

        const deltaMs = nowMs - state.lastSpeedSampleAtMs
        const deltaBytes = state.transferredBytes - state.lastSpeedSampleBytes
        if (deltaMs <= 0 || deltaBytes < BigInt(0)) {
          return
        }

        const instantBytesPerSecond = (bigintToNumberLossy(deltaBytes) * 1000) / deltaMs
        if (Number.isFinite(instantBytesPerSecond) && instantBytesPerSecond >= 0) {
          state.throughputBytesPerSec =
            state.throughputBytesPerSec === null
              ? instantBytesPerSecond
              : state.throughputBytesPerSec * 0.7 + instantBytesPerSecond * 0.3
        }

        state.lastSpeedSampleAtMs = nowMs
        state.lastSpeedSampleBytes = state.transferredBytes

        if (
          state.totalBytes !== null &&
          state.totalBytes > BigInt(0) &&
          state.throughputBytesPerSec &&
          state.throughputBytesPerSec > 0 &&
          state.transferredBytes <= state.totalBytes
        ) {
          const remainingBytes = state.totalBytes - state.transferredBytes
          state.etaSeconds = Math.ceil(
            bigintToNumberLossy(remainingBytes) / state.throughputBytesPerSec
          )
        } else {
          state.etaSeconds = null
        }
      }

      const transferTelemetryHooks: TransferTelemetryHooks = {
        start: ({ sourceKey, destinationKey, strategy, totalBytes }) => {
          const state = getOrCreateTelemetryState(sourceKey, destinationKey)
          state.strategy = strategy
          state.totalBytes = totalBytes ?? state.totalBytes
          state.stage = "queued"
          state.fallbackReason = null
          state.transferredBytes = BigInt(0)
          state.throughputBytesPerSec = null
          state.etaSeconds = null
          const nowMs = Date.now()
          state.lastProgressAtMs = nowMs
          state.lastSpeedSampleAtMs = null
          state.lastSpeedSampleBytes = BigInt(0)
          activeTransferTelemetryKey = getTelemetryStateKey(sourceKey, destinationKey)
          persistLiveProgressSnapshot(true)
          maybeEmitProgressSample(state, nowMs, true)
        },
        progress: ({ sourceKey, destinationKey, strategy, transferredBytes, totalBytes, stage }) => {
          const state = getOrCreateTelemetryState(sourceKey, destinationKey)
          const previousStage = state.stage
          state.strategy = strategy
          state.totalBytes = totalBytes ?? state.totalBytes
          state.transferredBytes = transferredBytes < BigInt(0) ? BigInt(0) : transferredBytes
          if (state.totalBytes !== null && state.transferredBytes > state.totalBytes) {
            state.transferredBytes = state.totalBytes
          }
          state.stage = stage ?? state.stage ?? "copying"
          const nowMs = Date.now()
          state.lastProgressAtMs = nowMs
          updateTelemetryProgressSpeed(state, nowMs)
          activeTransferTelemetryKey = getTelemetryStateKey(sourceKey, destinationKey)
          maybeEmitProgressSample(state, nowMs, previousStage !== state.stage)
          persistLiveProgressSnapshot(false)
        },
        stage: ({ sourceKey, destinationKey, strategy, stage }) => {
          const state = getOrCreateTelemetryState(sourceKey, destinationKey)
          const previousStage = state.stage
          state.strategy = strategy ?? state.strategy
          state.stage = stage
          const nowMs = Date.now()
          state.lastProgressAtMs = nowMs
          activeTransferTelemetryKey = getTelemetryStateKey(sourceKey, destinationKey)
          maybeEmitProgressSample(state, nowMs, previousStage !== stage)
          persistLiveProgressSnapshot(true)
        },
        fallback: ({ sourceKey, destinationKey, reason, nextStrategy }) => {
          const state = getOrCreateTelemetryState(sourceKey, destinationKey)
          state.fallbackReason = reason
          state.strategy = nextStrategy
          state.lastProgressAtMs = Date.now()
          activeTransferTelemetryKey = getTelemetryStateKey(sourceKey, destinationKey)
          persistLiveProgressSnapshot(true)
        },
        finish: ({ sourceKey, destinationKey, strategy, status }) => {
          const state = getOrCreateTelemetryState(sourceKey, destinationKey)
          const previousStage = state.stage
          state.strategy = strategy ?? state.strategy
          state.stage = status === "completed" ? "completed" : "failed"
          if (status === "completed" && state.totalBytes !== null) {
            state.transferredBytes = state.totalBytes
          }
          const nowMs = Date.now()
          state.lastProgressAtMs = nowMs
          maybeEmitProgressSample(state, nowMs, previousStage !== state.stage)
          persistLiveProgressSnapshot(true)
        },
      }

      for (let index = 0; index < actionableBatch.length; index += transferItemConcurrency) {
        if (
          processedInBatch > 0 &&
          Date.now() - batchStartedAt >= getTaskWorkerUserBudgetMs()
        ) {
          timeBudgetReached = true
          break
        }

        const slice = actionableBatch.slice(index, index + transferItemConcurrency)
        const prepared = await Promise.all(
          slice.map(async ({ sourceFile, destinationKey }): Promise<PreparedTransferItem> => {
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
            let shouldSkipForUpToDateSync =
              activeTransferPayload.operation === "sync" &&
              destinationExisting &&
              isDestinationUpToDateForSync(
                {
                  size: sourceFile.size,
                  lastModified: sourceFile.lastModified,
                },
                destinationExisting
              )

            // When sync would skip based on cached metadata, verify the
            // destination object actually exists in S3. The cache can be stale
            // if files were deleted outside the app (lifecycle rules, external
            // tools, etc.), causing the sync to incorrectly skip missing files.
            if (shouldSkipForUpToDateSync && !destinationExistsRemotely) {
              try {
                const verifySnapshot = await readRemoteObjectSnapshot({
                  client: destinationClient,
                  bucket: activeTransferPayload.destinationBucket,
                  key: destinationKey,
                })
                if (!verifySnapshot) {
                  shouldSkipForUpToDateSync = false
                  destinationByKey.delete(destinationKey)
                  staleDestinationKeys.push(destinationKey)
                }
              } catch {
                // Verification failed — be safe, proceed with copy.
                shouldSkipForUpToDateSync = false
              }
            }

            return {
              sourceFile,
              destinationKey,
              createsNewDestination: !destinationExisting && !destinationExistsRemotely,
              skip: Boolean(shouldSkipForExistingDestination || shouldSkipForUpToDateSync),
              skipReason:
                shouldSkipForExistingDestination
                  ? "already_exists"
                  : shouldSkipForUpToDateSync
                    ? "up_to_date"
                    : null,
            }
          })
        )

        const actionable: PreparedTransferItem[] = []
        const skippedForResults: Array<{
          sourceFile: TransferSourceRow
          destinationKey: string
          reason: TransferSkipReason
        }> = []
        for (const item of prepared) {
          if (item.skip) {
            skippedInBatch++
            processedInBatch++
            skippedForResults.push({
              sourceFile: item.sourceFile,
              destinationKey: item.destinationKey,
              reason: item.skipReason ?? "up_to_date",
            })
            continue
          }

          if (item.createsNewDestination && remainingCacheSlots !== null) {
            if (remainingCacheSlots <= 0) {
              skippedInBatch++
              processedInBatch++
              skippedForResults.push({
                sourceFile: item.sourceFile,
                destinationKey: item.destinationKey,
                reason: "cache_limit_reached",
              })
              continue
            }
            remainingCacheSlots -= 1
          }

          actionable.push(item)
        }

        const retryMaxAttempts = getTaskTransferItemRetryMaxAttempts()
        const retryBaseDelayMs = getTaskTransferItemRetryBaseDelayMs()

        let results = await Promise.all(
          actionable.map(async (item): Promise<TransferItemResult> => {
            let lastError: unknown = null

            for (let attempt = 0; attempt <= retryMaxAttempts; attempt++) {
              try {
                if (attempt > 0) {
                  const delay = computeRetryDelayMs(attempt - 1, retryBaseDelayMs)
                  await sleep(delay)
                }

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
                  telemetry: transferTelemetryHooks,
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
                lastError = itemError

                // Don't retry non-transient errors or missing source
                if (isS3MissingObjectError(itemError) || !isTransientS3Error(itemError)) {
                  break
                }

                // Don't retry if we've exhausted attempts
                if (attempt >= retryMaxAttempts) {
                  break
                }
              }
            }

            const errorCode = getS3ErrorCode(lastError)
            const errorMessage = formatTaskProcessingError(lastError)
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
          await Promise.all(
            copiedRows.map((result) =>
              transferTelemetryHooks.stage?.({
                sourceKey: result.sourceKey,
                destinationKey: result.destinationKey,
                strategy: null,
                stage: "deleting_source",
              })
            )
          )

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

          // missing_source items are handled gracefully (stale source cache
          // entries cleaned up), so don't surface their error as a task-level error.
          if (!batchLastError && result.errorMessage && result.status !== "missing_source") {
            batchLastError = result.errorMessage
          }

          processedInBatch++
          if (result.status === "copied") {
            copiedInBatch++
            bytesProcessedCompleted += result.size
          } else if (result.status === "moved") {
            movedInBatch++
            deletedInBatch++
            bytesProcessedCompleted += result.size
          } else if (result.status === "skipped" || result.status === "missing_source") {
            skippedInBatch++
          } else {
            if (result.status === "failed" && result.sourceDeleteRequired) {
              bytesProcessedCompleted += result.size
            }
            failedInBatch++
          }

          const telemetryStateKey = getTelemetryStateKey(result.sourceKey, result.destinationKey)
          transferTelemetryByFile.delete(telemetryStateKey)
          if (activeTransferTelemetryKey === telemetryStateKey) {
            activeTransferTelemetryKey = transferTelemetryByFile.keys().next().value ?? null
          }
        }

        persistLiveProgressSnapshot(true)

        // Record per-file events for this slice
        const fileEvents: Prisma.BackgroundTaskEventCreateManyInput[] = []
        for (const skippedItem of skippedForResults) {
          const reasonLabel = formatTransferSkipReason(skippedItem.reason)
          fileEvents.push({
            taskId: candidate.id,
            userId: actorUserId,
            eventType: "file_skipped",
            message: `SKIP ${activeTransferPayload.sourceBucket}/${skippedItem.sourceFile.key} -> ${activeTransferPayload.destinationBucket}/${skippedItem.destinationKey} (${reasonLabel})`,
            metadata: {
              sourceKey: skippedItem.sourceFile.key,
              destinationKey: skippedItem.destinationKey,
              size: skippedItem.sourceFile.size.toString(),
              reason: skippedItem.reason,
            },
          })
        }
        for (const result of results) {
          fileEvents.push({
            taskId: candidate.id,
            userId: actorUserId,
            eventType: `file_${result.status}`,
            message: `${result.status.toUpperCase()} ${activeTransferPayload.sourceBucket}/${result.sourceKey} -> ${activeTransferPayload.destinationBucket}/${result.destinationKey}`,
            metadata: {
              sourceKey: result.sourceKey,
              destinationKey: result.destinationKey,
              size: result.size.toString(),
              error: result.errorMessage ?? undefined,
            },
          })
        }
        if (fileEvents.length > 0) {
          try {
            await prisma.backgroundTaskEvent.createMany({ data: fileEvents })
          } catch {
            // Non-critical: don't fail the task if event recording fails
          }
        }

        lastProcessedCursorKey = slice[slice.length - 1]?.sourceFile.key ?? lastProcessedCursorKey
      }

      // When the actionable loop completed fully (no time budget break),
      // advance cursor to the end of the original batch so bulk-skipped
      // files at the tail are not re-fetched in the next batch call.
      if (!timeBudgetReached && bulkSkippedCount > 0 && mappedBatch.length > 0) {
        const lastBatchKey = mappedBatch[mappedBatch.length - 1]!.sourceFile.key
        if (!lastProcessedCursorKey || lastBatchKey > lastProcessedCursorKey) {
          lastProcessedCursorKey = lastBatchKey
        }
      }

      // Clean up stale destination metadata entries discovered during sync
      // verification. These are cache entries for files no longer in S3.
      if (staleDestinationKeys.length > 0) {
        await prisma.fileMetadata.deleteMany({
          where: {
            userId: actorUserId,
            credentialId: activeTransferPayload.destinationCredentialId,
            bucket: activeTransferPayload.destinationBucket,
            key: { in: staleDestinationKeys },
          },
        })
      }

      await telemetryWriteQueue

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
        currentFileKey: null,
        currentFileSizeBytes: null,
        currentFileTransferredBytes: null,
        currentFileStage: null,
        transferStrategy: null,
        fallbackReason: null,
        bytesProcessedTotal: bytesProcessedCompleted.toString(),
        bytesEstimatedTotal: bytesEstimatedTotal?.toString() ?? null,
        throughputBytesPerSec: null,
        etaSeconds: null,
        lastProgressAt: null,
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
        const response = await getS3Client(actorUserId, group.credentialId, {
          trafficClass: "background",
        })
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
