import { createHash } from "node:crypto"
import { CopyObjectCommand, DeleteObjectsCommand, PutObjectCommand, type S3Client } from "@aws-sdk/client-s3"
import { envVar } from "@/lib/env"

export const THUMBNAIL_OBJECT_PREFIX = ".s3-admin-generated-thumbnails/"
export const THUMBNAIL_SOURCE_LAST_MODIFIED_META_KEY = "s3admin-source-last-modified"
export const THUMBNAIL_SOURCE_SIZE_META_KEY = "s3admin-source-size"

export function isThumbnailGenerationEnabled(): boolean {
  const raw = envVar("THUMBNAIL_GENERATION_ENABLED").trim().toLowerCase()
  if (!raw) return true // enabled by default in community edition
  return raw === "true" || raw === "1" || raw === "yes"
}

export function getThumbnailUrlTtlSeconds(): number {
  const raw = envVar("THUMBNAIL_URL_TTL_SECONDS")
  const parsed = Number.parseInt(raw, 10)
  if (!Number.isFinite(parsed) || parsed <= 0) return 86400 // 24 hours — thumbnails are immutable
  return Math.min(604800, parsed) // max 7 days
}

export function getThumbnailMaxWidth(): number {
  const raw = envVar("THUMBNAIL_MAX_WIDTH")
  const parsed = Number.parseInt(raw, 10)
  if (!Number.isFinite(parsed) || parsed < 64) return 480
  return Math.min(2048, parsed)
}

function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex")
}

export function buildThumbnailObjectKey(input: {
  bucket: string
  key: string
  sourceLastModified: Date
  sourceSize: bigint
}): string {
  // Deterministic and credential-agnostic key so thumbnails can be reused after
  // credential reconfiguration. Source fingerprint is stored in object metadata.
  const hash = sha256(`v2|${input.bucket}|${input.key}`)
  return `${THUMBNAIL_OBJECT_PREFIX}${hash}.webp`
}

export function buildLegacyThumbnailObjectKey(input: {
  bucket: string
  key: string
  sourceLastModified: Date
  sourceSize: bigint
}): string {
  const hash = sha256(
    `${input.bucket}|${input.key}|${input.sourceLastModified.toISOString()}|${input.sourceSize.toString()}`
  )
  return `${THUMBNAIL_OBJECT_PREFIX}${hash}.webp`
}

export function buildThumbnailSourceMetadata(input: {
  sourceLastModified: Date
  sourceSize: bigint
}): Record<string, string> {
  return {
    [THUMBNAIL_SOURCE_LAST_MODIFIED_META_KEY]: input.sourceLastModified.toISOString(),
    [THUMBNAIL_SOURCE_SIZE_META_KEY]: input.sourceSize.toString(),
  }
}

export function doesThumbnailMetadataMatchSource(
  metadata: Record<string, string> | undefined,
  source: { sourceLastModified: Date; sourceSize: bigint }
): boolean {
  const sourceLastModified = metadata?.[THUMBNAIL_SOURCE_LAST_MODIFIED_META_KEY]
  const sourceSize = metadata?.[THUMBNAIL_SOURCE_SIZE_META_KEY]

  if (!sourceLastModified || !sourceSize) {
    return false
  }

  return (
    sourceLastModified === source.sourceLastModified.toISOString() &&
    sourceSize === source.sourceSize.toString()
  )
}

export async function uploadThumbnailToSameBucket(params: {
  client: S3Client
  bucket: string
  key: string
  body: Buffer
  contentType?: string
  metadata?: Record<string, string>
}) {
  await params.client.send(
    new PutObjectCommand({
      Bucket: params.bucket,
      Key: params.key,
      Body: params.body,
      ContentType: params.contentType ?? "image/webp",
      CacheControl: "public, max-age=31536000, immutable",
      Metadata: params.metadata,
    })
  )
}

export async function deleteThumbnailsFromBucket(params: {
  client: S3Client
  bucket: string
  keys: string[]
}) {
  if (params.keys.length === 0) return

  for (let i = 0; i < params.keys.length; i += 1000) {
    const batch = params.keys.slice(i, i + 1000)
    await params.client.send(
      new DeleteObjectsCommand({
        Bucket: params.bucket,
        Delete: {
          Objects: batch.map((Key) => ({ Key })),
          Quiet: true,
        },
      })
    )
  }
}

export async function copyThumbnailInBucket(params: {
  client: S3Client
  bucket: string
  oldKey: string
  newKey: string
}) {
  await params.client.send(
    new CopyObjectCommand({
      Bucket: params.bucket,
      Key: params.newKey,
      CopySource: encodeURIComponent(`${params.bucket}/${params.oldKey}`),
      CacheControl: "public, max-age=31536000, immutable",
      MetadataDirective: "REPLACE",
      ContentType: "image/webp",
    })
  )
}
