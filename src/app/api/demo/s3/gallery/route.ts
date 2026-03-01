import { NextRequest, NextResponse } from "next/server"
import { ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { demoGuard, getDemoS3Client } from "@/lib/demo"
import { getMediaTypeFromExtension, isGallerySupportedExtension } from "@/lib/media"

const PREVIEW_URL_TTL_SECONDS = 86400

function encodeCursor(offset: number): string {
  return Buffer.from(JSON.stringify({ offset }), "utf8").toString("base64url")
}

function decodeCursor(raw: string): { offset: number } | null {
  try {
    const parsed = JSON.parse(Buffer.from(raw, "base64url").toString("utf8"))
    if (typeof parsed.offset !== "number" || parsed.offset < 0) return null
    return { offset: Math.floor(parsed.offset) }
  } catch {
    return null
  }
}

function getExtension(key: string): string {
  const dot = key.lastIndexOf(".")
  if (dot <= 0) return ""
  return key.slice(dot + 1).toLowerCase()
}

export async function GET(request: NextRequest) {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const { searchParams } = request.nextUrl
    const bucket = searchParams.get("bucket") ?? ""
    const prefix = searchParams.get("prefix") ?? ""
    const cursor = searchParams.get("cursor") ?? null
    const limitRaw = Number(searchParams.get("limit") ?? "25")
    const limit = Math.min(100, Math.max(1, limitRaw))

    if (!bucket) {
      return NextResponse.json({ error: "bucket is required" }, { status: 400 })
    }

    const { client, credential } = getDemoS3Client()
    const isStoradera = credential.provider.trim().toUpperCase() === "STORADERA"
    const cursorData = cursor ? decodeCursor(cursor) : { offset: 0 }

    if (!cursorData) {
      return NextResponse.json({ error: "Invalid cursor" }, { status: 400 })
    }

    // List all objects under the prefix
    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        Delimiter: "/",
        MaxKeys: 1000,
      })
    )

    // Build folders with file counts
    const folderPrefixes = (response.CommonPrefixes ?? [])
      .filter((cp) => cp.Prefix)
      .map((cp) => cp.Prefix!)

    const folders = await Promise.all(
      folderPrefixes.map(async (folderPrefix) => {
        let fileCount = 0
        let totalSize = 0
        let continuationToken: string | undefined

        do {
          const listing = await client.send(
            new ListObjectsV2Command({
              Bucket: bucket,
              Prefix: folderPrefix,
              MaxKeys: 1000,
              ContinuationToken: continuationToken,
            })
          )
          for (const obj of listing.Contents ?? []) {
            fileCount++
            totalSize += obj.Size ?? 0
          }
          continuationToken = listing.IsTruncated
            ? listing.NextContinuationToken
            : undefined
        } while (continuationToken)

        return {
          kind: "folder" as const,
          key: folderPrefix,
          lastModified: new Date(),
          fileCount,
          totalSize,
        }
      })
    )

    // Build gallery-eligible files
    const mediaFiles = (response.Contents ?? [])
      .filter((obj) => {
        if (!obj.Key || obj.Key === prefix) return false
        const ext = getExtension(obj.Key)
        return isGallerySupportedExtension(ext)
      })
      .map((obj) => ({
        kind: "file" as const,
        key: obj.Key!,
        size: obj.Size ?? 0,
        lastModified: obj.LastModified ?? new Date(),
        extension: getExtension(obj.Key!),
      }))

    // Merge and sort by date desc
    type Candidate = (typeof folders)[number] | (typeof mediaFiles)[number]
    const merged: Candidate[] = [...folders, ...mediaFiles].sort(
      (a, b) => b.lastModified.getTime() - a.lastModified.getTime()
    )

    // Paginate
    const start = cursorData.offset
    const endExclusive = start + limit
    const page = merged.slice(start, endExclusive)
    const hasMore = endExclusive < merged.length
    const nextCursor = hasMore ? encodeCursor(endExclusive) : null

    const items = await Promise.all(
      page.map(async (candidate, idx) => {
        if (candidate.kind === "folder") {
          return {
            id: `folder:${candidate.key}`,
            key: candidate.key,
            size: 0,
            lastModified: candidate.lastModified.toISOString(),
            extension: "",
            mediaType: null,
            previewUrl: null,
            isVideo: false,
            isFolder: true,
            fileCount: candidate.fileCount,
            totalSize: candidate.totalSize,
          }
        }

        const mediaType = getMediaTypeFromExtension(candidate.extension)
        let previewUrl: string | null = null

        if (mediaType) {
          if (isStoradera) {
            const params = new URLSearchParams({ bucket, key: candidate.key })
            previewUrl = `/api/demo/s3/preview/proxy?${params.toString()}`
          } else {
            try {
              previewUrl = await getSignedUrl(
                client,
                new GetObjectCommand({
                  Bucket: bucket,
                  Key: candidate.key,
                  ResponseContentDisposition: "inline",
                  ResponseCacheControl: `public, max-age=${PREVIEW_URL_TTL_SECONDS}`,
                }),
                { expiresIn: PREVIEW_URL_TTL_SECONDS }
              )
            } catch {
              previewUrl = null
            }
          }
        }

        return {
          id: `demo-${start + idx}`,
          key: candidate.key,
          size: candidate.size,
          lastModified: candidate.lastModified.toISOString(),
          extension: candidate.extension,
          mediaType,
          previewUrl,
          isVideo: mediaType === "video",
          isFolder: false,
          fileCount: undefined,
          totalSize: undefined,
        }
      })
    )

    return NextResponse.json({ items, nextCursor, hasMore })
  } catch (error) {
    console.error("Demo: Failed to list gallery items:", error)
    return NextResponse.json({ error: "Failed to list gallery items" }, { status: 500 })
  }
}
