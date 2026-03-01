import { NextRequest, NextResponse } from "next/server"
import { ListBucketsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"
import { getMediaTypeFromExtension } from "@/lib/media"

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
    const query = searchParams.get("q") ?? ""
    const bucket = searchParams.get("bucket") ?? ""
    const skipRaw = Number(searchParams.get("skip") || "0")
    const takeRaw = Number(searchParams.get("take") || "100")
    const skip = Math.max(0, Math.floor(skipRaw) || 0)
    const take = Math.min(100, Math.max(1, Math.floor(takeRaw) || 100))

    if (query.trim().length < 2) {
      return NextResponse.json({ results: [], total: 0 })
    }

    const { client, credential } = getDemoS3Client()
    const lowerQuery = query.toLowerCase()

    const bucketNames = bucket
      ? [bucket]
      : (await client.send(new ListBucketsCommand({}))).Buckets
          ?.map((entry) => entry.Name)
          .filter((name): name is string => Boolean(name)) ?? []

    // List all objects and filter by query
    const allObjects: Array<{
      bucket: string
      key: string
      size: number
      lastModified: Date
    }> = []

    for (const bucketName of bucketNames) {
      let continuationToken: string | undefined

      do {
        const response = await client.send(
          new ListObjectsV2Command({
            Bucket: bucketName,
            MaxKeys: 1000,
            ContinuationToken: continuationToken,
          })
        )
        for (const obj of response.Contents ?? []) {
          if (obj.Key && obj.Key.toLowerCase().includes(lowerQuery)) {
            allObjects.push({
              bucket: bucketName,
              key: obj.Key,
              size: obj.Size ?? 0,
              lastModified: obj.LastModified ?? new Date(),
            })
          }
        }
        continuationToken = response.IsTruncated
          ? response.NextContinuationToken
          : undefined
      } while (continuationToken)
    }

    allObjects.sort((a, b) => {
      const byModified = b.lastModified.getTime() - a.lastModified.getTime()
      if (byModified !== 0) return byModified
      return a.key.localeCompare(b.key)
    })

    const total = allObjects.length
    const page = allObjects.slice(skip, skip + take)

    const results = page.map((obj, idx) => {
      const extension = getExtension(obj.key)
      const mediaType = getMediaTypeFromExtension(extension)
      const previewUrl = mediaType
        ? `/api/demo/s3/preview/proxy?${new URLSearchParams({
            bucket: obj.bucket,
            key: obj.key,
          }).toString()}`
        : null

      return {
        id: `demo-search-${skip + idx}`,
        key: obj.key,
        bucket: obj.bucket,
        credentialId: credential.id,
        extension,
        mediaType,
        previewUrl,
        isVideo: mediaType === "video",
        size: obj.size,
        lastModified: obj.lastModified.toISOString(),
      }
    })

    return NextResponse.json({ results, total })
  } catch (error) {
    console.error("Demo: Failed to search files:", error)
    return NextResponse.json({ error: "Failed to search files" }, { status: 500 })
  }
}
