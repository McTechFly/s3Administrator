import { NextResponse } from "next/server"
import { ListBucketsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

function getExtension(key: string): string {
  const fileName = key.split("/").pop() ?? key
  if (!fileName) return ""
  const dotIndex = fileName.lastIndexOf(".")
  if (dotIndex <= 0 || dotIndex === fileName.length - 1) return ""
  return fileName.slice(dotIndex + 1).toLowerCase()
}

export async function GET() {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const { client, credential } = getDemoS3Client()

    const bucketsResponse = await client.send(new ListBucketsCommand({}))
    const bucketNames = (bucketsResponse.Buckets ?? [])
      .map((b) => b.Name)
      .filter(Boolean) as string[]

    let totalFiles = 0
    let totalSize = 0
    let lastModified: Date | null = null
    const extensionMap = new Map<string, { count: number; size: number }>()
    const bucketStats: Array<{
      bucket: string
      credentialId: string
      credentialLabel: string
      fileCount: number
      totalSize: number
    }> = []

    for (const bucketName of bucketNames) {
      let bucketFileCount = 0
      let bucketTotalSize = 0
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
          if (!obj.Key) continue
          const size = obj.Size ?? 0
          bucketFileCount++
          bucketTotalSize += size

          const ext = getExtension(obj.Key)
          const existing = extensionMap.get(ext)
          if (existing) {
            existing.count++
            existing.size += size
          } else {
            extensionMap.set(ext, { count: 1, size })
          }

          if (obj.LastModified) {
            if (!lastModified || obj.LastModified > lastModified) {
              lastModified = obj.LastModified
            }
          }
        }

        continuationToken = response.IsTruncated
          ? response.NextContinuationToken
          : undefined
      } while (continuationToken)

      totalFiles += bucketFileCount
      totalSize += bucketTotalSize

      bucketStats.push({
        bucket: bucketName,
        credentialId: credential.id,
        credentialLabel: credential.label,
        fileCount: bucketFileCount,
        totalSize: bucketTotalSize,
      })
    }

    const extensions = Array.from(extensionMap.entries())
      .map(([ext, stats]) => ({
        extension: ext,
        fileCount: stats.count,
        totalSize: stats.size,
        type: "other",
      }))
      .sort((a, b) => b.fileCount - a.fileCount)

    return NextResponse.json({
      summary: {
        indexedBucketCount: bucketNames.length,
        indexedFileCount: totalFiles,
        indexedTotalSize: totalSize,
        distinctExtensionCount: extensionMap.size,
        lastIndexedAt: lastModified?.toISOString() ?? null,
        multipartIncomplete: {
          uploads: 0,
          parts: 0,
          totalSize: 0,
          scannedBuckets: 0,
          failedBuckets: 0,
        },
      },
      buckets: bucketStats.sort((a, b) => b.totalSize - a.totalSize),
      extensions,
      types: [],
    })
  } catch (error) {
    console.error("Demo: Failed to build overview:", error)
    return NextResponse.json({ error: "Failed to build dashboard overview" }, { status: 500 })
  }
}
