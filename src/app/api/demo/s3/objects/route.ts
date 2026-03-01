import { NextRequest, NextResponse } from "next/server"
import { ListObjectsV2Command } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export async function GET(request: NextRequest) {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const { searchParams } = request.nextUrl
    const bucket = searchParams.get("bucket") ?? ""
    const prefix = searchParams.get("prefix") ?? ""

    if (!bucket) {
      return NextResponse.json({ error: "bucket is required" }, { status: 400 })
    }

    const { client } = getDemoS3Client()

    const response = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        Delimiter: "/",
        MaxKeys: 1000,
      })
    )

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
          key: folderPrefix,
          size: 0,
          lastModified: new Date().toISOString(),
          isFolder: true,
          totalSize,
          fileCount,
        }
      })
    )

    const files = (response.Contents ?? [])
      .filter((obj) => obj.Key && obj.Key !== prefix)
      .map((obj) => ({
        key: obj.Key!,
        size: obj.Size ?? 0,
        lastModified: obj.LastModified?.toISOString() ?? new Date().toISOString(),
        isFolder: false,
      }))

    return NextResponse.json({
      folders: folders.sort((a, b) => a.key.localeCompare(b.key)),
      files: files.sort((a, b) => a.key.localeCompare(b.key)),
    })
  } catch (error) {
    console.error("Demo: Failed to list objects:", error)
    return NextResponse.json({ error: "Failed to list objects" }, { status: 500 })
  }
}
