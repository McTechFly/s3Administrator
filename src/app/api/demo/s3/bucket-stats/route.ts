import { NextResponse } from "next/server"
import { ListBucketsCommand, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export async function GET() {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const { client, credential } = getDemoS3Client()

    const bucketsResponse = await client.send(new ListBucketsCommand({}))
    const bucketNames = (bucketsResponse.Buckets ?? [])
      .map((b) => b.Name)
      .filter(Boolean) as string[]

    const buckets = await Promise.all(
      bucketNames.map(async (name) => {
        let fileCount = 0
        let totalSize = 0
        let continuationToken: string | undefined

        // Paginate through all objects to get accurate stats
        do {
          const response = await client.send(
            new ListObjectsV2Command({
              Bucket: name,
              MaxKeys: 1000,
              ContinuationToken: continuationToken,
            })
          )
          for (const obj of response.Contents ?? []) {
            fileCount++
            totalSize += obj.Size ?? 0
          }
          continuationToken = response.IsTruncated
            ? response.NextContinuationToken
            : undefined
        } while (continuationToken)

        return {
          name,
          totalSize,
          fileCount,
          credentialId: credential.id,
        }
      })
    )

    return NextResponse.json({ buckets })
  } catch (error) {
    console.error("Demo: Failed to get bucket stats:", error)
    return NextResponse.json({ error: "Failed to get bucket stats" }, { status: 500 })
  }
}
