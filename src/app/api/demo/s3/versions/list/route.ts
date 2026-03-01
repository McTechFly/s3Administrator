import { NextRequest, NextResponse } from "next/server"
import { ListObjectVersionsCommand } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export async function GET(request: NextRequest) {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const { searchParams } = request.nextUrl
    const bucket = searchParams.get("bucket") ?? ""
    const prefix = searchParams.get("prefix") ?? ""
    const limitRaw = Number(searchParams.get("limit") ?? "500")
    const limit = Math.min(1000, Math.max(1, limitRaw))

    if (!bucket) {
      return NextResponse.json({ error: "bucket is required" }, { status: 400 })
    }

    const { client } = getDemoS3Client()

    const response = await client.send(
      new ListObjectVersionsCommand({
        Bucket: bucket,
        Prefix: prefix || undefined,
        MaxKeys: limit,
      })
    )

    const versions = [
      ...(response.Versions ?? []).map((v) => ({
        key: v.Key ?? "",
        versionId: v.VersionId ?? "",
        size: v.Size ?? 0,
        lastModifiedUtc: v.LastModified?.toISOString() ?? new Date().toISOString(),
        isLatest: v.IsLatest ?? false,
        isDeleteMarker: false,
      })),
      ...(response.DeleteMarkers ?? []).map((dm) => ({
        key: dm.Key ?? "",
        versionId: dm.VersionId ?? "",
        size: 0,
        lastModifiedUtc: dm.LastModified?.toISOString() ?? new Date().toISOString(),
        isLatest: dm.IsLatest ?? false,
        isDeleteMarker: true,
      })),
    ].filter((v) => v.key.length > 0)

    return NextResponse.json({
      versions,
      pagination: {
        hasMore: response.IsTruncated ?? false,
      },
    })
  } catch (error) {
    console.error("Demo: Failed to list versions:", error)
    return NextResponse.json({ error: "Failed to list versions" }, { status: 500 })
  }
}
