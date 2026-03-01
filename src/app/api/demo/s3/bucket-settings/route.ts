import { NextRequest, NextResponse } from "next/server"
import { GetBucketVersioningCommand } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export async function GET(request: NextRequest) {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const bucket = request.nextUrl.searchParams.get("bucket") ?? ""

    if (!bucket) {
      return NextResponse.json({ error: "bucket is required" }, { status: 400 })
    }

    const { client } = getDemoS3Client()

    let versioningStatus = "unversioned"
    try {
      const versioning = await client.send(
        new GetBucketVersioningCommand({ Bucket: bucket })
      )
      versioningStatus = versioning.Status?.toLowerCase() ?? "unversioned"
    } catch {
      // Some providers don't support versioning queries
    }

    return NextResponse.json({
      settings: {
        versioning: { status: versioningStatus },
      },
    })
  } catch (error) {
    console.error("Demo: Failed to get bucket settings:", error)
    return NextResponse.json({ error: "Failed to get bucket settings" }, { status: 500 })
  }
}
