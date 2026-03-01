import { NextRequest, NextResponse } from "next/server"
import { GetObjectCommand } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export const runtime = "nodejs"

export async function GET(request: NextRequest) {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const bucket = request.nextUrl.searchParams.get("bucket") ?? ""
    const key = request.nextUrl.searchParams.get("key") ?? ""

    if (!bucket || !key) {
      return NextResponse.json({ error: "bucket and key are required" }, { status: 400 })
    }

    const { client } = getDemoS3Client()

    const response = await client.send(
      new GetObjectCommand({ Bucket: bucket, Key: key })
    )

    if (!response.Body) {
      return NextResponse.json({ error: "Empty response from storage" }, { status: 502 })
    }

    const headers = new Headers()
    headers.set("Content-Disposition", "inline")
    if (response.ContentType) {
      headers.set("Content-Type", response.ContentType)
    }
    if (response.ContentLength != null) {
      headers.set("Content-Length", String(response.ContentLength))
    }
    headers.set("Cache-Control", "public, max-age=300")

    const webStream = response.Body.transformToWebStream()
    return new NextResponse(webStream, { status: 200, headers })
  } catch (error) {
    console.error("Demo: Preview proxy failed:", error)
    return NextResponse.json({ error: "Failed to preview object" }, { status: 500 })
  }
}
