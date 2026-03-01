import { NextRequest, NextResponse } from "next/server"
import { GetObjectCommand } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

function extractFilename(key: string): string {
  const normalized = key.endsWith("/") ? key.slice(0, -1) : key
  return normalized.split("/").pop() || "download"
}

function toContentDispositionFilename(filename: string): string {
  return filename.replace(/["\\]/g, "_")
}

function shouldUseProxyDownload(provider: string): boolean {
  return provider.trim().toUpperCase() === "STORADERA"
}

export async function POST(request: NextRequest) {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const body = await request.json()
    const { bucket, key } = body

    if (!bucket || !key) {
      return NextResponse.json({ error: "bucket and key are required" }, { status: 400 })
    }

    const { client, credential } = getDemoS3Client()
    const filename = extractFilename(key)

    let url: string
    if (shouldUseProxyDownload(credential.provider)) {
      const params = new URLSearchParams({ bucket, key })
      url = `/api/demo/s3/download/proxy?${params.toString()}`
    } else {
      url = await getSignedUrl(
        client,
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
          ResponseContentDisposition: `attachment; filename="${toContentDispositionFilename(filename)}"`,
        }),
        { expiresIn: 3600 }
      )
    }

    return NextResponse.json({ url, filename })
  } catch (error) {
    console.error("Demo: Failed to generate download URL:", error)
    return NextResponse.json({ error: "Failed to generate download URL" }, { status: 500 })
  }
}
