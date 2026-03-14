import { NextRequest, NextResponse } from "next/server"
import { GetObjectCommand } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { demoGuard, getDemoS3Client } from "@/lib/demo"
import { isStoraderaProvider } from "@/lib/s3-provider"

const PREVIEW_URL_TTL_SECONDS = 86400

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
    const isStoradera = isStoraderaProvider(credential.provider)

    let url: string
    if (isStoradera) {
      const params = new URLSearchParams({ bucket, key })
      url = `/api/demo/s3/preview/proxy?${params.toString()}`
    } else {
      url = await getSignedUrl(
        client,
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
          ResponseContentDisposition: "inline",
          ResponseCacheControl: `public, max-age=${PREVIEW_URL_TTL_SECONDS}`,
        }),
        { expiresIn: PREVIEW_URL_TTL_SECONDS }
      )
    }

    return NextResponse.json({ url })
  } catch (error) {
    console.error("Demo: Failed to create preview URL:", error)
    return NextResponse.json({ error: "Failed to create preview URL" }, { status: 500 })
  }
}
