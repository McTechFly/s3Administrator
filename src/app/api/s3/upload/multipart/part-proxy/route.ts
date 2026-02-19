import { NextRequest, NextResponse } from "next/server"
import { UploadPartCommand } from "@aws-sdk/client-s3"
import { auth } from "@/lib/auth"
import { getS3Client } from "@/lib/s3"

export const runtime = "nodejs"

export async function PUT(request: NextRequest) {
  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const bucket = request.nextUrl.searchParams.get("bucket") ?? ""
    const key = request.nextUrl.searchParams.get("key") ?? ""
    const uploadId = request.nextUrl.searchParams.get("uploadId") ?? ""
    const partNumberStr = request.nextUrl.searchParams.get("partNumber") ?? ""
    const credentialId = request.nextUrl.searchParams.get("credentialId") || undefined

    const partNumber = Number(partNumberStr)
    if (!bucket || !key || !uploadId || !Number.isInteger(partNumber) || partNumber < 1 || partNumber > 10000) {
      return NextResponse.json(
        { error: "bucket, key, uploadId and partNumber (1-10000) are required" },
        { status: 400 }
      )
    }

    if (!request.body) {
      return NextResponse.json({ error: "Missing upload body" }, { status: 400 })
    }

    const { client } = await getS3Client(session.user.id, credentialId)
    const bodyBuffer = Buffer.from(await request.arrayBuffer())

    const response = await client.send(
      new UploadPartCommand({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: bodyBuffer,
        ContentLength: bodyBuffer.length,
      })
    )

    const etag = response.ETag ?? ""
    return NextResponse.json(
      { ETag: etag },
      {
        status: 200,
        headers: { ETag: etag },
      }
    )
  } catch (error) {
    console.error("Multipart part proxy upload failed:", error)
    return NextResponse.json({ error: "Failed to upload part" }, { status: 500 })
  }
}
