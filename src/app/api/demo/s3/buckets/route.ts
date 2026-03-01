import { NextResponse } from "next/server"
import { ListBucketsCommand } from "@aws-sdk/client-s3"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export async function GET() {
  const guard = demoGuard()
  if (guard) return guard

  try {
    const { client, credential } = getDemoS3Client()
    const response = await client.send(new ListBucketsCommand({}))

    const buckets = (response.Buckets ?? [])
      .map((bucket) => ({
        name: bucket.Name ?? "",
        creationDate: bucket.CreationDate?.toISOString() ?? null,
        credentialId: credential.id,
        credentialLabel: credential.label,
        provider: credential.provider,
      }))
      .filter((bucket) => bucket.name.length > 0)

    return NextResponse.json({ buckets })
  } catch (error) {
    console.error("Demo: Failed to list buckets:", error)
    return NextResponse.json({ error: "Failed to list buckets" }, { status: 500 })
  }
}
