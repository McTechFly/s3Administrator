import { NextResponse } from "next/server"
import { demoGuard, getDemoS3Client } from "@/lib/demo"

export async function GET() {
  const guard = demoGuard()
  if (guard) return guard

  const { credential } = getDemoS3Client()

  return NextResponse.json({
    id: credential.id,
    label: credential.label,
    provider: credential.provider,
    endpoint: credential.endpoint,
    region: credential.region,
    isDefault: true,
  })
}
