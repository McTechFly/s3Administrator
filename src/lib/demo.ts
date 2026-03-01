import { S3Client } from "@aws-sdk/client-s3"
import { NextResponse } from "next/server"
import { createS3ClientFromConfig } from "@/lib/s3"

export const DEMO_WRITE_BLOCKED = "This action is not available in demo mode"

export function isDemoEnabled(): boolean {
  return Boolean(process.env.DEMO_S3_ACCESS_KEY?.trim())
}

export function getDemoS3Config() {
  const accessKeyId = process.env.DEMO_S3_ACCESS_KEY?.trim()
  const secretAccessKey = process.env.DEMO_S3_SECRET_KEY?.trim()
  const endpoint = process.env.DEMO_S3_ENDPOINT?.trim()
  const region = process.env.DEMO_S3_REGION?.trim() || "us-east-1"
  const provider = process.env.DEMO_S3_PROVIDER?.trim() || "GENERIC"
  const label = process.env.DEMO_S3_LABEL?.trim() || "Demo Storage"

  if (!accessKeyId || !secretAccessKey || !endpoint) {
    throw new Error(
      "Demo mode requires DEMO_S3_ACCESS_KEY, DEMO_S3_SECRET_KEY, and DEMO_S3_ENDPOINT"
    )
  }

  return { accessKeyId, secretAccessKey, endpoint, region, provider, label }
}

let _cachedClient: { client: S3Client; credential: DemoCredential } | null = null

interface DemoCredential {
  id: string
  endpoint: string
  region: string
  provider: string
  label: string
  isDefault: boolean
}

export function getDemoS3Client(): {
  client: S3Client
  credential: DemoCredential
} {
  if (_cachedClient) return _cachedClient

  const config = getDemoS3Config()
  const { client } = createS3ClientFromConfig({
    endpoint: config.endpoint,
    region: config.region,
    provider: config.provider,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
  })

  const credential: DemoCredential = {
    id: "demo",
    endpoint: config.endpoint,
    region: config.region,
    provider: config.provider,
    label: config.label,
    isDefault: true,
  }

  _cachedClient = { client, credential }
  return _cachedClient
}

export function demoGuard() {
  if (!isDemoEnabled()) {
    return NextResponse.json(
      { error: "Demo mode is not enabled" },
      { status: 404 }
    )
  }
  return null
}
