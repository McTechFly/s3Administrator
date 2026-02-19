import { S3Client } from "@aws-sdk/client-s3"
import { prisma } from "@/lib/db"
import { decrypt } from "@/lib/crypto"
import { quietAwsLogger } from "@/lib/aws-logger"

const LOCAL_HOSTNAMES = new Set(["localhost", "127.0.0.1", "0.0.0.0", "::1"])

function hasProtocol(value: string): boolean {
  return /^https?:\/\//i.test(value)
}

export function normalizeS3Endpoint(endpoint: string): string {
  const trimmed = endpoint.trim()
  if (!trimmed) {
    throw new Error("Endpoint is required")
  }

  let withProtocol = trimmed
  if (!hasProtocol(withProtocol)) {
    const hostPort = withProtocol.split("/")[0] || withProtocol
    const hostname = hostPort.split(":")[0]?.toLowerCase() || ""
    const protocol = LOCAL_HOSTNAMES.has(hostname) ? "http" : "https"
    withProtocol = `${protocol}://${withProtocol}`
  }

  let parsed: URL
  try {
    parsed = new URL(withProtocol)
  } catch {
    throw new Error("Endpoint must be a valid URL")
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    throw new Error("Endpoint protocol must be http or https")
  }

  return parsed.toString().replace(/\/+$/, "")
}

/**
 * Providers whose S3-compatible API uses a fixed signing region
 * regardless of the regional endpoint URL selected by the user.
 * The region the user chose is still stored and used for endpoint
 * construction, but the SDK signing region must be overridden.
 */
const FIXED_SIGNING_REGION_PROVIDERS: Record<string, string> = {
  STORADERA: "us-east-1",
}

export function normalizeS3Region(provider: string, region: string | null | undefined): string {
  const normalizedRegion = region?.trim() ?? ""
  if (normalizedRegion) return normalizedRegion

  const normalizedProvider = provider.trim().toUpperCase()
  if (normalizedProvider === "MINIO" || normalizedProvider === "GENERIC") {
    return "us-east-1"
  }

  throw new Error("Region is required for this provider")
}

/**
 * Return the signing region the SDK should use.
 * Some S3-compatible providers ignore the region in SigV4 and always
 * expect a fixed value (typically "us-east-1").
 */
export function getSigningRegion(provider: string, region: string): string {
  const override = FIXED_SIGNING_REGION_PROVIDERS[provider.trim().toUpperCase()]
  return override ?? region
}

export function createS3ClientFromConfig(config: {
  endpoint: string
  region: string | null | undefined
  provider: string
  accessKeyId: string
  secretAccessKey: string
}): {
  client: S3Client
  endpoint: string
  region: string
} {
  const endpoint = normalizeS3Endpoint(config.endpoint)
  const region = normalizeS3Region(config.provider, config.region)
  const signingRegion = getSigningRegion(config.provider, region)
  const client = new S3Client({
    endpoint,
    region: signingRegion,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
    forcePathStyle: true,
    // S3-compatible providers may reject optional checksum features on presigned GET URLs.
    // Keep checksum behavior to required-only for broad compatibility.
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
    logger: quietAwsLogger,
  })

  return { client, endpoint, region }
}

export async function getS3Client(
  userId: string,
  credentialId?: string
): Promise<{
  client: S3Client
  credential: {
    id: string
    endpoint: string
    region: string
    provider: string
    label: string
  }
}> {
  const credential = await prisma.s3Credential.findFirst({
    where: credentialId
      ? { id: credentialId, userId }
      : { userId, isDefault: true },
  })

  if (!credential) {
    throw new Error("No S3 credentials configured")
  }

  const accessKey = decrypt(credential.accessKeyEnc, credential.ivAccessKey)
  const secretKey = decrypt(credential.secretKeyEnc, credential.ivSecretKey)
  const { client, endpoint, region } = createS3ClientFromConfig({
    endpoint: credential.endpoint,
    region: credential.region,
    provider: credential.provider,
    accessKeyId: accessKey,
    secretAccessKey: secretKey,
  })

  return {
    client,
    credential: {
      id: credential.id,
      endpoint,
      region,
      provider: credential.provider,
      label: credential.label,
    },
  }
}
