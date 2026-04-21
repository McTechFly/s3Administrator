import { prisma } from "@/lib/db"

export type PermissionLevel = "read" | "read_write" | "full"

const PERMISSION_LEVELS: PermissionLevel[] = ["read", "read_write", "full"]

export function isValidPermissionLevel(v: unknown): v is PermissionLevel {
  return typeof v === "string" && PERMISSION_LEVELS.includes(v as PermissionLevel)
}

/**
 * A credential the current user can legitimately use: either owned, or
 * shared to them via a BucketShare row.
 */
export type ResolvedCredential = {
  id: string
  ownerUserId: string
  label: string
  provider: string
  endpoint: string
  region: string
  accessKeyEnc: string
  ivAccessKey: string
  secretKeyEnc: string
  ivSecretKey: string
  // null => full credential access (owner or credential-wide share)
  // non-null => restricted to this single bucket
  restrictedBucket: string | null
  permissionLevel: PermissionLevel
  isOwner: boolean
}

/**
 * Resolve (credentialId, bucketName?) for the given viewer, enforcing either
 * ownership or an active BucketShare. Returns null if the viewer has no right.
 *
 * When `bucket` is provided, bucket-scoped shares matching it also qualify.
 * When `bucket` is omitted, only ownership or credential-wide shares qualify.
 */
export async function resolveCredentialForUser(
  viewerUserId: string,
  credentialId: string,
  bucket?: string | null,
): Promise<ResolvedCredential | null> {
  const cred = await prisma.s3Credential.findUnique({
    where: { id: credentialId },
  })
  if (!cred) return null

  if (cred.userId === viewerUserId) {
    return {
      id: cred.id,
      ownerUserId: cred.userId,
      label: cred.label,
      provider: cred.provider,
      endpoint: cred.endpoint,
      region: cred.region,
      accessKeyEnc: cred.accessKeyEnc,
      ivAccessKey: cred.ivAccessKey,
      secretKeyEnc: cred.secretKeyEnc,
      ivSecretKey: cred.ivSecretKey,
      restrictedBucket: null,
      permissionLevel: "full",
      isOwner: true,
    }
  }

  const shares = await prisma.bucketShare.findMany({
    where: { credentialId: cred.id, targetUserId: viewerUserId },
  })
  if (shares.length === 0) return null

  // Prefer a bucket-scoped share that matches the requested bucket, else a
  // credential-wide share (bucket = null). If bucket is not specified, only
  // credential-wide shares qualify.
  const credentialWide = shares.find((s) => s.bucket === null)
  const bucketScoped = bucket ? shares.find((s) => s.bucket === bucket) : undefined

  const chosen = bucketScoped ?? credentialWide
  if (!chosen) return null

  const level = isValidPermissionLevel(chosen.permissionLevel)
    ? chosen.permissionLevel
    : "read"

  return {
    id: cred.id,
    ownerUserId: cred.userId,
    label: cred.label,
    provider: cred.provider,
    endpoint: cred.endpoint,
    region: cred.region,
    accessKeyEnc: cred.accessKeyEnc,
    ivAccessKey: cred.ivAccessKey,
    secretKeyEnc: cred.secretKeyEnc,
    ivSecretKey: cred.ivSecretKey,
    restrictedBucket: chosen.bucket,
    permissionLevel: level,
    isOwner: false,
  }
}

/**
 * List all credentials visible to the user: their own + credentials shared
 * with them. Each entry includes the access restriction (bucket scope) and
 * the owner's display info.
 */
export async function listVisibleCredentialsForUser(viewerUserId: string) {
  const [owned, shares] = await Promise.all([
    prisma.s3Credential.findMany({
      where: { userId: viewerUserId },
      select: {
        id: true,
        label: true,
        provider: true,
        endpoint: true,
        region: true,
        isDefault: true,
        createdAt: true,
      },
      orderBy: { createdAt: "desc" },
    }),
    prisma.bucketShare.findMany({
      where: { targetUserId: viewerUserId },
      include: {
        credential: {
          select: {
            id: true,
            label: true,
            provider: true,
            endpoint: true,
            region: true,
            createdAt: true,
          },
        },
        owner: { select: { id: true, email: true, name: true } },
      },
      orderBy: { createdAt: "desc" },
    }),
  ])

  return {
    owned,
    shared: shares.map((s) => ({
      shareId: s.id,
      credential: s.credential,
      owner: s.owner,
      bucket: s.bucket,
      permissionLevel: s.permissionLevel,
      createdAt: s.createdAt,
    })),
  }
}

export function canWrite(level: PermissionLevel): boolean {
  return level === "read_write" || level === "full"
}

export function canManage(level: PermissionLevel): boolean {
  return level === "full"
}
