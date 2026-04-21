import { NextRequest, NextResponse } from "next/server"
import { z } from "zod/v4"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { isValidPermissionLevel } from "@/lib/credential-resolver"

const createShareSchema = z.object({
  credentialId: z.string().min(1),
  targetEmail: z.string().email(),
  bucket: z.string().trim().min(1).max(255).optional().nullable(),
  permissionLevel: z.enum(["read", "read_write", "full"]).default("read"),
})

/**
 * GET /api/shares
 * Returns both shares I created (outgoing) and shares I received (incoming).
 */
export async function GET() {
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const userId = session.user.id
  const [incoming, outgoing] = await Promise.all([
    prisma.bucketShare.findMany({
      where: { targetUserId: userId },
      include: {
        credential: { select: { id: true, label: true, provider: true } },
        owner: { select: { id: true, email: true, name: true } },
      },
      orderBy: { createdAt: "desc" },
    }),
    prisma.bucketShare.findMany({
      where: { ownerUserId: userId },
      include: {
        credential: { select: { id: true, label: true, provider: true } },
        target: { select: { id: true, email: true, name: true } },
      },
      orderBy: { createdAt: "desc" },
    }),
  ])

  return NextResponse.json({ incoming, outgoing })
}

/**
 * POST /api/shares
 * Create a new share from the current user to another user (by email).
 * Body: { credentialId, targetEmail, bucket?, permissionLevel? }
 */
export async function POST(req: NextRequest) {
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const body = await req.json().catch(() => null)
  const parsed = createShareSchema.safeParse(body)
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid input", details: parsed.error.flatten() },
      { status: 400 },
    )
  }
  const { credentialId, targetEmail, bucket, permissionLevel } = parsed.data
  if (!isValidPermissionLevel(permissionLevel)) {
    return NextResponse.json({ error: "Invalid permissionLevel" }, { status: 400 })
  }

  const ownerId = session.user.id

  // Ownership check: only the credential owner can share it.
  const credential = await prisma.s3Credential.findUnique({
    where: { id: credentialId },
    select: { id: true, userId: true, label: true },
  })
  if (!credential) return NextResponse.json({ error: "Credential not found" }, { status: 404 })
  if (credential.userId !== ownerId) {
    return NextResponse.json({ error: "Forbidden" }, { status: 403 })
  }

  const target = await prisma.user.findUnique({
    where: { email: targetEmail.toLowerCase() },
    select: { id: true, email: true, name: true },
  })
  if (!target) return NextResponse.json({ error: "Target user not found" }, { status: 404 })
  if (target.id === ownerId) {
    return NextResponse.json({ error: "You cannot share with yourself" }, { status: 400 })
  }

  const normalizedBucket = bucket?.trim() ? bucket.trim() : null

  const existing = await prisma.bucketShare.findFirst({
    where: {
      credentialId,
      targetUserId: target.id,
      bucket: normalizedBucket,
    },
  })
  const share = existing
    ? await prisma.bucketShare.update({
        where: { id: existing.id },
        data: { permissionLevel },
      })
    : await prisma.bucketShare.create({
        data: {
          credentialId,
          ownerUserId: ownerId,
          targetUserId: target.id,
          bucket: normalizedBucket,
          permissionLevel,
        },
      })

  return NextResponse.json({ ok: true, share })
}
