import { NextRequest, NextResponse } from "next/server"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { rateLimitByUser, rateLimitResponse } from "@/lib/rate-limit"
import { getRequestContext, logUserAuditAction } from "@/lib/audit-logger"
import { bucketManageSchema } from "@/lib/validations"

const BATCH_SIZE = 10_000

export async function POST(request: NextRequest) {
  let userId: string | undefined
  let auditBucket = ""
  const requestContext = getRequestContext(request)

  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }
    userId = session.user.id

    const rl = rateLimitByUser(userId, "export-paths", 10)
    if (!rl.success) return rateLimitResponse(rl.retryAfterSeconds)

    const body = await request.json()
    const parsed = bucketManageSchema.safeParse(body)
    if (!parsed.success) {
      return NextResponse.json({ error: "Invalid parameters" }, { status: 400 })
    }

    const { bucket, credentialId: rawCredentialId } = parsed.data
    auditBucket = bucket

    let credentialId = rawCredentialId
    if (!credentialId) {
      const defaultCred = await prisma.s3Credential.findFirst({
        where: { userId, isDefault: true },
        select: { id: true },
      })
      if (!defaultCred) {
        return NextResponse.json({ error: "No credentials configured" }, { status: 404 })
      }
      credentialId = defaultCred.id
    } else {
      const credential = await prisma.s3Credential.findFirst({
        where: { id: credentialId, userId },
        select: { id: true },
      })
      if (!credential) {
        return NextResponse.json({ error: "Credential not found" }, { status: 404 })
      }
    }

    const encoder = new TextEncoder()
    const stream = new ReadableStream({
      async start(controller) {
        try {
          let cursor: string | undefined
          let hasMore = true

          while (hasMore) {
            const rows = await prisma.fileMetadata.findMany({
              where: { credentialId, bucket, userId, isFolder: false },
              select: { id: true, key: true },
              orderBy: { id: "asc" },
              take: BATCH_SIZE,
              ...(cursor ? { cursor: { id: cursor }, skip: 1 } : {}),
            })

            if (rows.length > 0) {
              const chunk = rows.map((r) => r.key).join("\n") + "\n"
              controller.enqueue(encoder.encode(chunk))
              cursor = rows[rows.length - 1].id
            }

            if (rows.length < BATCH_SIZE) {
              hasMore = false
            }
          }

          controller.close()
        } catch {
          controller.error(new Error("Failed to read file metadata"))
        }
      },
    })

    await logUserAuditAction({
      userId,
      eventType: "s3_action",
      eventName: "export_paths",
      path: "/api/s3/export-paths",
      method: "POST",
      metadata: {
        bucket,
        credentialId,
      },
      ...requestContext,
    })

    const safeFilename = bucket.replace(/[^a-zA-Z0-9._-]/g, "_")

    return new Response(stream, {
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Content-Disposition": `attachment; filename="${safeFilename}-paths.txt"`,
      },
    })
  } catch (error) {
    console.error("Failed to export paths:", error)
    if (userId) {
      await logUserAuditAction({
        userId,
        eventType: "s3_action",
        eventName: "export_paths_failed",
        path: "/api/s3/export-paths",
        method: "POST",
        metadata: {
          bucket: auditBucket || null,
          error: error instanceof Error ? error.message : "export_paths_failed",
        },
        ...requestContext,
      })
    }
    return NextResponse.json({ error: "Failed to export paths" }, { status: 500 })
  }
}
