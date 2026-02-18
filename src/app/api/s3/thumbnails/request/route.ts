import { NextRequest, NextResponse } from "next/server"
import { HeadObjectCommand } from "@aws-sdk/client-s3"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { getS3Client } from "@/lib/s3"
import { isThumbnailSupportedExtension } from "@/lib/media"
import { thumbnailRequestSchema } from "@/lib/validations"
import { rateLimitByUser, rateLimitResponse } from "@/lib/rate-limit"
import {
  buildThumbnailObjectKey,
  doesThumbnailMetadataMatchSource,
  isThumbnailGenerationEnabled,
} from "@/lib/thumbnail-storage"
import { queueThumbnailTasks } from "@/lib/media-thumbnails"
import { getRequestContext, logUserAuditAction } from "@/lib/audit-logger"

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

    const limitResult = rateLimitByUser(session.user.id, "s3-thumbnail-request", 40, 60_000)
    if (!limitResult.success) {
      return rateLimitResponse(limitResult.retryAfterSeconds)
    }

    const body = await request.json()
    const parsed = thumbnailRequestSchema.safeParse(body)
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Invalid parameters", details: parsed.error.flatten() },
        { status: 400 }
      )
    }

    const { bucket, credentialId, keys } = parsed.data
    auditBucket = bucket
    const dedupedKeys = Array.from(new Set(keys))

    if (!isThumbnailGenerationEnabled()) {
      await logUserAuditAction({
        userId: session.user.id,
        eventType: "s3_action",
        eventName: "thumbnail_request_blocked",
        path: "/api/s3/thumbnails/request",
        method: "POST",
        target: bucket,
        metadata: {
          bucket,
          requested: dedupedKeys.length,
          reason: "thumbnail_generation_disabled",
        },
        ...requestContext,
      })

      return NextResponse.json({
        accepted: 0,
        queued: 0,
        skipped: dedupedKeys.length,
        disabled: true,
        reason: "thumbnail_generation_disabled",
      })
    }

    const { client, credential } = await getS3Client(session.user.id, credentialId)

    const files = await prisma.fileMetadata.findMany({
      where: {
        userId: session.user.id,
        credentialId: credential.id,
        bucket,
        isFolder: false,
        key: { in: dedupedKeys },
      },
      select: {
        key: true,
        extension: true,
        lastModified: true,
        size: true,
      },
    })

    const mediaFiles = files.filter((file) => isThumbnailSupportedExtension(file.extension))
    if (mediaFiles.length === 0) {
      return NextResponse.json({
        accepted: 0,
        queued: 0,
        skipped: dedupedKeys.length,
      })
    }

    const existingRows = await prisma.mediaThumbnail.findMany({
      where: {
        userId: session.user.id,
        credentialId: credential.id,
        bucket,
        key: { in: mediaFiles.map((file) => file.key) },
      },
      select: {
        key: true,
        status: true,
        sourceLastModified: true,
        sourceSize: true,
      },
    })
    const existingByKey = new Map(existingRows.map((row) => [row.key, row]))

    const acceptedForQueue: Array<{ bucket: string; key: string; credentialId: string }> = []

    for (const file of mediaFiles) {
      const existing = existingByKey.get(file.key)
      const sourceLastModified = file.lastModified
      const sourceSize = file.size
      const isUpToDate =
        existing &&
        existing.status === "ready" &&
        existing.sourceLastModified?.getTime() === sourceLastModified.getTime() &&
        existing.sourceSize?.toString() === sourceSize.toString()

      if (isUpToDate) {
        continue
      }

      const thumbnailKey = buildThumbnailObjectKey({
        bucket,
        key: file.key,
        sourceLastModified,
        sourceSize,
      })

      let reusableThumbnailExists = false
      try {
        const head = await client.send(
          new HeadObjectCommand({
            Bucket: bucket,
            Key: thumbnailKey,
          })
        )
        reusableThumbnailExists = doesThumbnailMetadataMatchSource(head.Metadata, {
          sourceLastModified,
          sourceSize,
        })
      } catch {
        reusableThumbnailExists = false
      }

      if (reusableThumbnailExists) {
        await prisma.mediaThumbnail.upsert({
          where: {
            userId_credentialId_bucket_key: {
              userId: session.user.id,
              credentialId: credential.id,
              bucket,
              key: file.key,
            },
          },
          create: {
            userId: session.user.id,
            credentialId: credential.id,
            bucket,
            key: file.key,
            status: "ready",
            thumbnailBucket: bucket,
            thumbnailKey,
            mimeType: "image/webp",
            sourceLastModified,
            sourceSize,
            lastError: null,
          },
          update: {
            status: "ready",
            thumbnailBucket: bucket,
            thumbnailKey,
            mimeType: "image/webp",
            sourceLastModified,
            sourceSize,
            lastError: null,
          },
        })
        continue
      }

      await prisma.mediaThumbnail.upsert({
        where: {
          userId_credentialId_bucket_key: {
            userId: session.user.id,
            credentialId: credential.id,
            bucket,
            key: file.key,
          },
        },
        create: {
          userId: session.user.id,
          credentialId: credential.id,
          bucket,
          key: file.key,
          status: "pending",
          thumbnailBucket: bucket,
          thumbnailKey,
          mimeType: "image/webp",
          sourceLastModified,
          sourceSize,
          lastError: null,
        },
        update: {
          status: "pending",
          thumbnailBucket: bucket,
          thumbnailKey,
          mimeType: "image/webp",
          sourceLastModified,
          sourceSize,
          lastError: null,
        },
      })

      acceptedForQueue.push({
        bucket,
        key: file.key,
        credentialId: credential.id,
      })
    }

    const queued = await queueThumbnailTasks(session.user.id, acceptedForQueue)

    await logUserAuditAction({
      userId: session.user.id,
      eventType: "s3_action",
      eventName: "thumbnail_request",
      path: "/api/s3/thumbnails/request",
      method: "POST",
      target: bucket,
      metadata: {
        bucket,
        credentialId: credential.id,
        requested: dedupedKeys.length,
        accepted: acceptedForQueue.length,
        queued,
      },
      ...requestContext,
    })

    return NextResponse.json({
      accepted: acceptedForQueue.length,
      queued,
      skipped: dedupedKeys.length - acceptedForQueue.length,
    })
  } catch (error) {
    console.error("Failed to enqueue thumbnail generation:", error)
    if (userId) {
      await logUserAuditAction({
        userId,
        eventType: "s3_action",
        eventName: "thumbnail_request_failed",
        path: "/api/s3/thumbnails/request",
        method: "POST",
        target: auditBucket || undefined,
        metadata: {
          error: error instanceof Error ? error.message : "thumbnail_request_failed",
        },
        ...requestContext,
      })
    }
    return NextResponse.json({ error: "Failed to enqueue thumbnail generation" }, { status: 500 })
  }
}
