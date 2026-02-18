import { NextRequest, NextResponse } from "next/server"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { IMAGE_EXTENSIONS, VIDEO_EXTENSIONS } from "@/lib/media"
import { rateLimitByUser, rateLimitResponse } from "@/lib/rate-limit"
import { buildThumbnailObjectKey, isThumbnailGenerationEnabled } from "@/lib/thumbnail-storage"
import { queueThumbnailTasks } from "@/lib/media-thumbnails"

const THUMBNAIL_SUPPORTED_EXTENSIONS = [
  ...IMAGE_EXTENSIONS.filter((ext) => ext !== "svg"),
  ...VIDEO_EXTENSIONS,
]

const CHUNK_SIZE = 500

export async function POST(request: NextRequest) {
  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const userId = session.user.id

    const limitResult = rateLimitByUser(userId, "s3-thumbnail-generate-all", 2, 60_000)
    if (!limitResult.success) {
      return rateLimitResponse(limitResult.retryAfterSeconds)
    }

    if (!isThumbnailGenerationEnabled()) {
      return NextResponse.json({
        queued: 0,
        skipped: 0,
        total: 0,
        disabled: true,
      })
    }

    const mediaFiles = await prisma.fileMetadata.findMany({
      where: {
        userId,
        isFolder: false,
        extension: { in: [...THUMBNAIL_SUPPORTED_EXTENSIONS] },
      },
      select: {
        credentialId: true,
        bucket: true,
        key: true,
        lastModified: true,
        size: true,
      },
    })

    if (mediaFiles.length === 0) {
      return NextResponse.json({ queued: 0, skipped: 0, total: 0 })
    }

    let totalQueued = 0

    for (let offset = 0; offset < mediaFiles.length; offset += CHUNK_SIZE) {
      const chunk = mediaFiles.slice(offset, offset + CHUNK_SIZE)

      const existingRows = await prisma.mediaThumbnail.findMany({
        where: {
          userId,
          key: { in: chunk.map((f) => f.key) },
        },
        select: {
          credentialId: true,
          bucket: true,
          key: true,
          status: true,
          sourceLastModified: true,
          sourceSize: true,
        },
      })

      const existingByCompoundKey = new Map(
        existingRows.map((row) => [`${row.credentialId}::${row.bucket}::${row.key}`, row])
      )

      const acceptedForQueue: Array<{ bucket: string; key: string; credentialId: string }> = []

      for (const file of chunk) {
        const compoundKey = `${file.credentialId}::${file.bucket}::${file.key}`
        const existing = existingByCompoundKey.get(compoundKey)

        const isUpToDate =
          existing &&
          existing.status === "ready" &&
          existing.sourceLastModified?.getTime() === file.lastModified.getTime() &&
          existing.sourceSize?.toString() === file.size.toString()

        if (isUpToDate) {
          continue
        }

        const thumbnailKey = buildThumbnailObjectKey({
          bucket: file.bucket,
          key: file.key,
          sourceLastModified: file.lastModified,
          sourceSize: file.size,
        })

        await prisma.mediaThumbnail.upsert({
          where: {
            userId_credentialId_bucket_key: {
              userId,
              credentialId: file.credentialId,
              bucket: file.bucket,
              key: file.key,
            },
          },
          create: {
            userId,
            credentialId: file.credentialId,
            bucket: file.bucket,
            key: file.key,
            status: "pending",
            thumbnailBucket: file.bucket,
            thumbnailKey,
            mimeType: "image/webp",
            sourceLastModified: file.lastModified,
            sourceSize: file.size,
            lastError: null,
          },
          update: {
            status: "pending",
            thumbnailBucket: file.bucket,
            thumbnailKey,
            mimeType: "image/webp",
            sourceLastModified: file.lastModified,
            sourceSize: file.size,
            lastError: null,
          },
        })

        acceptedForQueue.push({
          bucket: file.bucket,
          key: file.key,
          credentialId: file.credentialId,
        })
      }

      if (acceptedForQueue.length > 0) {
        totalQueued += await queueThumbnailTasks(userId, acceptedForQueue)
      }
    }

    return NextResponse.json({
      queued: totalQueued,
      skipped: mediaFiles.length - totalQueued,
      total: mediaFiles.length,
    })
  } catch (error) {
    console.error("Failed to bulk generate thumbnails:", error)
    return NextResponse.json({ error: "Failed to bulk generate thumbnails" }, { status: 500 })
  }
}
