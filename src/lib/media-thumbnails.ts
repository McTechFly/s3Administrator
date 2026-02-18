import { prisma } from "@/lib/db"
import type { Prisma } from "@prisma/client"
import { buildThumbnailObjectKey, copyThumbnailInBucket, deleteThumbnailsFromBucket } from "@/lib/thumbnail-storage"
import { getS3Client } from "@/lib/s3"
import { buildTaskDedupeKey, createTaskExecutionPlan } from "@/lib/task-plans"

export interface ThumbnailTaskPayload {
  bucket: string
  key: string
  credentialId: string
}

function thumbnailTaskTitle(key: string) {
  const name = key.split("/").pop() || key
  return `Generate thumbnail: ${name}`
}

export async function queueThumbnailTasks(userId: string, tasks: ThumbnailTaskPayload[]) {
  const deduped = new Map<string, ThumbnailTaskPayload>()
  for (const task of tasks) {
    const dedupeKey = `${task.credentialId}::${task.bucket}::${task.key}`
    deduped.set(dedupeKey, task)
  }

  const payloads = Array.from(deduped.values())
  if (payloads.length === 0) return 0

  const candidates = payloads.map((task) => {
    const payload = {
      bucket: task.bucket,
      key: task.key,
      credentialId: task.credentialId,
    }

    return {
      task,
      payload,
      dedupeKey: buildTaskDedupeKey("thumbnail_generate", payload),
    }
  })

  const existing = await prisma.backgroundTask.findMany({
    where: {
      userId,
      type: "thumbnail_generate",
      lifecycleState: {
        in: ["active", "paused"],
      },
      status: {
        in: ["pending", "in_progress"],
      },
      dedupeKey: {
        in: candidates.map((candidate) => candidate.dedupeKey),
      },
    },
    select: {
      dedupeKey: true,
    },
  })

  const existingKeys = new Set(
    existing.map((row) => row.dedupeKey).filter((value): value is string => Boolean(value))
  )
  const queueable = candidates.filter((candidate) => !existingKeys.has(candidate.dedupeKey))
  if (queueable.length === 0) return 0

  await prisma.backgroundTask.createMany({
    data: queueable.map(({ task, payload, dedupeKey }) => ({
      userId,
      type: "thumbnail_generate",
      title: thumbnailTaskTitle(task.key),
      status: "pending",
      lifecycleState: "active",
      payload: payload as Prisma.InputJsonObject,
      executionPlan: createTaskExecutionPlan("thumbnail_generate", payload),
      dedupeKey,
      isRecurring: false,
      scheduleIntervalSeconds: null,
      nextRunAt: new Date(),
    })),
  })

  return queueable.length
}

export async function deleteMediaThumbnailsForKeys(params: {
  userId: string
  credentialId: string
  bucket: string
  keys: string[]
}) {
  if (params.keys.length === 0) {
    return { deletedRows: 0, deletedObjects: 0 }
  }

  const rows = await prisma.mediaThumbnail.findMany({
    where: {
      userId: params.userId,
      credentialId: params.credentialId,
      bucket: params.bucket,
      key: { in: params.keys },
    },
    select: {
      id: true,
      thumbnailKey: true,
    },
  })

  if (rows.length === 0) {
    return { deletedRows: 0, deletedObjects: 0 }
  }

  const thumbnailKeys = Array.from(
    new Set(rows.map((row) => row.thumbnailKey).filter((value): value is string => Boolean(value)))
  )
  let deletedObjects = 0
  if (thumbnailKeys.length > 0) {
    try {
      const { client } = await getS3Client(params.userId, params.credentialId)
      await deleteThumbnailsFromBucket({ client, bucket: params.bucket, keys: thumbnailKeys })
      deletedObjects = thumbnailKeys.length
    } catch {
      deletedObjects = 0
    }
  }

  await prisma.mediaThumbnail.deleteMany({
    where: {
      id: { in: rows.map((row) => row.id) },
    },
  })

  return { deletedRows: rows.length, deletedObjects }
}

export async function purgeMediaThumbnailsForUser(params: { userId: string }) {
  const rows = await prisma.mediaThumbnail.findMany({
    where: {
      userId: params.userId,
    },
    select: {
      id: true,
      credentialId: true,
      bucket: true,
      thumbnailKey: true,
    },
  })

  // Group thumbnail keys by (credentialId, bucket) so we use the right S3 client per group
  const groups = new Map<string, { credentialId: string; bucket: string; thumbnailKeys: string[] }>()
  for (const row of rows) {
    if (!row.thumbnailKey) continue
    const groupKey = `${row.credentialId}::${row.bucket}`
    const group = groups.get(groupKey)
    if (group) {
      group.thumbnailKeys.push(row.thumbnailKey)
    } else {
      groups.set(groupKey, {
        credentialId: row.credentialId,
        bucket: row.bucket,
        thumbnailKeys: [row.thumbnailKey],
      })
    }
  }

  let deletedObjects = 0
  for (const group of groups.values()) {
    const uniqueKeys = Array.from(new Set(group.thumbnailKeys))
    if (uniqueKeys.length === 0) continue
    try {
      const { client } = await getS3Client(params.userId, group.credentialId)
      await deleteThumbnailsFromBucket({ client, bucket: group.bucket, keys: uniqueKeys })
      deletedObjects += uniqueKeys.length
    } catch {
      // Continue with other groups even if one fails
    }
  }

  const deletedRows = await prisma.mediaThumbnail.deleteMany({
    where: {
      userId: params.userId,
    },
  })

  const deletedTasks = await prisma.backgroundTask.deleteMany({
    where: {
      userId: params.userId,
      type: "thumbnail_generate",
      status: "pending",
    },
  })

  return {
    deletedRows: deletedRows.count,
    deletedObjects,
    deletedTasks: deletedTasks.count,
  }
}

export async function moveMediaThumbnailForObject(params: {
  userId: string
  credentialId: string
  fromBucket: string
  fromKey: string
  toBucket: string
  toKey: string
  sourceLastModified: Date
  sourceSize: bigint
}) {
  const existing = await prisma.mediaThumbnail.findUnique({
    where: {
      userId_credentialId_bucket_key: {
        userId: params.userId,
        credentialId: params.credentialId,
        bucket: params.fromBucket,
        key: params.fromKey,
      },
    },
  })

  if (!existing) return { moved: false, queued: false }

  const sourceLastModified = existing.sourceLastModified ?? params.sourceLastModified
  const sourceSize = existing.sourceSize ?? params.sourceSize

  const nextThumbnailKey = buildThumbnailObjectKey({
    bucket: params.toBucket,
    key: params.toKey,
    sourceLastModified,
    sourceSize,
  })

  const thumbnailBucket = params.toBucket

  const updateBase = {
    bucket: params.toBucket,
    key: params.toKey,
    sourceLastModified,
    sourceSize,
    thumbnailBucket,
  }

  if (!existing.thumbnailKey) {
    await prisma.mediaThumbnail.update({
      where: { id: existing.id },
      data: {
        ...updateBase,
        status: "pending",
        thumbnailKey: nextThumbnailKey,
        lastError: "Thumbnail missing; requeued after move",
      },
    })
    await queueThumbnailTasks(params.userId, [
      {
        credentialId: params.credentialId,
        bucket: params.toBucket,
        key: params.toKey,
      },
    ])
    return { moved: false, queued: true }
  }

  // Cross-bucket moves require regeneration since thumbnails live in the source bucket
  const sameBucket = params.fromBucket === params.toBucket
  if (!sameBucket) {
    // Delete old thumbnail from source bucket, queue regeneration in destination bucket
    try {
      const { client } = await getS3Client(params.userId, params.credentialId)
      await deleteThumbnailsFromBucket({ client, bucket: params.fromBucket, keys: [existing.thumbnailKey] })
    } catch {
      // Best effort deletion
    }

    await prisma.mediaThumbnail.update({
      where: { id: existing.id },
      data: {
        ...updateBase,
        status: "pending",
        thumbnailKey: nextThumbnailKey,
        lastError: "Thumbnail moved to different bucket; requeued",
      },
    })

    await queueThumbnailTasks(params.userId, [
      {
        credentialId: params.credentialId,
        bucket: params.toBucket,
        key: params.toKey,
      },
    ])
    return { moved: false, queued: true }
  }

  try {
    const { client } = await getS3Client(params.userId, params.credentialId)
    await copyThumbnailInBucket({ client, bucket: params.toBucket, oldKey: existing.thumbnailKey, newKey: nextThumbnailKey })
    await deleteThumbnailsFromBucket({ client, bucket: params.toBucket, keys: [existing.thumbnailKey] })

    await prisma.mediaThumbnail.update({
      where: { id: existing.id },
      data: {
        ...updateBase,
        status: "ready",
        thumbnailKey: nextThumbnailKey,
        mimeType: existing.mimeType ?? "image/webp",
        lastError: null,
      },
    })

    return { moved: true, queued: false }
  } catch (error) {
    await prisma.mediaThumbnail.update({
      where: { id: existing.id },
      data: {
        ...updateBase,
        status: "pending",
        thumbnailKey: nextThumbnailKey,
        lastError: error instanceof Error ? error.message : "thumbnail_move_failed",
      },
    })

    await queueThumbnailTasks(params.userId, [
      {
        credentialId: params.credentialId,
        bucket: params.toBucket,
        key: params.toKey,
      },
    ])
    return { moved: false, queued: true }
  }
}
