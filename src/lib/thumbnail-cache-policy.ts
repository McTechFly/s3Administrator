import { purgeMediaThumbnailsForUser } from "@/lib/media-thumbnails"
import { isThumbnailGenerationEnabled } from "@/lib/thumbnail-storage"

export async function isThumbnailCacheEnabledForUser(_userId: string): Promise<boolean> {
  return isThumbnailGenerationEnabled()
}

export async function enforceThumbnailCachePolicyForUser(userId: string) {
  const enabled = isThumbnailGenerationEnabled()
  if (enabled) {
    return {
      enabled: true,
      purged: false,
      deletedRows: 0,
      deletedObjects: 0,
      deletedTasks: 0,
    }
  }

  const purged = await purgeMediaThumbnailsForUser({ userId })

  return {
    enabled: false,
    purged: true,
    ...purged,
  }
}
