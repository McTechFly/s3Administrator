import { prisma } from "@/lib/db"

const DEDUPE_KEY = "database_backup_global"

export async function ensureBackupTaskScheduled(): Promise<void> {
  const now = new Date()

  await prisma.backgroundTask.updateMany({
    where: {
      dedupeKey: DEDUPE_KEY,
      isRecurring: true,
      status: {
        not: "in_progress",
      },
    },
    data: {
      isRecurring: false,
      scheduleCron: null,
      scheduleIntervalSeconds: null,
      status: "completed",
      completedAt: now,
      nextRunAt: now,
      lastError: null,
    },
  })
}
