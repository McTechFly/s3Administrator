import { prisma } from "@/lib/db"
import { getBackupConfig } from "@/lib/backup"
import { resolveTaskSchedule, nextRunAtForTaskSchedule } from "@/lib/task-schedule"

const DEDUPE_KEY = "database_backup_global"

export async function ensureBackupTaskScheduled(): Promise<void> {
  const config = getBackupConfig()
  if (!config) return // backup not configured

  // Find the first admin user to use as the task owner
  const adminUser = await prisma.user.findFirst({
    where: { role: "admin" },
    select: { id: true },
  })
  if (!adminUser) return // no admin yet — will be scheduled on next boot after admin is created

  const existing = await prisma.backgroundTask.findFirst({
    where: {
      dedupeKey: DEDUPE_KEY,
      lifecycleState: "active",
    },
    select: { id: true, scheduleCron: true },
  })

  const resolved = resolveTaskSchedule({
    isRecurring: true,
    scheduleCron: config.scheduleCron,
  })
  const nextRunAt = nextRunAtForTaskSchedule(resolved, new Date()) ?? new Date()

  if (existing) {
    // Update cron if it changed
    if (existing.scheduleCron !== config.scheduleCron) {
      await prisma.backgroundTask.update({
        where: { id: existing.id },
        data: { scheduleCron: config.scheduleCron, nextRunAt },
      })
    }
    return
  }

  await prisma.backgroundTask.create({
    data: {
      userId: adminUser.id,
      type: "database_backup",
      title: "Scheduled database backup",
      status: "pending",
      lifecycleState: "active",
      payload: {},
      isRecurring: true,
      scheduleCron: config.scheduleCron,
      dedupeKey: DEDUPE_KEY,
      nextRunAt,
    },
  })
}
