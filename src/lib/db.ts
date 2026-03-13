import { Prisma, PrismaClient } from "@prisma/client"
import { PrismaPg } from "@prisma/adapter-pg"
import { logSystemEvent } from "@/lib/system-logger"

const globalForPrisma = globalThis as unknown as {
  prisma: PrismaClient | undefined
}

function getDbPoolMax(): number {
  const raw = process.env.DATABASE_POOL_MAX
  if (!raw) return 20
  const parsed = Number.parseInt(raw, 10)
  if (!Number.isFinite(parsed)) return 20
  return Math.min(200, Math.max(1, parsed))
}

function createPrismaClient() {
  const adapter = new PrismaPg({
    connectionString: process.env.DATABASE_URL!,
    max: getDbPoolMax(),
  })
  const client = new PrismaClient({
    adapter,
    log: [
      { emit: "event", level: "warn" },
      { emit: "event", level: "error" },
    ],
  })

  client.$on("warn", (event: Prisma.LogEvent) => {
    void logSystemEvent({
      source: "db",
      level: "warn",
      message: event.message,
      metadata: {
        target: event.target ?? null,
        timestamp: event.timestamp?.toISOString?.() ?? null,
      },
    })
  })

  client.$on("error", (event: Prisma.LogEvent) => {
    void logSystemEvent({
      source: "db",
      level: "error",
      message: event.message,
      metadata: {
        target: event.target ?? null,
        timestamp: event.timestamp?.toISOString?.() ?? null,
      },
    })
  })

  return client
}

export const prisma = globalForPrisma.prisma ?? createPrismaClient()

if (process.env.NODE_ENV !== "production") globalForPrisma.prisma = prisma
