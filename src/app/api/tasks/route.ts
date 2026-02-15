import { NextRequest, NextResponse } from "next/server"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { VIDEO_EXTENSIONS } from "@/lib/media"
import { getUpcomingRunDates, normalizeExecutionHistory } from "@/lib/task-plans"

type TaskStatus = "pending" | "in_progress" | "completed" | "failed"

export async function GET(request: NextRequest) {
  try {
    const session = await auth()
    if (!session?.user?.id) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 })
    }

    const scope = request.nextUrl.searchParams.get("scope") ?? "ongoing"
    const limitRaw = Number(request.nextUrl.searchParams.get("limit") ?? "8")
    const limit = Number.isFinite(limitRaw) ? Math.min(50, Math.max(1, Math.floor(limitRaw))) : 8

    const statuses: TaskStatus[] =
      scope === "history"
        ? ["completed", "failed"]
        : scope === "all"
          ? ["pending", "in_progress", "completed", "failed"]
          : ["pending", "in_progress", "failed"]

    const [tasks, cachedFiles, totalVideoFiles, readyThumbnails, pendingThumbnails, failedThumbnails] = await Promise.all([
      prisma.backgroundTask.findMany({
        where: {
          userId: session.user.id,
          status: {
            in: statuses,
          },
        },
        orderBy: [
          {
            updatedAt: "desc",
          },
        ],
        take: limit,
        select: {
          id: true,
          type: true,
          title: true,
          status: true,
          progress: true,
          attempts: true,
          maxAttempts: true,
          runCount: true,
          lastError: true,
          lifecycleState: true,
          isRecurring: true,
          scheduleIntervalSeconds: true,
          nextRunAt: true,
          lastRunAt: true,
          pausedAt: true,
          executionHistory: true,
          createdAt: true,
          updatedAt: true,
          completedAt: true,
        },
      }),
      prisma.fileMetadata.count({
        where: {
          userId: session.user.id,
          isFolder: false,
        },
      }),
      prisma.fileMetadata.count({
        where: {
          userId: session.user.id,
          isFolder: false,
          extension: {
            in: [...VIDEO_EXTENSIONS],
          },
        },
      }),
      prisma.mediaThumbnail.count({
        where: {
          userId: session.user.id,
          status: "ready",
        },
      }),
      prisma.mediaThumbnail.count({
        where: {
          userId: session.user.id,
          status: {
            in: ["pending", "processing"],
          },
        },
      }),
      prisma.mediaThumbnail.count({
        where: {
          userId: session.user.id,
          status: "failed",
        },
      }),
    ])

    const taskIds = tasks.map((task) => task.id)
    let latestRuns: Array<{
      taskId: string
      status: string
      startedAt: Date
      finishedAt: Date | null
    }> = []
    let runCounts: Array<{
      taskId: string
      status: string
      _count: { _all: number }
    }> = []

    if (taskIds.length > 0) {
      try {
        ;[latestRuns, runCounts] = await Promise.all([
          prisma.backgroundTaskRun.findMany({
            where: {
              taskId: {
                in: taskIds,
              },
            },
            orderBy: [
              { taskId: "asc" },
              { runNumber: "desc" },
            ],
            distinct: ["taskId"],
            select: {
              taskId: true,
              status: true,
              startedAt: true,
              finishedAt: true,
            },
          }),
          prisma.backgroundTaskRun.groupBy({
            by: ["taskId", "status"],
            where: {
              taskId: {
                in: taskIds,
              },
            },
            _count: {
              _all: true,
            },
          }),
        ])
      } catch (error) {
        const code =
          error && typeof error === "object" && "code" in error
            ? (error as { code?: string }).code
            : ""
        if (code !== "P2021") {
          throw error
        }
      }
    }

    const latestRunByTaskId = new Map(
      latestRuns.map((run) => [run.taskId, run])
    )
    const countsByTaskId = new Map<string, { successRuns: number; failedRuns: number }>()
    for (const row of runCounts) {
      const current = countsByTaskId.get(row.taskId) ?? { successRuns: 0, failedRuns: 0 }
      if (row.status === "succeeded") {
        current.successRuns += row._count._all
      } else if (row.status === "failed") {
        current.failedRuns += row._count._all
      }
      countsByTaskId.set(row.taskId, current)
    }

    const mappedTasks = tasks.map((task) => ({
      ...task,
      executionHistory: normalizeExecutionHistory(task.executionHistory),
      upcomingRuns:
        task.lifecycleState === "active" && task.isRecurring
          ? getUpcomingRunDates(task.nextRunAt, task.scheduleIntervalSeconds, 3)
          : [],
      lastRunStatus: latestRunByTaskId.get(task.id)?.status ?? null,
      lastRunDurationMs: (() => {
        const latest = latestRunByTaskId.get(task.id)
        if (!latest?.finishedAt) return null
        return Math.max(0, latest.finishedAt.getTime() - latest.startedAt.getTime())
      })(),
      successRuns: countsByTaskId.get(task.id)?.successRuns ?? 0,
      failedRuns: countsByTaskId.get(task.id)?.failedRuns ?? 0,
    }))

    return NextResponse.json({
      tasks: mappedTasks,
      summary: {
        cachedFiles,
        thumbnails: {
          ready: readyThumbnails,
          total: totalVideoFiles,
          pending: pendingThumbnails,
          failed: failedThumbnails,
        },
      },
    })
  } catch (error) {
    console.error("Failed to list tasks:", error)
    const message = error instanceof Error ? error.message : "Failed to list tasks"
    return NextResponse.json({ error: message }, { status: 500 })
  }
}
