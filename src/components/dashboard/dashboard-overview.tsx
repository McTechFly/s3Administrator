"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { RefreshCw, RotateCcw, Trash2 } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Skeleton } from "@/components/ui/skeleton"
import { formatSize, formatDate } from "@/lib/format"

interface OverviewSummary {
  indexedBucketCount: number
  indexedFileCount: number
  indexedTotalSize: number
  distinctExtensionCount: number
  lastIndexedAt: string | null
  multipartIncomplete: {
    uploads: number
    parts: number
    totalSize: number
    scannedBuckets: number
    failedBuckets: number
  }
  thumbnails: {
    ready: number
    pending: number
    failed: number
    total: number
  }
}

interface OverviewBucket {
  bucket: string
  credentialId: string
  credentialLabel: string
  fileCount: number
  totalSize: number
}

interface OverviewExtension {
  extension: string
  fileCount: number
  totalSize: number
  type: string
}

interface OverviewType {
  type: string
  fileCount: number
  totalSize: number
  multipartIncompleteUploads: number
  multipartIncompleteParts: number
  multipartIncompleteSize: number
}

interface OverviewResponse {
  summary: OverviewSummary
  buckets: OverviewBucket[]
  extensions: OverviewExtension[]
  types: OverviewType[]
}

interface BucketRef {
  name: string
  credentialId: string
}

interface RefreshProgress {
  current: number
  total: number
  bucketName: string
}

interface ThumbnailTaskRow {
  id: string
  type: string
  title: string
  status: "pending" | "in_progress" | "completed" | "failed"
  lifecycleState: "active" | "paused"
  lastError: string | null
  updatedAt: string
}

type ThumbnailStatusFilter = "all" | "pending" | "in_progress" | "completed" | "failed"

function getStatusBadgeVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  if (status === "completed") return "default"
  if (status === "failed") return "destructive"
  if (status === "in_progress") return "secondary"
  return "outline"
}

function formatCount(value: number): string {
  return Number(value || 0).toLocaleString()
}

function titleCase(value: string): string {
  if (!value) return value
  return `${value[0].toUpperCase()}${value.slice(1)}`
}

function extensionLabel(extension: string): string {
  return extension ? `.${extension}` : "(no extension)"
}

export function DashboardOverview() {
  const router = useRouter()
  const queryClient = useQueryClient()

  const [isRefreshing, setIsRefreshing] = useState(false)
  const [refreshProgress, setRefreshProgress] = useState<RefreshProgress | null>(null)
  const [isGeneratingAll, setIsGeneratingAll] = useState(false)
  const [thumbnailDetailOpen, setThumbnailDetailOpen] = useState(false)
  const [thumbnailStatusFilter, setThumbnailStatusFilter] = useState<ThumbnailStatusFilter>("all")
  const [controllingTaskId, setControllingTaskId] = useState<string | null>(null)
  const isRefreshingRef = useRef(false)
  const autoRefreshStartedRef = useRef(false)

  const {
    data,
    isLoading,
    isError,
    refetch,
  } = useQuery<OverviewResponse>({
    queryKey: ["dashboard-overview"],
    queryFn: async () => {
      const res = await fetch("/api/s3/overview")
      if (!res.ok) {
        throw new Error("Failed to load overview")
      }
      return res.json() as Promise<OverviewResponse>
    },
  })

  const runLiveRefresh = useCallback(async (trigger: "auto" | "manual") => {
    if (isRefreshingRef.current) return
    isRefreshingRef.current = true
    setIsRefreshing(true)
    setRefreshProgress(null)

    let successCount = 0
    let failureCount = 0
    let syncedTotal = 0

    try {
      const bucketsRes = await fetch("/api/s3/buckets?all=true")
      if (!bucketsRes.ok) {
        throw new Error("Failed to load buckets")
      }

      const bucketData = (await bucketsRes.json()) as { buckets?: BucketRef[] }
      const buckets = bucketData.buckets ?? []

      if (buckets.length === 0) {
        toast.info("No buckets available for live refresh")
        return
      }

      for (let index = 0; index < buckets.length; index++) {
        const bucket = buckets[index]
        setRefreshProgress({
          current: index + 1,
          total: buckets.length,
          bucketName: bucket.name,
        })

        try {
          const syncRes = await fetch("/api/s3/sync", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              bucket: bucket.name,
              credentialId: bucket.credentialId,
            }),
          })

          const syncData = await syncRes.json().catch(() => ({}))
          if (syncRes.ok) {
            successCount++
            syncedTotal += Number(syncData?.synced ?? 0)
          } else {
            failureCount++
          }
        } catch {
          failureCount++
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to refresh metadata"
      toast.error(message)
      return
    } finally {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["dashboard-overview"] }),
        queryClient.invalidateQueries({ queryKey: ["bucket-stats"] }),
        queryClient.invalidateQueries({ queryKey: ["objects"] }),
      ])
      isRefreshingRef.current = false
      setIsRefreshing(false)
      setRefreshProgress(null)
    }

    if (failureCount === 0) {
      const prefix = trigger === "auto" ? "Auto refresh complete" : "Refresh complete"
      toast.success(`${prefix}: synced ${formatCount(syncedTotal)} file(s) across ${successCount} bucket(s)`)
      return
    }

    toast.error(
      `Refresh finished: ${successCount} bucket(s) succeeded, ${failureCount} failed, ${formatCount(syncedTotal)} file(s) synced`
    )
  }, [queryClient])

  const generateAllThumbnails = useCallback(async () => {
    if (isGeneratingAll) return
    setIsGeneratingAll(true)

    try {
      const res = await fetch("/api/s3/thumbnails/generate-all", {
        method: "POST",
      })

      const data = await res.json()
      if (!res.ok) {
        throw new Error(data?.error ?? "Failed to queue thumbnails")
      }

      if (data.disabled) {
        toast.error("Thumbnail generation is disabled")
        return
      }

      toast.success(`Queued ${Number(data.queued ?? 0).toLocaleString()} thumbnails (${Number(data.skipped ?? 0).toLocaleString()} skipped)`)
      await queryClient.invalidateQueries({ queryKey: ["dashboard-overview"] })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to queue thumbnails")
    } finally {
      setIsGeneratingAll(false)
    }
  }, [isGeneratingAll, queryClient])

  const thumbnailScope = thumbnailStatusFilter === "all"
    ? "all"
    : thumbnailStatusFilter === "completed" || thumbnailStatusFilter === "failed"
      ? "history"
      : "ongoing"

  const { data: thumbnailTasksData, refetch: refetchThumbnailTasks } = useQuery<{ tasks: ThumbnailTaskRow[] }>({
    queryKey: ["thumbnail-tasks", thumbnailScope],
    queryFn: async () => {
      const params = new URLSearchParams({
        type: "thumbnail_generate",
        scope: thumbnailScope,
        limit: "200",
      })
      const res = await fetch(`/api/tasks?${params}`)
      if (!res.ok) return { tasks: [] }
      return (await res.json()) as { tasks: ThumbnailTaskRow[] }
    },
    enabled: thumbnailDetailOpen,
    refetchInterval: thumbnailDetailOpen ? 10_000 : false,
  })

  const filteredThumbnailTasks = useMemo(() => {
    const tasks = thumbnailTasksData?.tasks ?? []
    if (thumbnailStatusFilter === "all") return tasks
    return tasks.filter((task) => task.status === thumbnailStatusFilter)
  }, [thumbnailTasksData?.tasks, thumbnailStatusFilter])

  async function handleThumbnailTaskControl(taskId: string, action: "restart" | "cancel") {
    setControllingTaskId(taskId)
    try {
      const res = await fetch(`/api/tasks/${taskId}`, {
        method: action === "cancel" ? "DELETE" : "PATCH",
        headers: { "Content-Type": "application/json" },
        ...(action === "restart" ? { body: JSON.stringify({ action: "restart" }) } : {}),
      })
      const responseData = await res.json()
      if (!res.ok) {
        throw new Error(responseData?.error ?? `Failed to ${action} task`)
      }
      toast.success(action === "restart" ? "Task restarted" : "Task removed")
      void refetchThumbnailTasks()
      await queryClient.invalidateQueries({ queryKey: ["dashboard-overview"] })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : `Failed to ${action} task`)
    } finally {
      setControllingTaskId(null)
    }
  }

  useEffect(() => {
    if (autoRefreshStartedRef.current) return
    autoRefreshStartedRef.current = true
    void runLiveRefresh("auto")
  }, [runLiveRefresh])

  if (isLoading) {
    return (
      <div className="space-y-6 p-6">
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          {Array.from({ length: 4 }).map((_, index) => (
            <Skeleton key={index} className="h-28 w-full" />
          ))}
        </div>
        <Skeleton className="h-56 w-full" />
        <Skeleton className="h-56 w-full" />
      </div>
    )
  }

  if (isError || !data) {
    return (
      <div className="flex h-full items-center justify-center p-8">
        <Card className="max-w-md">
          <CardHeader>
            <CardTitle>Overview unavailable</CardTitle>
            <CardDescription>
              Could not load metadata overview for your buckets.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={() => void refetch()}>
              Retry
            </Button>
          </CardContent>
        </Card>
      </div>
    )
  }

  const refreshPercent = refreshProgress && refreshProgress.total > 0
    ? Math.round((refreshProgress.current / refreshProgress.total) * 100)
    : 0

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-semibold">Overview</h1>
          <p className="text-sm text-muted-foreground">
            Indexed snapshot with live refresh across all buckets
          </p>
        </div>
        <Button
          variant="outline"
          onClick={() => void runLiveRefresh("manual")}
          disabled={isRefreshing}
        >
          <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
          Refresh Live Metadata
        </Button>
      </div>

      {refreshProgress && (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Live refresh in progress</CardTitle>
            <CardDescription>
              Syncing {refreshProgress.current}/{refreshProgress.total}: {refreshProgress.bucketName}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{ width: `${refreshPercent}%` }}
              />
            </div>
          </CardContent>
        </Card>
      )}

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Indexed Buckets</CardDescription>
            <CardTitle>{formatCount(data.summary.indexedBucketCount)}</CardTitle>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Indexed Files</CardDescription>
            <CardTitle>{formatCount(data.summary.indexedFileCount)}</CardTitle>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Total Indexed Size</CardDescription>
            <CardTitle>{formatSize(data.summary.indexedTotalSize)}</CardTitle>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Distinct Extensions</CardDescription>
            <CardTitle>{formatCount(data.summary.distinctExtensionCount)}</CardTitle>
          </CardHeader>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Incomplete Multipart Size</CardDescription>
            <CardTitle>{formatSize(data.summary.multipartIncomplete.totalSize)}</CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="text-xs text-muted-foreground">
              {formatCount(data.summary.multipartIncomplete.uploads)} uploads, {formatCount(data.summary.multipartIncomplete.parts)} parts
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              scanned {formatCount(data.summary.multipartIncomplete.scannedBuckets)} bucket(s)
              {data.summary.multipartIncomplete.failedBuckets > 0 && `, ${formatCount(data.summary.multipartIncomplete.failedBuckets)} failed`}
            </p>
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardDescription>Thumbnails Generated</CardDescription>
            <CardTitle>
              {formatCount(data.summary.thumbnails.ready)}
              {data.summary.thumbnails.total > 0 && (
                <span className="text-sm font-normal text-muted-foreground">
                  {" "}/ {formatCount(data.summary.thumbnails.total)}
                </span>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0">
            {(data.summary.thumbnails.pending > 0 || data.summary.thumbnails.failed > 0) && (
              <p className="text-xs text-muted-foreground">
                {[
                  data.summary.thumbnails.pending > 0 && `${formatCount(data.summary.thumbnails.pending)} pending`,
                  data.summary.thumbnails.failed > 0 && `${formatCount(data.summary.thumbnails.failed)} failed`,
                ].filter(Boolean).join(", ")}
              </p>
            )}
            <div className="mt-2 flex flex-wrap items-center gap-2">
              {data.summary.thumbnails.total > 0 && data.summary.thumbnails.ready < data.summary.thumbnails.total && (
                <Button
                  variant="outline"
                  size="sm"
                  className="h-7 text-xs"
                  disabled={isGeneratingAll}
                  onClick={() => void generateAllThumbnails()}
                >
                  {isGeneratingAll ? "Queuing..." : "Generate All"}
                </Button>
              )}
              <Button
                variant="ghost"
                size="sm"
                className="h-7 text-xs"
                onClick={() => setThumbnailDetailOpen(true)}
              >
                View Details
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Index status</CardTitle>
          <CardDescription>
            Last indexed object timestamp: {formatDate(data.summary.lastIndexedAt, "Never")}
          </CardDescription>
        </CardHeader>
        {data.summary.indexedFileCount === 0 && (
          <CardContent>
            <p className="text-sm text-muted-foreground">
              No indexed metadata yet. Use live refresh to build this overview.
            </p>
          </CardContent>
        )}
      </Card>

      <div className="grid gap-6 xl:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">By Type</CardTitle>
            <CardDescription>Grouped using existing file type categories</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {data.types.map((entry) => (
                <div key={entry.type} className="flex items-center justify-between rounded-md border px-3 py-2">
                  <div>
                    <p className="text-sm font-medium">{titleCase(entry.type)}</p>
                    <p className="text-xs text-muted-foreground">
                      {formatCount(entry.fileCount)} files
                    </p>
                    <p className="text-xs text-muted-foreground">
                      {formatCount(entry.multipartIncompleteUploads)} incomplete upload(s), {formatCount(entry.multipartIncompleteParts)} part(s)
                    </p>
                  </div>
                  <div className="text-right">
                    <p className="text-sm">{formatSize(entry.totalSize)}</p>
                    <p className="text-xs text-muted-foreground">
                      {formatSize(entry.multipartIncompleteSize)} incomplete
                    </p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base">By Extension</CardTitle>
            <CardDescription>Extension-level distribution from indexed metadata</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="max-h-[360px] overflow-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Extension</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead className="text-right">Files</TableHead>
                    <TableHead className="text-right">Size</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.extensions.map((entry) => (
                    <TableRow key={entry.extension || "none"}>
                      <TableCell>{extensionLabel(entry.extension)}</TableCell>
                      <TableCell>{titleCase(entry.type)}</TableCell>
                      <TableCell className="text-right">{formatCount(entry.fileCount)}</TableCell>
                      <TableCell className="text-right">{formatSize(entry.totalSize)}</TableCell>
                    </TableRow>
                  ))}
                  {data.extensions.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={4} className="text-center text-muted-foreground">
                        No extension stats available yet
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">By Bucket</CardTitle>
          <CardDescription>Click a bucket row to open explorer view</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="max-h-[420px] overflow-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Bucket</TableHead>
                  <TableHead>Account</TableHead>
                  <TableHead className="text-right">Files</TableHead>
                  <TableHead className="text-right">Size</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.buckets.map((entry) => (
                  <TableRow
                    key={`${entry.credentialId}:${entry.bucket}`}
                    className="cursor-pointer"
                    onClick={() => {
                      const params = new URLSearchParams({
                        bucket: entry.bucket,
                        credentialId: entry.credentialId,
                      })
                      router.push(`/dashboard?${params}`)
                    }}
                  >
                    <TableCell>{entry.bucket}</TableCell>
                    <TableCell>{entry.credentialLabel}</TableCell>
                    <TableCell className="text-right">{formatCount(entry.fileCount)}</TableCell>
                    <TableCell className="text-right">{formatSize(entry.totalSize)}</TableCell>
                  </TableRow>
                ))}
                {data.buckets.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={4} className="text-center text-muted-foreground">
                      No indexed bucket metadata available yet
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Dialog open={thumbnailDetailOpen} onOpenChange={setThumbnailDetailOpen}>
        <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Thumbnail Generation Tasks</DialogTitle>
            <DialogDescription>
              {formatCount(data.summary.thumbnails.ready)} ready, {formatCount(data.summary.thumbnails.pending)} pending, {formatCount(data.summary.thumbnails.failed)} failed of {formatCount(data.summary.thumbnails.total)} media files
            </DialogDescription>
          </DialogHeader>

          <div className="flex items-center gap-2">
            <Select
              value={thumbnailStatusFilter}
              onValueChange={(value) => setThumbnailStatusFilter(value as ThumbnailStatusFilter)}
            >
              <SelectTrigger className="w-40">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All statuses</SelectItem>
                <SelectItem value="pending">Pending</SelectItem>
                <SelectItem value="in_progress">In progress</SelectItem>
                <SelectItem value="completed">Completed</SelectItem>
                <SelectItem value="failed">Failed</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="max-h-[60vh] space-y-2 overflow-y-auto">
            {filteredThumbnailTasks.length === 0 ? (
              <p className="py-4 text-center text-sm text-muted-foreground">
                No thumbnail tasks match this filter.
              </p>
            ) : (
              filteredThumbnailTasks.map((task) => (
                <div key={task.id} className="rounded-md border px-3 py-2">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-medium">{task.title}</p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        Updated {new Date(task.updatedAt).toLocaleString()}
                      </p>
                      {task.lastError && (
                        <p className="mt-0.5 truncate text-xs text-destructive">{task.lastError}</p>
                      )}
                    </div>
                    <div className="flex shrink-0 items-center gap-1.5">
                      <Badge
                        variant={getStatusBadgeVariant(task.status)}
                        className="h-5 px-1.5 text-[10px] capitalize"
                      >
                        {task.status.replace("_", " ")}
                      </Badge>
                      {task.status === "failed" && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 w-6 p-0"
                          disabled={controllingTaskId === task.id}
                          onClick={() => void handleThumbnailTaskControl(task.id, "restart")}
                          title="Restart"
                        >
                          <RotateCcw className="h-3 w-3" />
                        </Button>
                      )}
                      {(task.status === "completed" || task.status === "failed") && (
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-6 w-6 p-0 text-muted-foreground hover:text-destructive"
                          disabled={controllingTaskId === task.id}
                          onClick={() => void handleThumbnailTaskControl(task.id, "cancel")}
                          title="Remove"
                        >
                          <Trash2 className="h-3 w-3" />
                        </Button>
                      )}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
