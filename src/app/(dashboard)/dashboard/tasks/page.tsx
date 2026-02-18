"use client"

import { useEffect, useMemo, useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { toast } from "sonner"
import { ListTodo, Pause, Play, RotateCcw, Trash2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Input } from "@/components/ui/input"
import { FolderPickerDialog } from "@/components/dashboard/folder-picker-dialog"
import { DestructiveConfirmDialog } from "@/components/shared/destructive-confirm-dialog"
import {
  DESTRUCTIVE_CONFIRM_SCOPE,
  hasDestructiveConfirmBypass,
} from "@/lib/destructive-confirmation"

type TaskScope = "folder" | "bucket"
type TaskOperation = "sync" | "copy" | "move" | "migrate"

interface Credential {
  id: string
  label: string
}

interface BucketOption {
  name: string
  credentialId: string
}

interface TaskRow {
  id: string
  type: string
  title: string
  status: "pending" | "in_progress" | "completed" | "failed"
  progress: unknown
  lifecycleState: "active" | "paused"
  lastError: string | null
  runCount: number
  isRecurring: boolean
  scheduleCron?: string | null
  scheduleIntervalSeconds: number | null
  nextRunAt: string
  lastRunAt: string | null
  upcomingRuns: string[]
  lastRunStatus: string | null
  lastRunDurationMs: number | null
  successRuns: number
  failedRuns: number
  executionHistory: Array<{
    at: string
    status: "succeeded" | "failed" | "skipped" | "paused" | "resumed" | "restarted"
    message: string
  }>
  createdAt: string
  completedAt: string | null
  updatedAt: string
}

interface TransferTaskCreateBody {
  scope: TaskScope
  operation: TaskOperation
  sourceBucket: string
  sourceCredentialId: string
  sourcePrefix?: string
  destinationBucket: string
  destinationCredentialId: string
  destinationPrefix?: string
  schedule?: {
    cron: string
  } | null
  confirmDestructiveSchedule?: boolean
}

interface TransferTaskPreview {
  type: "object_transfer"
  summary: string[]
  commands: string[]
  estimatedObjects: number
  sampleObjects: string[]
  warnings: string[]
  detailedPlan: TransferTaskDetailedPreviewPlan
}

interface TransferTaskDetailedPreviewAction {
  phase: "transfer" | "sync_cleanup"
  operation: "copy" | "delete_source" | "delete_destination"
  command: string
  sourceKey: string | null
  destinationKey: string | null
}

interface TransferTaskDetailedPreviewCursor {
  phase: "source" | "cleanup"
  sourceKey: string | null
  cleanupKey: string | null
}

interface TransferTaskDetailedPreviewPlan {
  actions: TransferTaskDetailedPreviewAction[]
  hasMore: boolean
  nextCursor: TransferTaskDetailedPreviewCursor | null
  pageSize: number
  scannedSourceObjects: number
  scanLimitReached: boolean
}

function toSafeInt(value: unknown): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return 0
  return Math.max(0, Math.floor(value))
}

function getTransferResultSummary(progress: unknown): string | null {
  if (!progress || typeof progress !== "object") return null
  const candidate = progress as {
    total?: unknown
    processed?: unknown
    copied?: unknown
    moved?: unknown
    failed?: unknown
    skipped?: unknown
  }
  const processed = toSafeInt(candidate.processed)
  const copied = toSafeInt(candidate.copied)
  const moved = toSafeInt(candidate.moved)
  const failed = toSafeInt(candidate.failed)
  const skipped = toSafeInt(candidate.skipped)
  const total = toSafeInt(candidate.total)

  if (processed <= 0 && failed <= 0 && copied <= 0 && moved <= 0) return null

  const effectiveCopied = copied > 0 || moved > 0 ? copied + moved : Math.max(0, processed - failed - skipped)
  const parts = [
    `${effectiveCopied.toLocaleString()} copied`,
  ]
  if (skipped > 0) {
    parts.push(`${skipped.toLocaleString()} skipped`)
  }
  if (failed > 0) {
    parts.push(`${failed.toLocaleString()} failed`)
  }
  if (total > 0) {
    parts.push(`${total.toLocaleString()} total`)
  }
  return parts.join(" • ")
}

function getBulkDeleteResultSummary(progress: unknown): string | null {
  if (!progress || typeof progress !== "object") return null
  const candidate = progress as {
    total?: unknown
    deleted?: unknown
    remaining?: unknown
  }
  const total = toSafeInt(candidate.total)
  const deleted = toSafeInt(candidate.deleted)
  const remaining = toSafeInt(candidate.remaining)
  if (total <= 0 && deleted <= 0) return null
  if (remaining > 0) {
    return `${deleted.toLocaleString()} deleted • ${remaining.toLocaleString()} remaining`
  }
  return `${deleted.toLocaleString()} deleted`
}

function getTaskResultSummary(task: TaskRow): string | null {
  if (task.type === "object_transfer") {
    return getTransferResultSummary(task.progress)
  }
  if (task.type === "bulk_delete") {
    return getBulkDeleteResultSummary(task.progress)
  }
  return null
}

function canRetryFailed(task: TaskRow): boolean {
  if (task.status !== "failed") return false
  if (task.type !== "object_transfer") return false
  if (!task.progress || typeof task.progress !== "object") return false
  const failed = toSafeInt((task.progress as { failed?: unknown }).failed)
  return failed > 0
}

function getStatusVariant(status: TaskRow["status"]): "default" | "secondary" | "destructive" | "outline" {
  if (status === "completed") return "default"
  if (status === "failed") return "destructive"
  if (status === "in_progress") return "secondary"
  return "outline"
}

function getDisplayState(task: TaskRow): string {
  if (task.lifecycleState === "paused") return "paused"
  return task.status.replace("_", " ")
}

function getTaskScheduleLabel(task: TaskRow): string {
  if (!task.isRecurring) return "One-time"
  if (task.scheduleCron && task.scheduleCron.trim()) {
    return `CRON (${task.scheduleCron}) UTC`
  }
  if (task.scheduleIntervalSeconds && task.scheduleIntervalSeconds > 0) {
    return `Every ${task.scheduleIntervalSeconds}s`
  }
  return "Scheduled"
}

const FOLDER_OPERATIONS: Array<{ value: TaskOperation; label: string }> = [
  { value: "sync", label: "Sync" },
  { value: "copy", label: "One-time copy" },
  { value: "move", label: "One-time move" },
]

const BUCKET_OPERATIONS: Array<{ value: TaskOperation; label: string }> = [
  { value: "sync", label: "Sync" },
  { value: "copy", label: "One-time copy" },
  { value: "migrate", label: "Migrate" },
]

export default function TasksPage() {
  const queryClient = useQueryClient()
  const [scope, setScope] = useState<TaskScope>("folder")
  const [operation, setOperation] = useState<TaskOperation>("sync")
  const [sourceCredentialId, setSourceCredentialId] = useState("")
  const [destinationCredentialId, setDestinationCredentialId] = useState("")
  const [sourceBucket, setSourceBucket] = useState("")
  const [destinationBucket, setDestinationBucket] = useState("")
  const [sourcePrefix, setSourcePrefix] = useState("")
  const [destinationPrefix, setDestinationPrefix] = useState("")
  const [scheduleMode, setScheduleMode] = useState<"once" | "cron">("once")
  const [scheduleCron, setScheduleCron] = useState("0 * * * *")
  const [submitting, setSubmitting] = useState(false)
  const [controllingTaskId, setControllingTaskId] = useState<string | null>(null)
  const [transferPreviewOpen, setTransferPreviewOpen] = useState(false)
  const [transferPreview, setTransferPreview] = useState<TransferTaskPreview | null>(null)
  const [transferConfirmOpen, setTransferConfirmOpen] = useState(false)
  const [pendingTransferBody, setPendingTransferBody] = useState<TransferTaskCreateBody | null>(null)
  const [loadingMoreTransferPlan, setLoadingMoreTransferPlan] = useState(false)

  const { data: credentials = [] } = useQuery<Credential[]>({
    queryKey: ["credentials"],
    queryFn: async () => {
      const res = await fetch("/api/s3/credentials")
      if (!res.ok) return []
      return (await res.json()) as Credential[]
    },
  })

  const { data: buckets = [] } = useQuery<BucketOption[]>({
    queryKey: ["task-bucket-options", credentials.map((c) => c.id).sort().join(",")],
    enabled: credentials.length > 0,
    queryFn: async () => {
      const responses = await Promise.all(
        credentials.map(async (credential) => {
          const params = new URLSearchParams({ credentialId: credential.id })
          const res = await fetch(`/api/s3/buckets?${params}`)
          if (!res.ok) return [] as BucketOption[]
          const data = (await res.json()) as { buckets?: Array<{ name: string; credentialId: string }> }
          return (data.buckets ?? []).map((bucket) => ({
            name: bucket.name,
            credentialId: credential.id,
          }))
        })
      )
      return responses.flat()
    },
  })

  const { data: tasksData, refetch: refetchTasks } = useQuery<{ tasks: TaskRow[] }>({
    queryKey: ["background-tasks", "tasks-page"],
    queryFn: async () => {
      const res = await fetch("/api/tasks?scope=all&limit=100")
      if (!res.ok) return { tasks: [] }
      return (await res.json()) as { tasks: TaskRow[] }
    },
    refetchInterval: 10_000,
    refetchIntervalInBackground: true,
  })

  const availableOperations = scope === "folder" ? FOLDER_OPERATIONS : BUCKET_OPERATIONS
  const sourceBuckets = useMemo(
    () => buckets.filter((bucket) => bucket.credentialId === sourceCredentialId),
    [buckets, sourceCredentialId]
  )
  const destinationBuckets = useMemo(
    () => buckets.filter((bucket) => bucket.credentialId === destinationCredentialId),
    [buckets, destinationCredentialId]
  )
  const queueTasks = useMemo(
    () =>
      (tasksData?.tasks ?? []).filter(
        (task) =>
          task.type !== "thumbnail_generate" &&
          (task.lifecycleState === "paused" ||
          task.status === "pending" ||
          task.status === "in_progress")
      ),
    [tasksData?.tasks]
  )
  const historyTasks = useMemo(
    () => (tasksData?.tasks ?? []).filter((task) => task.type !== "thumbnail_generate" && (task.status === "completed" || task.status === "failed")),
    [tasksData?.tasks]
  )

  useEffect(() => {
    if (!availableOperations.some((item) => item.value === operation)) {
      setOperation(availableOperations[0]?.value ?? "sync")
    }
  }, [availableOperations, operation])

  useEffect(() => {
    if (credentials.length === 0) return
    if (!sourceCredentialId) setSourceCredentialId(credentials[0].id)
    if (!destinationCredentialId) setDestinationCredentialId(credentials[0].id)
  }, [credentials, sourceCredentialId, destinationCredentialId])

  useEffect(() => {
    if (!sourceCredentialId) return
    if (!sourceBuckets.some((bucket) => bucket.name === sourceBucket)) {
      setSourceBucket(sourceBuckets[0]?.name ?? "")
    }
  }, [sourceBuckets, sourceCredentialId, sourceBucket])

  useEffect(() => {
    if (!destinationCredentialId) return
    if (!destinationBuckets.some((bucket) => bucket.name === destinationBucket)) {
      setDestinationBucket(destinationBuckets[0]?.name ?? "")
    }
  }, [destinationBuckets, destinationCredentialId, destinationBucket])

  useEffect(() => {
    setSourcePrefix("")
  }, [sourceCredentialId, sourceBucket])

  useEffect(() => {
    setDestinationPrefix("")
  }, [destinationCredentialId, destinationBucket])

  const destructiveTask = operation === "sync" || operation === "move" || operation === "migrate"

  function buildTransferTaskBody(): TransferTaskCreateBody | null {
    if (!sourceCredentialId || !destinationCredentialId) {
      toast.error("Select source and destination accounts")
      return null
    }
    if (!sourceBucket || !destinationBucket) {
      toast.error("Select source and destination buckets")
      return null
    }
    if (scope === "folder") {
      if (!sourcePrefix.trim() || !destinationPrefix.trim()) {
        toast.error("Source and destination folder prefixes are required")
        return null
      }
    }

    const body: TransferTaskCreateBody = {
      scope,
      operation,
      sourceBucket,
      sourceCredentialId,
      destinationBucket,
      destinationCredentialId,
    }

    if (scope === "folder") {
      body.sourcePrefix = sourcePrefix.trim()
      body.destinationPrefix = destinationPrefix.trim()
    }

    if (scheduleMode === "cron") {
      if (!scheduleCron.trim()) {
        toast.error("Cron schedule is required")
        return null
      }
      body.schedule = { cron: scheduleCron.trim() }
    } else {
      body.schedule = null
    }

    return body
  }

  async function fetchTransferPreview(
    body: TransferTaskCreateBody,
    options?: {
      cursor?: TransferTaskDetailedPreviewCursor | null
      limit?: number
    }
  ): Promise<TransferTaskPreview> {
    const res = await fetch("/api/tasks/transfer", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        ...body,
        previewOnly: true,
        previewCursor: options?.cursor ?? undefined,
        previewLimit: options?.limit ?? undefined,
      }),
    })

    const data = await res.json()
    if (!res.ok) {
      if (res.status === 403 && data?.details?.plan) {
        throw new Error(
          `${data.error} (resolved plan: ${data.details.plan}, source: ${data.details.planSource})`
        )
      }
      throw new Error(data?.error ?? "Failed to build transfer plan")
    }

    const preview = data?.preview as TransferTaskPreview | undefined
    if (
      !preview ||
      !Array.isArray(preview.summary) ||
      !Array.isArray(preview.commands) ||
      !preview.detailedPlan ||
      !Array.isArray(preview.detailedPlan.actions)
    ) {
      throw new Error("Invalid transfer preview response")
    }

    return preview
  }

  async function createTransferTask(body: TransferTaskCreateBody) {
    setSubmitting(true)
    try {
      const requestBody: TransferTaskCreateBody = {
        ...body,
        ...(body.schedule && destructiveTask
          ? { confirmDestructiveSchedule: true }
          : {}),
      }
      const res = await fetch("/api/tasks/transfer", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
      })

      const data = await res.json()
      if (!res.ok) {
        if (res.status === 403 && data?.details?.plan) {
          throw new Error(
            `${data.error} (resolved plan: ${data.details.plan}, source: ${data.details.planSource})`
          )
        }
        throw new Error(data?.error ?? "Failed to start task")
      }

      queryClient.invalidateQueries({ queryKey: ["background-tasks"] })
      void refetchTasks()
      toast.success(data?.duplicate ? "Equivalent task already queued" : "Task created")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to start task")
      throw error
    } finally {
      setSubmitting(false)
    }
  }

  async function handleStartTask() {
    const body = buildTransferTaskBody()
    if (!body) return

    setSubmitting(true)
    try {
      const preview = await fetchTransferPreview(body)
      setTransferPreview(preview)
      setPendingTransferBody(body)
      setTransferPreviewOpen(true)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to build transfer plan")
    } finally {
      setSubmitting(false)
    }
  }

  async function handleLoadMoreTransferPlan() {
    if (!pendingTransferBody || !transferPreview?.detailedPlan.hasMore) {
      return
    }
    if (!transferPreview.detailedPlan.nextCursor) {
      return
    }

    setLoadingMoreTransferPlan(true)
    try {
      const nextPreview = await fetchTransferPreview(pendingTransferBody, {
        cursor: transferPreview.detailedPlan.nextCursor,
        limit: transferPreview.detailedPlan.pageSize,
      })

      setTransferPreview((current) => {
        if (!current) return nextPreview

        return {
          ...current,
          summary: nextPreview.summary,
          commands: nextPreview.commands,
          estimatedObjects: nextPreview.estimatedObjects,
          sampleObjects:
            current.sampleObjects.length > 0
              ? current.sampleObjects
              : nextPreview.sampleObjects,
          warnings: nextPreview.warnings,
          detailedPlan: {
            ...nextPreview.detailedPlan,
            actions: [...current.detailedPlan.actions, ...nextPreview.detailedPlan.actions],
          },
        }
      })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to load more plan actions")
    } finally {
      setLoadingMoreTransferPlan(false)
    }
  }

  async function handleConfirmTransferFromPreview() {
    if (!pendingTransferBody) {
      toast.error("Missing transfer payload")
      return
    }

    if (destructiveTask && !hasDestructiveConfirmBypass(DESTRUCTIVE_CONFIRM_SCOPE)) {
      setTransferPreviewOpen(false)
      setTransferConfirmOpen(true)
      return
    }

    try {
      await createTransferTask(pendingTransferBody)
      setTransferPreviewOpen(false)
      setTransferPreview(null)
      setPendingTransferBody(null)
    } catch {
      // createTransferTask already handled toast
    }
  }

  async function handleTaskControl(
    taskId: string,
    action: "pause" | "resume" | "restart" | "retry_failed" | "cancel",
    options?: {
      cancelLabel?: "task" | "schedule"
    }
  ) {
    setControllingTaskId(taskId)
    try {
      const res = await fetch(`/api/tasks/${taskId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action }),
      })
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data?.error ?? "Failed to control task")
      }
      queryClient.invalidateQueries({ queryKey: ["background-tasks"] })
      queryClient.invalidateQueries({ queryKey: ["background-tasks", "tasks-page"] })
      void refetchTasks()
      toast.success(
        action === "pause"
          ? "Task paused"
          : action === "resume"
            ? "Task resumed"
            : action === "cancel"
              ? options?.cancelLabel === "schedule"
                ? "Task schedule canceled"
                : "Task canceled"
              : action === "retry_failed"
                ? "Retry for failed items started"
                : "Task restarted"
      )
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to control task")
    } finally {
      setControllingTaskId(null)
    }
  }

  async function handleDeleteTask(taskId: string) {
    if (!window.confirm("Remove this task from history? This cannot be undone.")) {
      return
    }

    setControllingTaskId(taskId)
    try {
      const res = await fetch(`/api/tasks/${taskId}`, {
        method: "DELETE",
      })
      const data = await res.json()
      if (!res.ok) {
        throw new Error(data?.error ?? "Failed to remove task")
      }
      queryClient.invalidateQueries({ queryKey: ["background-tasks"] })
      queryClient.invalidateQueries({ queryKey: ["background-tasks", "tasks-page"] })
      void refetchTasks()
      toast.success("Task removed from history")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Failed to remove task")
    } finally {
      setControllingTaskId(null)
    }
  }

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1 className="flex items-center gap-2 text-2xl font-bold">
          <ListTodo className="h-6 w-6" />
          Tasks
        </h1>
        <p className="text-sm text-muted-foreground">
          Start background tasks and track their execution status.
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Available Task Types</CardTitle>
          <CardDescription>
            Folder scope supports sync/copy/move. Bucket scope supports sync/copy/migrate.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <div className="flex h-full flex-col rounded-md border p-3">
            <p className="mb-2 text-sm font-medium">Between 2 folders (cross bucket possible)</p>
            <ul className="list-disc space-y-1 pl-4 text-sm text-muted-foreground">
              <li>Sync</li>
              <li>One-time copy</li>
              <li>One-time move</li>
            </ul>
          </div>
          <div className="flex h-full flex-col rounded-md border p-3">
            <p className="mb-2 text-sm font-medium">Between 2 buckets</p>
            <ul className="list-disc space-y-1 pl-4 text-sm text-muted-foreground">
              <li>Sync</li>
              <li>One-time copy</li>
              <li>Migrate</li>
            </ul>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <div className="space-y-1">
            <CardTitle>Start New Task</CardTitle>
            <CardDescription>
              Transfers run on cached files only and follow plan limits. Scheduling is optional for every
              transfer operation. Sync mirrors destination scope and deletes destination-only files.
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label>Scope</Label>
              <Select value={scope} onValueChange={(value) => setScope(value as TaskScope)}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="folder">Folder to folder</SelectItem>
                  <SelectItem value="bucket">Bucket to bucket</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Operation</Label>
              <Select value={operation} onValueChange={(value) => setOperation(value as TaskOperation)}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {availableOperations.map((item) => (
                    <SelectItem key={item.value} value={item.value}>
                      {item.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label>Schedule</Label>
              <Select value={scheduleMode} onValueChange={(value) => setScheduleMode(value as "once" | "cron")}>
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="once">One-time run</SelectItem>
                  <SelectItem value="cron">Cron schedule (UTC)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {scheduleMode === "cron" ? (
              <div className="space-y-2">
                <Label>Cron expression (UTC)</Label>
                <Input
                  value={scheduleCron}
                  onChange={(event) => setScheduleCron(event.target.value)}
                  placeholder="0 * * * *"
                />
                <p className="text-xs text-muted-foreground">
                  Minimum supported frequency is once per hour.
                </p>
              </div>
            ) : null}
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label>Source account</Label>
              <Select value={sourceCredentialId} onValueChange={setSourceCredentialId}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select source account" />
                </SelectTrigger>
                <SelectContent>
                  {credentials.map((credential) => (
                    <SelectItem key={credential.id} value={credential.id}>
                      {credential.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Destination account</Label>
              <Select value={destinationCredentialId} onValueChange={setDestinationCredentialId}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select destination account" />
                </SelectTrigger>
                <SelectContent>
                  {credentials.map((credential) => (
                    <SelectItem key={credential.id} value={credential.id}>
                      {credential.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label>Source bucket</Label>
              <Select value={sourceBucket} onValueChange={setSourceBucket}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select source bucket" />
                </SelectTrigger>
                <SelectContent>
                  {sourceBuckets.map((bucket) => (
                    <SelectItem key={`${bucket.credentialId}:${bucket.name}`} value={bucket.name}>
                      {bucket.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Destination bucket</Label>
              <Select value={destinationBucket} onValueChange={setDestinationBucket}>
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select destination bucket" />
                </SelectTrigger>
                <SelectContent>
                  {destinationBuckets.map((bucket) => (
                    <SelectItem key={`${bucket.credentialId}:${bucket.name}`} value={bucket.name}>
                      {bucket.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {scope === "folder" ? (
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2">
                <Label>Source folder prefix</Label>
                <FolderPickerDialog
                  title="Pick Source Folder"
                  description="Select the source folder from cached paths."
                  credentialId={sourceCredentialId}
                  bucket={sourceBucket}
                  value={sourcePrefix}
                  onChange={setSourcePrefix}
                  disabled={!sourceCredentialId || !sourceBucket}
                />
              </div>
              <div className="space-y-2">
                <Label>Destination folder prefix</Label>
                <FolderPickerDialog
                  title="Pick Destination Folder"
                  description="Select the destination folder from cached paths."
                  credentialId={destinationCredentialId}
                  bucket={destinationBucket}
                  value={destinationPrefix}
                  onChange={setDestinationPrefix}
                  disabled={!destinationCredentialId || !destinationBucket}
                />
              </div>
            </div>
          ) : null}

          <div className="flex flex-wrap items-center gap-2">
            <Button
              onClick={() => void handleStartTask()}
              disabled={submitting}
              className="w-full sm:w-auto"
            >
              {submitting ? "Starting..." : "Start Task"}
            </Button>
          </div>

          {destructiveTask ? (
            <p className="text-xs text-destructive">
              This operation can delete data. You will be asked to type confirmation unless bypass is active.
            </p>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Queue</CardTitle>
          <CardDescription>Live tasks with pause/resume/restart/cancel controls.</CardDescription>
        </CardHeader>
        <CardContent>
          {queueTasks.length === 0 ? (
            <p className="text-sm text-muted-foreground">No queued tasks.</p>
          ) : (
            <div className="space-y-2">
              {queueTasks.map((task) => (
                <div key={task.id} className="rounded-md border px-3 py-2">
                  <div className="flex flex-wrap items-start justify-between gap-2">
                    <div className="min-w-0">
                      <p className="min-w-0 text-sm font-medium leading-5">{task.title}</p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        Updated {new Date(task.updatedAt).toLocaleString()} • Runs {task.runCount}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        Schedule: {getTaskScheduleLabel(task)}
                      </p>
                      {(task.successRuns > 0 || task.failedRuns > 0) ? (
                        <p className="mt-1 text-xs text-muted-foreground">
                          Run summary: {task.successRuns} succeeded • {task.failedRuns} failed
                        </p>
                      ) : null}
                      {getTaskResultSummary(task) ? (
                        <p className="mt-1 text-xs text-muted-foreground">
                          Latest result: {getTaskResultSummary(task)}
                        </p>
                      ) : null}
                      {task.isRecurring && task.upcomingRuns.length > 0 ? (
                        <p className="mt-1 text-xs text-muted-foreground">
                          Next 3 scheduled runs:{" "}
                          {task.upcomingRuns
                            .map((item) => new Date(item).toLocaleString())
                            .join(" • ")}
                        </p>
                      ) : null}
                      {task.lastError ? (
                        <p className="mt-1 text-xs text-destructive">{task.lastError}</p>
                      ) : null}
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge
                        variant={
                          task.lifecycleState === "paused"
                            ? "outline"
                            : getStatusVariant(task.status)
                        }
                        className="h-6 px-2 text-xs capitalize"
                      >
                        {getDisplayState(task)}
                      </Badge>
                      {task.lifecycleState === "paused" ? (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={controllingTaskId === task.id}
                          onClick={() => void handleTaskControl(task.id, "resume")}
                        >
                          <Play className="mr-1 h-3.5 w-3.5" />
                          Resume
                        </Button>
                      ) : (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={controllingTaskId === task.id || task.status === "completed"}
                          onClick={() => void handleTaskControl(task.id, "pause")}
                        >
                          <Pause className="mr-1 h-3.5 w-3.5" />
                          Pause
                        </Button>
                      )}
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={controllingTaskId === task.id || task.status === "in_progress"}
                        onClick={() => void handleTaskControl(task.id, "restart")}
                      >
                        <RotateCcw className="mr-1 h-3.5 w-3.5" />
                        Restart
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={controllingTaskId === task.id || task.status === "in_progress"}
                        onClick={() =>
                          void handleTaskControl(task.id, "cancel", {
                            cancelLabel: task.isRecurring ? "schedule" : "task",
                          })
                        }
                      >
                        {task.isRecurring ? "Cancel Schedule" : "Cancel Task"}
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>History</CardTitle>
          <CardDescription>
            Completed/failed tasks, latest execution note, and history cleanup controls.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {historyTasks.length === 0 ? (
            <p className="text-sm text-muted-foreground">No task history yet.</p>
          ) : (
            <div className="space-y-2">
              {historyTasks.map((task) => (
                <div key={task.id} className="rounded-md border px-3 py-2">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0">
                      <p className="min-w-0 text-sm font-medium leading-5">{task.title}</p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        Created {new Date(task.createdAt).toLocaleString()}
                        {task.completedAt
                          ? ` • Completed ${new Date(task.completedAt).toLocaleString()}`
                          : ""}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        Schedule: {getTaskScheduleLabel(task)}
                      </p>
                      {(task.successRuns > 0 || task.failedRuns > 0) ? (
                        <p className="mt-1 text-xs text-muted-foreground">
                          Run summary: {task.successRuns} succeeded • {task.failedRuns} failed
                        </p>
                      ) : null}
                      {getTaskResultSummary(task) ? (
                        <p className="mt-1 text-xs text-muted-foreground">
                          Latest result: {getTaskResultSummary(task)}
                        </p>
                      ) : null}
                      {task.executionHistory[0]?.message ? (
                        <p className="mt-1 text-xs text-muted-foreground">
                          Last event: {task.executionHistory[0].message}
                        </p>
                      ) : null}
                      {task.lastError ? (
                        <p className="mt-1 text-xs text-destructive">{task.lastError}</p>
                      ) : null}
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant={getStatusVariant(task.status)} className="h-6 px-2 text-xs capitalize">
                        {task.status.replace("_", " ")}
                      </Badge>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={controllingTaskId === task.id}
                        onClick={() => void handleTaskControl(task.id, "restart")}
                      >
                        <RotateCcw className="mr-1 h-3.5 w-3.5" />
                        Restart
                      </Button>
                      {canRetryFailed(task) ? (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={controllingTaskId === task.id}
                          onClick={() => void handleTaskControl(task.id, "retry_failed")}
                        >
                          Retry Failed
                        </Button>
                      ) : null}
                      <Button
                        size="sm"
                        variant="ghost"
                        className="text-muted-foreground hover:text-destructive"
                        disabled={controllingTaskId === task.id}
                        onClick={() => void handleDeleteTask(task.id)}
                      >
                        <Trash2 className="mr-1 h-3.5 w-3.5" />
                        Remove
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog
        open={transferPreviewOpen}
        onOpenChange={(open) => {
          setTransferPreviewOpen(open)
          if (!open) {
            setTransferPreview(null)
            setLoadingMoreTransferPlan(false)
            if (!transferConfirmOpen) {
              setPendingTransferBody(null)
            }
          }
        }}
      >
        <DialogContent className="max-h-[85vh] overflow-y-auto overflow-x-hidden sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Task Execution Plan</DialogTitle>
            <DialogDescription>
              Review the planned execution summary before this task starts.
            </DialogDescription>
          </DialogHeader>

          {transferPreview ? (
            <div className="space-y-4 text-sm">
              <div className="space-y-2">
                <p className="font-medium">Summary</p>
                <ul className="list-disc space-y-1 pl-5 text-muted-foreground">
                  {transferPreview.summary.map((line) => (
                    <li key={line}>{line}</li>
                  ))}
                </ul>
              </div>

              <div className="space-y-2">
                <p className="font-medium">Planned commands</p>
                <ul className="list-disc space-y-1 pl-5 text-muted-foreground">
                  {transferPreview.commands.map((line) => (
                    <li key={line}>{line}</li>
                  ))}
                </ul>
              </div>

              <div className="space-y-2">
                <p className="font-medium">
                  Planned object actions ({transferPreview.detailedPlan.actions.length} loaded)
                </p>
                {transferPreview.detailedPlan.actions.length > 0 ? (
                  <ul className="max-h-72 overflow-y-auto overflow-x-hidden rounded-md border p-2 font-mono text-xs">
                    {transferPreview.detailedPlan.actions.map((action, index) => (
                      <li
                        key={`${action.command}:${action.sourceKey ?? "none"}:${action.destinationKey ?? "none"}:${index}`}
                        className="break-all py-0.5 whitespace-normal"
                      >
                        {action.command}
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    No object-level actions in this page. Load more to continue scanning.
                  </p>
                )}
                <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                  <span>
                    Scanned {transferPreview.detailedPlan.scannedSourceObjects.toLocaleString()} source objects in this
                    request.
                  </span>
                  {transferPreview.detailedPlan.scanLimitReached ? (
                    <span>Scan limit reached for this page; load more to continue.</span>
                  ) : null}
                </div>
                {transferPreview.detailedPlan.hasMore ? (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => void handleLoadMoreTransferPlan()}
                    disabled={loadingMoreTransferPlan || submitting}
                  >
                    {loadingMoreTransferPlan ? "Loading..." : "Load more actions"}
                  </Button>
                ) : (
                  <p className="text-xs text-muted-foreground">
                    Full plan loaded for the current cached metadata snapshot.
                  </p>
                )}
              </div>

              {transferPreview.warnings.length > 0 ? (
                <div className="space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-3">
                  <p className="font-medium text-destructive">Warnings</p>
                  <ul className="list-disc space-y-1 pl-5 text-xs text-destructive">
                    {transferPreview.warnings.map((line) => (
                      <li key={line}>{line}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="flex justify-end gap-2">
            <Button
              variant="outline"
              onClick={() => setTransferPreviewOpen(false)}
              disabled={submitting}
            >
              Cancel
            </Button>
            <Button onClick={() => void handleConfirmTransferFromPreview()} disabled={submitting}>
              {submitting ? "Starting..." : "Start Task"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      <DestructiveConfirmDialog
        open={transferConfirmOpen}
        onOpenChange={(open) => {
          setTransferConfirmOpen(open)
          if (!open) {
            setTransferPreview(null)
            setPendingTransferBody(null)
          }
        }}
        title="Confirm destructive transfer"
        description={
          operation === "sync"
            ? "Sync mirrors destination to source scope and deletes destination-only files."
            : operation === "move" || operation === "migrate"
              ? "Move and migrate delete source objects after copying them to destination."
              : "This operation can delete objects."
        }
        actionLabel="Start Task"
        onConfirm={async () => {
          if (!pendingTransferBody) {
            throw new Error("Missing transfer payload")
          }
          await createTransferTask(pendingTransferBody)
          setTransferPreview(null)
          setPendingTransferBody(null)
        }}
      />
    </div>
  )
}
