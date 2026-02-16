"use client"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Loader2 } from "lucide-react"

export interface BulkDeleteTaskPreview {
  type: "bulk_delete"
  summary: string[]
  commands: string[]
  estimatedObjects: number
  sampleObjects: string[]
  warnings: string[]
}

interface BulkDeletePreviewDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  preview: BulkDeleteTaskPreview | null
  isRunning: boolean
  onConfirm: () => void
  onCancel: () => void
}

export function BulkDeletePreviewDialog({
  open,
  onOpenChange,
  preview,
  isRunning,
  onConfirm,
  onCancel,
}: BulkDeletePreviewDialogProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Bulk Delete Execution Plan</DialogTitle>
          <DialogDescription>
            Review the planned execution summary before this task starts.
          </DialogDescription>
        </DialogHeader>

        {preview ? (
          <div className="space-y-4 text-sm">
            <div className="space-y-2">
              <p className="font-medium">Summary</p>
              <ul className="list-disc space-y-1 pl-5 text-muted-foreground">
                {preview.summary.map((line) => (
                  <li key={line}>{line}</li>
                ))}
              </ul>
            </div>

            <div className="space-y-2">
              <p className="font-medium">Planned commands</p>
              <ul className="list-disc space-y-1 pl-5 text-muted-foreground">
                {preview.commands.map((line) => (
                  <li key={line}>{line}</li>
                ))}
              </ul>
            </div>

            <div className="space-y-2">
              <p className="font-medium">
                Sample matching objects ({preview.sampleObjects.length})
              </p>
              {preview.sampleObjects.length > 0 ? (
                <ul className="max-h-44 overflow-y-auto rounded-md border p-2 font-mono text-xs">
                  {preview.sampleObjects.map((item) => (
                    <li key={item} className="truncate">
                      {item}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-xs text-muted-foreground">No sample objects available.</p>
              )}
            </div>

            {preview.warnings.length > 0 ? (
              <div className="space-y-2 rounded-md border border-destructive/40 bg-destructive/5 p-3">
                <p className="font-medium text-destructive">Warnings</p>
                <ul className="list-disc space-y-1 pl-5 text-xs text-destructive">
                  {preview.warnings.map((line) => (
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
            onClick={onCancel}
            disabled={isRunning}
          >
            Cancel
          </Button>
          <Button onClick={onConfirm} disabled={isRunning}>
            {isRunning ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
            Start Delete Task
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}
