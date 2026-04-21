"use client"

import { useEffect, useMemo, useState, type FormEvent } from "react"
import { toast } from "sonner"
import { Trash2, Loader2, Plus } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
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
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"

type PermissionLevel = "read" | "read_write" | "full"

type OwnedCredential = { id: string; label: string; provider: string }

type IncomingShare = {
  id: string
  bucket: string | null
  permissionLevel: PermissionLevel
  createdAt: string
  credential: { id: string; label: string; provider: string }
  owner: { id: string; email: string; name: string | null }
}

type OutgoingShare = {
  id: string
  bucket: string | null
  permissionLevel: PermissionLevel
  createdAt: string
  credential: { id: string; label: string; provider: string }
  target: { id: string; email: string; name: string | null }
}

export function SharesManager() {
  const [owned, setOwned] = useState<OwnedCredential[]>([])
  const [incoming, setIncoming] = useState<IncomingShare[]>([])
  const [outgoing, setOutgoing] = useState<OutgoingShare[]>([])
  const [loading, setLoading] = useState(true)

  async function refresh() {
    setLoading(true)
    try {
      const [credRes, sharesRes] = await Promise.all([
        fetch("/api/s3/credentials", { cache: "no-store" }),
        fetch("/api/shares", { cache: "no-store" }),
      ])
      const creds = credRes.ok ? await credRes.json() : []
      const shares = sharesRes.ok
        ? await sharesRes.json()
        : { incoming: [], outgoing: [] }
      setOwned(
        (Array.isArray(creds) ? creds : [])
          .filter((c: { sharedFrom?: unknown }) => !c.sharedFrom)
          .map((c: OwnedCredential) => ({ id: c.id, label: c.label, provider: c.provider })),
      )
      setIncoming(shares.incoming ?? [])
      setOutgoing(shares.outgoing ?? [])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    void refresh()
  }, [])

  async function revoke(id: string) {
    const res = await fetch(`/api/shares/${id}`, { method: "DELETE" })
    if (!res.ok) {
      toast.error("Failed to revoke share")
      return
    }
    toast.success("Share removed")
    void refresh()
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" />
        Loading shares…
      </div>
    )
  }

  return (
    <div className="grid gap-6">
      <Card>
        <CardHeader className="flex-row items-start justify-between gap-4">
          <div>
            <CardTitle>Shares you granted</CardTitle>
            <CardDescription>
              Buckets you exposed to other users. You keep full control of the
              access keys.
            </CardDescription>
          </div>
          <NewShareDialog owned={owned} onCreated={refresh} />
        </CardHeader>
        <CardContent>
          {outgoing.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              You haven&apos;t shared any bucket yet.
            </p>
          ) : (
            <ul className="divide-y">
              {outgoing.map((s) => (
                <li key={s.id} className="py-3 flex items-center justify-between gap-4">
                  <div className="min-w-0">
                    <div className="font-medium truncate">
                      {s.credential.label}
                      {s.bucket ? (
                        <span className="text-muted-foreground"> / {s.bucket}</span>
                      ) : (
                        <span className="text-muted-foreground"> (all buckets)</span>
                      )}
                    </div>
                    <div className="text-xs text-muted-foreground truncate">
                      → {s.target.email} · <Badge variant="secondary">{s.permissionLevel}</Badge>
                    </div>
                  </div>
                  <Button size="sm" variant="ghost" onClick={() => revoke(s.id)}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Shared with you</CardTitle>
          <CardDescription>
            Buckets other users granted you access to. They appear alongside
            your own buckets on the dashboard.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {incoming.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No one has shared a bucket with you yet.
            </p>
          ) : (
            <ul className="divide-y">
              {incoming.map((s) => (
                <li key={s.id} className="py-3 flex items-center justify-between gap-4">
                  <div className="min-w-0">
                    <div className="font-medium truncate">
                      {s.credential.label}
                      {s.bucket ? (
                        <span className="text-muted-foreground"> / {s.bucket}</span>
                      ) : (
                        <span className="text-muted-foreground"> (all buckets)</span>
                      )}
                    </div>
                    <div className="text-xs text-muted-foreground truncate">
                      from {s.owner.email} · <Badge variant="secondary">{s.permissionLevel}</Badge>
                    </div>
                  </div>
                  <Button size="sm" variant="ghost" onClick={() => revoke(s.id)}>
                    Remove
                  </Button>
                </li>
              ))}
            </ul>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

function NewShareDialog({
  owned,
  onCreated,
}: {
  owned: OwnedCredential[]
  onCreated: () => void
}) {
  const [open, setOpen] = useState(false)
  const [credentialId, setCredentialId] = useState<string>("")
  const [targetEmail, setTargetEmail] = useState("")
  const [bucket, setBucket] = useState("")
  const [permissionLevel, setPermissionLevel] = useState<PermissionLevel>("read")
  const [submitting, setSubmitting] = useState(false)

  const disabled = useMemo(
    () => owned.length === 0,
    [owned.length],
  )

  async function submit(e: FormEvent) {
    e.preventDefault()
    if (!credentialId || !targetEmail) return
    setSubmitting(true)
    try {
      const res = await fetch("/api/shares", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          credentialId,
          targetEmail,
          bucket: bucket.trim() || null,
          permissionLevel,
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(data?.error ?? "Failed to share bucket")
        return
      }
      toast.success("Bucket shared")
      setOpen(false)
      setTargetEmail("")
      setBucket("")
      setPermissionLevel("read")
      onCreated()
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button disabled={disabled} size="sm">
          <Plus className="h-4 w-4 mr-1" /> New share
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Share a bucket</DialogTitle>
        </DialogHeader>
        <form onSubmit={submit} className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="credential">Credential</Label>
            <Select value={credentialId} onValueChange={setCredentialId}>
              <SelectTrigger id="credential">
                <SelectValue placeholder="Select a connected credential" />
              </SelectTrigger>
              <SelectContent>
                {owned.map((c) => (
                  <SelectItem key={c.id} value={c.id}>
                    {c.label} ({c.provider})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="email">Target user email</Label>
            <Input
              id="email"
              type="email"
              required
              value={targetEmail}
              onChange={(e) => setTargetEmail(e.target.value)}
              placeholder="user@example.com"
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="bucket">Bucket name (optional)</Label>
            <Input
              id="bucket"
              value={bucket}
              onChange={(e) => setBucket(e.target.value)}
              placeholder="Leave blank to share all buckets of this credential"
            />
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="level">Permission</Label>
            <Select
              value={permissionLevel}
              onValueChange={(v) => setPermissionLevel(v as PermissionLevel)}
            >
              <SelectTrigger id="level">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="read">Read only</SelectItem>
                <SelectItem value="read_write">Read & write</SelectItem>
                <SelectItem value="full">Full (incl. delete)</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button type="submit" disabled={submitting || !credentialId || !targetEmail}>
              {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : "Share"}
            </Button>
          </div>
        </form>
      </DialogContent>
    </Dialog>
  )
}
