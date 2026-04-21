"use client"

import { useState, useTransition } from "react"
import { useRouter } from "next/navigation"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { MoreHorizontal } from "lucide-react"

export type AdminUserRow = {
  id: string
  email: string
  name: string | null
  role: string
  isActive: boolean
  credentialCount: number
  lastLoginAt: string | null
  createdAt: string
}

export function AdminUsersTable({ users }: { users: AdminUserRow[] }) {
  const router = useRouter()
  const [busyId, setBusyId] = useState<string | null>(null)
  const [pending, startTransition] = useTransition()

  async function patch(id: string, data: Record<string, unknown>, okMsg: string) {
    setBusyId(id)
    try {
      const res = await fetch(`/api/admin/users/${id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(body?.error ?? "Failed")
        return
      }
      toast.success(okMsg)
      startTransition(() => router.refresh())
    } finally {
      setBusyId(null)
    }
  }

  async function remove(id: string, email: string) {
    if (!confirm(`Delete user ${email}? This removes their buckets and shares.`)) return
    setBusyId(id)
    try {
      const res = await fetch(`/api/admin/users/${id}`, { method: "DELETE" })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(body?.error ?? "Failed")
        return
      }
      toast.success("User deleted")
      startTransition(() => router.refresh())
    } finally {
      setBusyId(null)
    }
  }

  return (
    <div className="rounded-md border overflow-hidden">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>User</TableHead>
            <TableHead>Role</TableHead>
            <TableHead>Status</TableHead>
            <TableHead className="text-right">Buckets</TableHead>
            <TableHead>Last login</TableHead>
            <TableHead className="w-10"></TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {users.length === 0 ? (
            <TableRow>
              <TableCell colSpan={6} className="text-center text-sm text-muted-foreground py-8">
                No users yet.
              </TableCell>
            </TableRow>
          ) : (
            users.map((u) => (
              <TableRow key={u.id} className={busyId === u.id || pending ? "opacity-50" : ""}>
                <TableCell>
                  <div className="font-medium">{u.name ?? "—"}</div>
                  <div className="text-xs text-muted-foreground">{u.email}</div>
                </TableCell>
                <TableCell>
                  <span
                    className={
                      u.role === "admin"
                        ? "text-xs px-2 py-0.5 rounded-md bg-primary/10 text-primary border border-primary/20"
                        : "text-xs px-2 py-0.5 rounded-md bg-muted text-muted-foreground"
                    }
                  >
                    {u.role}
                  </span>
                </TableCell>
                <TableCell>
                  <span
                    className={
                      u.isActive
                        ? "text-xs px-2 py-0.5 rounded-md bg-emerald-500/10 text-emerald-600 border border-emerald-500/20"
                        : "text-xs px-2 py-0.5 rounded-md bg-destructive/10 text-destructive border border-destructive/20"
                    }
                  >
                    {u.isActive ? "active" : "disabled"}
                  </span>
                </TableCell>
                <TableCell className="text-right tabular-nums">{u.credentialCount}</TableCell>
                <TableCell className="text-xs text-muted-foreground">
                  {u.lastLoginAt ? new Date(u.lastLoginAt).toLocaleString() : "never"}
                </TableCell>
                <TableCell>
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button variant="ghost" size="icon-sm" disabled={busyId === u.id}>
                        <MoreHorizontal />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem
                        onClick={() =>
                          patch(
                            u.id,
                            { role: u.role === "admin" ? "user" : "admin" },
                            u.role === "admin" ? "Role set to user" : "Promoted to admin",
                          )
                        }
                      >
                        {u.role === "admin" ? "Demote to user" : "Promote to admin"}
                      </DropdownMenuItem>
                      <DropdownMenuItem
                        onClick={() =>
                          patch(
                            u.id,
                            { isActive: !u.isActive },
                            u.isActive ? "User disabled" : "User enabled",
                          )
                        }
                      >
                        {u.isActive ? "Disable account" : "Enable account"}
                      </DropdownMenuItem>
                      <DropdownMenuItem
                        onClick={() => {
                          const pwd = prompt("New password for " + u.email + " (min 8 chars):")
                          if (!pwd) return
                          if (pwd.length < 8) {
                            toast.error("Password too short")
                            return
                          }
                          patch(u.id, { password: pwd }, "Password reset")
                        }}
                      >
                        Reset password…
                      </DropdownMenuItem>
                      <DropdownMenuSeparator />
                      <DropdownMenuItem
                        className="text-destructive focus:text-destructive"
                        onClick={() => remove(u.id, u.email)}
                      >
                        Delete user
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>
    </div>
  )
}
