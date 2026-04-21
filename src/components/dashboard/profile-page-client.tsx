"use client"

import Image from "next/image"
import { useEffect, useState, useTransition } from "react"
import { useSession } from "next-auth/react"
import { toast } from "sonner"
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

interface AccountResponse {
  user: {
    id: string
    name: string | null
    email: string
    role: string
    totpEnabled: boolean
  }
}

interface SetupResponse {
  secret: string
  otpauthUrl: string
  qrDataUrl: string
}

export function ProfilePageClient() {
  const { data: session, update: updateSession } = useSession()
  const [name, setName] = useState("")
  const [email, setEmail] = useState("")
  const [totpEnabled, setTotpEnabled] = useState(false)
  const [loadingProfile, setLoadingProfile] = useState(true)

  const [currentPassword, setCurrentPassword] = useState("")
  const [newPassword, setNewPassword] = useState("")
  const [confirmPassword, setConfirmPassword] = useState("")

  const [setup, setSetup] = useState<SetupResponse | null>(null)
  const [otpCode, setOtpCode] = useState("")
  const [backupCodes, setBackupCodes] = useState<string[] | null>(null)
  const [disablePassword, setDisablePassword] = useState("")

  const [profilePending, startProfileTransition] = useTransition()
  const [passwordPending, startPasswordTransition] = useTransition()
  const [setupPending, startSetupTransition] = useTransition()
  const [enablePending, startEnableTransition] = useTransition()
  const [disablePending, startDisableTransition] = useTransition()

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = await fetch("/api/account/profile", { cache: "no-store" })
        if (!res.ok) return
        const json = (await res.json()) as AccountResponse
        if (cancelled) return
        setName(json.user.name ?? "")
        setEmail(json.user.email)
        setTotpEnabled(json.user.totpEnabled)
      } finally {
        if (!cancelled) setLoadingProfile(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  function handleProfileSubmit(e: React.FormEvent) {
    e.preventDefault()
    startProfileTransition(async () => {
      const res = await fetch("/api/account/profile", {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name, email }),
      })
      const json = (await res.json().catch(() => ({}))) as { error?: string }
      if (!res.ok) {
        toast.error(json.error ?? "Could not update profile")
        return
      }
      toast.success("Profile updated")
      await updateSession?.({ name, email })
    })
  }

  function handlePasswordSubmit(e: React.FormEvent) {
    e.preventDefault()
    startPasswordTransition(async () => {
      const res = await fetch("/api/account/password", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ currentPassword, newPassword, confirmPassword }),
      })
      const json = (await res.json().catch(() => ({}))) as { error?: string }
      if (!res.ok) {
        toast.error(json.error ?? "Could not change password")
        return
      }
      toast.success("Password changed")
      setCurrentPassword("")
      setNewPassword("")
      setConfirmPassword("")
    })
  }

  function handleStartSetup() {
    startSetupTransition(async () => {
      const res = await fetch("/api/account/2fa/setup", { method: "POST" })
      const json = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(("error" in json && json.error) || "Could not start 2FA setup")
        return
      }
      setSetup(json as SetupResponse)
      setOtpCode("")
      setBackupCodes(null)
    })
  }

  function handleEnable(e: React.FormEvent) {
    e.preventDefault()
    startEnableTransition(async () => {
      const res = await fetch("/api/account/2fa/enable", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ code: otpCode }),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(("error" in json && json.error) || "Invalid code")
        return
      }
      setTotpEnabled(true)
      setSetup(null)
      setBackupCodes((json as { backupCodes: string[] }).backupCodes)
      toast.success("Two-factor authentication enabled")
    })
  }

  function handleDisable(e: React.FormEvent) {
    e.preventDefault()
    startDisableTransition(async () => {
      const res = await fetch("/api/account/2fa/disable", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ password: disablePassword }),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(("error" in json && json.error) || "Could not disable 2FA")
        return
      }
      setTotpEnabled(false)
      setDisablePassword("")
      setBackupCodes(null)
      setSetup(null)
      toast.success("Two-factor authentication disabled")
    })
  }

  return (
    <div className="mx-auto max-w-3xl space-y-6 p-4 sm:p-6">
      <div>
        <h1 className="text-2xl font-semibold">Profile</h1>
        <p className="text-sm text-muted-foreground">
          Manage your account, password and two-factor authentication.
          {session?.user?.role === "admin" && " · You are an administrator."}
        </p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Account</CardTitle>
          <CardDescription>Update your display name and email.</CardDescription>
        </CardHeader>
        <form onSubmit={handleProfileSubmit}>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="profile-name">Name</Label>
              <Input
                id="profile-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                disabled={loadingProfile || profilePending}
                required
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="profile-email">Email</Label>
              <Input
                id="profile-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                disabled={loadingProfile || profilePending}
                required
              />
            </div>
            <div>
              <Button type="submit" disabled={loadingProfile || profilePending}>
                {profilePending ? "Saving…" : "Save changes"}
              </Button>
            </div>
          </CardContent>
        </form>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Password</CardTitle>
          <CardDescription>
            Minimum 8 characters. Your current password is required to make the change.
          </CardDescription>
        </CardHeader>
        <form onSubmit={handlePasswordSubmit}>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="current-password">Current password</Label>
              <Input
                id="current-password"
                type="password"
                autoComplete="current-password"
                value={currentPassword}
                onChange={(e) => setCurrentPassword(e.target.value)}
                required
                disabled={passwordPending}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-password">New password</Label>
              <Input
                id="new-password"
                type="password"
                autoComplete="new-password"
                minLength={8}
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                required
                disabled={passwordPending}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="confirm-password">Confirm new password</Label>
              <Input
                id="confirm-password"
                type="password"
                autoComplete="new-password"
                minLength={8}
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                required
                disabled={passwordPending}
              />
            </div>
            <div>
              <Button type="submit" disabled={passwordPending}>
                {passwordPending ? "Updating…" : "Change password"}
              </Button>
            </div>
          </CardContent>
        </form>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Two-factor authentication</CardTitle>
          <CardDescription>
            Protect your account with a time-based one-time code (TOTP) from an
            authenticator app such as 1Password, Bitwarden, Google Authenticator or Aegis.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {totpEnabled ? (
            <form onSubmit={handleDisable} className="space-y-3">
              <p className="text-sm">
                Two-factor authentication is <strong>enabled</strong> for your account.
              </p>
              <div className="space-y-2">
                <Label htmlFor="disable-password">Password</Label>
                <Input
                  id="disable-password"
                  type="password"
                  autoComplete="current-password"
                  value={disablePassword}
                  onChange={(e) => setDisablePassword(e.target.value)}
                  required
                  disabled={disablePending}
                />
              </div>
              <Button type="submit" variant="destructive" disabled={disablePending}>
                {disablePending ? "Disabling…" : "Disable 2FA"}
              </Button>
            </form>
          ) : setup ? (
            <form onSubmit={handleEnable} className="space-y-4">
              <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center">
                <Image
                  src={setup.qrDataUrl}
                  alt="Scan this QR code with your authenticator app"
                  width={176}
                  height={176}
                  unoptimized
                  className="rounded-md border bg-white p-2"
                />
                <div className="space-y-2 text-sm">
                  <p>
                    Scan the QR code or paste this secret into your authenticator:
                  </p>
                  <code className="block break-all rounded bg-muted px-2 py-1 text-xs">
                    {setup.secret}
                  </code>
                </div>
              </div>
              <div className="space-y-2">
                <Label htmlFor="enable-otp">Code from authenticator</Label>
                <Input
                  id="enable-otp"
                  inputMode="numeric"
                  pattern="[0-9]{6}"
                  placeholder="123456"
                  value={otpCode}
                  onChange={(e) => setOtpCode(e.target.value)}
                  required
                  disabled={enablePending}
                />
              </div>
              <Button type="submit" disabled={enablePending}>
                {enablePending ? "Verifying…" : "Enable 2FA"}
              </Button>
            </form>
          ) : (
            <Button onClick={handleStartSetup} disabled={setupPending}>
              {setupPending ? "Preparing…" : "Set up two-factor authentication"}
            </Button>
          )}

          {backupCodes && (
            <div className="rounded-md border border-amber-500/50 bg-amber-500/10 p-3 text-sm">
              <p className="font-medium">Save these backup codes</p>
              <p className="text-xs text-muted-foreground">
                Each code works once. Store them somewhere safe &mdash; they are shown only this time.
              </p>
              <ul className="mt-2 grid grid-cols-2 gap-1 font-mono text-xs sm:grid-cols-5">
                {backupCodes.map((code) => (
                  <li key={code} className="rounded bg-background px-2 py-1">
                    {code}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
