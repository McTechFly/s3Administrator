"use client"

import { useState, useTransition } from "react"
import Link from "next/link"
import { useRouter, useSearchParams } from "next/navigation"
import { signIn } from "next-auth/react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"

export function LoginForm() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const callbackUrl = searchParams.get("callbackUrl") ?? "/dashboard"
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [totp, setTotp] = useState("")
  const [needsTotp, setNeedsTotp] = useState(false)
  const [pending, startTransition] = useTransition()

  function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    startTransition(async () => {
      const res = await signIn("credentials", {
        email,
        password,
        totp: needsTotp ? totp : "",
        redirect: false,
      })
      if (!res || res.error) {
        const code = (res?.code ?? "").toString()
        if (code.includes("TOTP_REQUIRED")) {
          setNeedsTotp(true)
          toast.info("Enter the 6-digit code from your authenticator app.")
          return
        }
        if (code.includes("TOTP_INVALID")) {
          toast.error("Invalid 2FA code")
          return
        }
        toast.error("Invalid email or password")
        return
      }
      toast.success("Signed in")
      router.push(callbackUrl)
      router.refresh()
    })
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>Sign in</CardTitle>
        <CardDescription>Access your S3 Administrator workspace.</CardDescription>
      </CardHeader>
      <form onSubmit={onSubmit}>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="email">Email</Label>
            <Input
              id="email"
              type="email"
              autoComplete="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              disabled={pending}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              autoComplete="current-password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={pending}
            />
          </div>
          {needsTotp && (
            <div className="space-y-2">
              <Label htmlFor="totp">Two-factor code</Label>
              <Input
                id="totp"
                type="text"
                inputMode="numeric"
                autoComplete="one-time-code"
                placeholder="123456 or backup code"
                required
                value={totp}
                onChange={(e) => setTotp(e.target.value)}
                disabled={pending}
              />
              <p className="text-xs text-muted-foreground">
                Enter the 6-digit code from your authenticator, or one of your recovery codes.
              </p>
            </div>
          )}
        </CardContent>
        <CardFooter className="flex flex-col gap-3 items-stretch">
          <Button type="submit" disabled={pending}>
            {pending ? "Signing in…" : "Sign in"}
          </Button>
          <p className="text-sm text-muted-foreground text-center">
            Don&apos;t have an account?{" "}
            <Link href="/register" className="text-primary hover:underline">
              Create one
            </Link>
          </p>
        </CardFooter>
      </form>
    </Card>
  )
}
