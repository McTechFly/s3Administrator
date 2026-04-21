import { NextResponse } from "next/server"
import { z } from "zod/v4"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { isMultiUserMode } from "@/lib/auth-mode"
import {
  generateBackupCodes,
  hashBackupCodes,
  verifyTotpCode,
} from "@/lib/totp"

const schema = z.object({
  code: z.string().trim().regex(/^\d{6}$/, "Enter the 6-digit code"),
})

/**
 * Confirms 2FA enrollment: requires the user to submit a valid TOTP code
 * matching the pending secret. On success we flip `totpEnabled` and issue
 * one-time backup recovery codes (returned ONCE in the response, never stored
 * in plaintext).
 */
export async function POST(req: Request) {
  if (!isMultiUserMode()) {
    return NextResponse.json({ error: "Not available in single-user mode" }, { status: 404 })
  }
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const body = await req.json().catch(() => null)
  const parsed = schema.safeParse(body)
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid input", details: parsed.error.flatten() },
      { status: 400 },
    )
  }

  const user = await prisma.user.findUnique({
    where: { id: session.user.id },
    select: { totpSecret: true, totpEnabled: true },
  })
  if (!user?.totpSecret) {
    return NextResponse.json(
      { error: "No pending 2FA enrollment. Call /api/account/2fa/setup first." },
      { status: 400 },
    )
  }
  if (user.totpEnabled) {
    return NextResponse.json({ error: "2FA is already enabled" }, { status: 409 })
  }

  const ok = verifyTotpCode(user.totpSecret, parsed.data.code)
  if (!ok) {
    return NextResponse.json({ error: "Invalid code. Check your authenticator clock." }, { status: 400 })
  }

  const backupCodes = generateBackupCodes(10)
  const hashed = await hashBackupCodes(backupCodes)

  await prisma.user.update({
    where: { id: session.user.id },
    data: {
      totpEnabled: true,
      totpBackupCodes: JSON.stringify(hashed),
    },
  })

  return NextResponse.json({ ok: true, backupCodes })
}
