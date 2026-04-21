import { NextResponse } from "next/server"
import { z } from "zod/v4"
import bcrypt from "bcryptjs"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { isMultiUserMode } from "@/lib/auth-mode"

const schema = z.object({
  password: z.string().min(1),
})

/**
 * Disables 2FA. Requires re-entry of the account password to prevent a hijacked
 * browser session from silently stripping the second factor.
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
    return NextResponse.json({ error: "Invalid input" }, { status: 400 })
  }

  const user = await prisma.user.findUnique({
    where: { id: session.user.id },
    select: { passwordHash: true, totpEnabled: true },
  })
  if (!user?.passwordHash) {
    return NextResponse.json({ error: "Password re-entry required" }, { status: 400 })
  }
  if (!user.totpEnabled) {
    return NextResponse.json({ error: "2FA is not enabled" }, { status: 400 })
  }
  const ok = await bcrypt.compare(parsed.data.password, user.passwordHash)
  if (!ok) return NextResponse.json({ error: "Password is incorrect" }, { status: 403 })

  await prisma.user.update({
    where: { id: session.user.id },
    data: { totpEnabled: false, totpSecret: null, totpBackupCodes: null },
  })
  return NextResponse.json({ ok: true })
}
