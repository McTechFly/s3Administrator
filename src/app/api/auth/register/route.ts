import { NextRequest, NextResponse } from "next/server"
import { z } from "zod/v4"
import bcrypt from "bcryptjs"
import { prisma } from "@/lib/db"
import { isMultiUserMode } from "@/lib/auth-mode"

const schema = z.object({
  name: z.string().trim().min(1).max(120),
  email: z.string().trim().toLowerCase().email(),
  password: z.string().min(8).max(256),
})

export async function POST(req: NextRequest) {
  if (!isMultiUserMode()) {
    return NextResponse.json(
      { error: "Registration is disabled (single-user mode)" },
      { status: 404 },
    )
  }

  // Optional signup lockdown once the instance is bootstrapped.
  // Set `DISABLE_SIGNUP=true` to block new registrations after the first admin exists.
  const signupDisabled = (process.env.DISABLE_SIGNUP ?? "").toLowerCase() === "true"

  const body = await req.json().catch(() => null)
  const parsed = schema.safeParse(body)
  if (!parsed.success) {
    return NextResponse.json(
      { error: "Invalid input", details: parsed.error.flatten() },
      { status: 400 },
    )
  }

  const { name, email, password } = parsed.data

  const existing = await prisma.user.findUnique({ where: { email } })
  if (existing) {
    return NextResponse.json(
      { error: "An account with this email already exists" },
      { status: 409 },
    )
  }

  const userCount = await prisma.user.count()
  const isFirstAdmin = userCount === 0

  if (!isFirstAdmin && signupDisabled) {
    return NextResponse.json(
      { error: "Signup is currently disabled. Ask an admin to invite you." },
      { status: 403 },
    )
  }

  const passwordHash = await bcrypt.hash(password, 12)

  const user = await prisma.user.create({
    data: {
      name,
      email,
      passwordHash,
      role: isFirstAdmin ? "admin" : "user",
      emailVerified: new Date(),
      isActive: true,
    },
    select: { id: true, email: true, role: true },
  })

  return NextResponse.json({
    ok: true,
    isFirstAdmin,
    user,
  })
}
