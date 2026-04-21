import { NextResponse } from "next/server"
import { z } from "zod/v4"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { isMultiUserMode } from "@/lib/auth-mode"

const schema = z.object({
  name: z.string().trim().min(1).max(120),
  email: z.string().trim().toLowerCase().email(),
})

export async function GET() {
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const user = await prisma.user.findUnique({
    where: { id: session.user.id },
    select: {
      id: true,
      name: true,
      email: true,
      role: true,
      createdAt: true,
      lastLoginAt: true,
      totpEnabled: true,
    },
  })
  if (!user) return NextResponse.json({ error: "Not found" }, { status: 404 })
  return NextResponse.json({ user })
}

export async function PATCH(req: Request) {
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
  const { name, email } = parsed.data

  // Reject email collision with another user.
  const collision = await prisma.user.findFirst({
    where: { email, NOT: { id: session.user.id } },
    select: { id: true },
  })
  if (collision) {
    return NextResponse.json({ error: "Email already in use" }, { status: 409 })
  }

  const user = await prisma.user.update({
    where: { id: session.user.id },
    data: { name, email },
    select: { id: true, name: true, email: true },
  })
  return NextResponse.json({ user })
}
