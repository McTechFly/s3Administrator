import { NextRequest, NextResponse } from "next/server"
import { z } from "zod/v4"
import bcrypt from "bcryptjs"
import { prisma } from "@/lib/db"
import { auth } from "@/lib/auth"
import { isAdmin } from "@/lib/permissions"

const patchSchema = z.object({
  role: z.enum(["admin", "user"]).optional(),
  isActive: z.boolean().optional(),
  password: z.string().min(8).max(256).optional(),
  name: z.string().trim().min(1).max(120).optional(),
})

async function requireAdmin() {
  const session = await auth()
  if (!session?.user?.id || !isAdmin(session.user.role)) return null
  return session
}

export async function PATCH(req: NextRequest, ctx: { params: Promise<{ id: string }> }) {
  const session = await requireAdmin()
  if (!session) return NextResponse.json({ error: "Forbidden" }, { status: 403 })

  const { id } = await ctx.params
  const body = await req.json().catch(() => null)
  const parsed = patchSchema.safeParse(body)
  if (!parsed.success) return NextResponse.json({ error: "Invalid input" }, { status: 400 })

  const target = await prisma.user.findUnique({ where: { id } })
  if (!target) return NextResponse.json({ error: "Not found" }, { status: 404 })

  const update: Record<string, unknown> = {}
  if (parsed.data.name !== undefined) update.name = parsed.data.name
  if (parsed.data.role !== undefined) {
    if (target.role === "admin" && parsed.data.role !== "admin") {
      const adminCount = await prisma.user.count({ where: { role: "admin" } })
      if (adminCount <= 1) {
        return NextResponse.json({ error: "Cannot demote the last admin" }, { status: 400 })
      }
    }
    update.role = parsed.data.role
  }
  if (parsed.data.isActive !== undefined) {
    if (target.id === session.user!.id && !parsed.data.isActive) {
      return NextResponse.json({ error: "You cannot disable your own account" }, { status: 400 })
    }
    update.isActive = parsed.data.isActive
  }
  if (parsed.data.password) {
    update.passwordHash = await bcrypt.hash(parsed.data.password, 12)
  }

  if (Object.keys(update).length === 0) return NextResponse.json({ ok: true })

  const updated = await prisma.user.update({
    where: { id },
    data: update,
    select: { id: true, email: true, role: true, isActive: true },
  })
  return NextResponse.json({ ok: true, user: updated })
}

export async function DELETE(_req: NextRequest, ctx: { params: Promise<{ id: string }> }) {
  const session = await requireAdmin()
  if (!session) return NextResponse.json({ error: "Forbidden" }, { status: 403 })

  const { id } = await ctx.params
  if (id === session.user!.id) {
    return NextResponse.json({ error: "You cannot delete your own account" }, { status: 400 })
  }

  const target = await prisma.user.findUnique({ where: { id }, select: { role: true } })
  if (!target) return NextResponse.json({ error: "Not found" }, { status: 404 })

  if (target.role === "admin") {
    const adminCount = await prisma.user.count({ where: { role: "admin" } })
    if (adminCount <= 1) {
      return NextResponse.json({ error: "Cannot delete the last admin" }, { status: 400 })
    }
  }

  await prisma.user.delete({ where: { id } })
  return NextResponse.json({ ok: true })
}
