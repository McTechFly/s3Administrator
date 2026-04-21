import { NextRequest, NextResponse } from "next/server"
import { z } from "zod/v4"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"

const patchSchema = z.object({
  permissionLevel: z.enum(["read", "read_write", "full"]),
})

/**
 * PATCH /api/shares/[id] — only the owner (granter) may change the level.
 * DELETE /api/shares/[id] — the owner OR the recipient may revoke.
 */
export async function PATCH(
  req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const { id } = await ctx.params
  const body = await req.json().catch(() => null)
  const parsed = patchSchema.safeParse(body)
  if (!parsed.success) return NextResponse.json({ error: "Invalid input" }, { status: 400 })

  const share = await prisma.bucketShare.findUnique({ where: { id } })
  if (!share) return NextResponse.json({ error: "Not found" }, { status: 404 })
  if (share.ownerUserId !== session.user.id) {
    return NextResponse.json({ error: "Forbidden" }, { status: 403 })
  }

  const updated = await prisma.bucketShare.update({
    where: { id },
    data: { permissionLevel: parsed.data.permissionLevel },
  })
  return NextResponse.json({ ok: true, share: updated })
}

export async function DELETE(
  _req: NextRequest,
  ctx: { params: Promise<{ id: string }> },
) {
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const { id } = await ctx.params
  const share = await prisma.bucketShare.findUnique({ where: { id } })
  if (!share) return NextResponse.json({ error: "Not found" }, { status: 404 })
  if (
    share.ownerUserId !== session.user.id &&
    share.targetUserId !== session.user.id
  ) {
    return NextResponse.json({ error: "Forbidden" }, { status: 403 })
  }

  await prisma.bucketShare.delete({ where: { id } })
  return NextResponse.json({ ok: true })
}
