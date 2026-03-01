import { NextResponse } from "next/server"
import { DEMO_WRITE_BLOCKED } from "@/lib/demo"

const blocked = () =>
  NextResponse.json({ error: DEMO_WRITE_BLOCKED }, { status: 403 })

export const POST = blocked
export const PUT = blocked
export const DELETE = blocked
export const PATCH = blocked
