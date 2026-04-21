import { NextResponse } from "next/server"

export const POST = () => NextResponse.json({ error: "Not available in this edition" }, { status: 404 })
