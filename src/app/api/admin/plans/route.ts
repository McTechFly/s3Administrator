import { NextResponse } from "next/server"

export const GET = () => NextResponse.json({ error: "Not available in this edition" }, { status: 404 })
export const POST = () => NextResponse.json({ error: "Not available in this edition" }, { status: 404 })
