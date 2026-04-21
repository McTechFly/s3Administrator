import { NextResponse } from "next/server"

export const DELETE = () => NextResponse.json({ error: "Not available in this edition" }, { status: 404 })
