// Disabled intentionally: legacy dev auth bypass is not supported.
import { NextResponse } from "next/server"

export const POST = () => NextResponse.json({ error: "Not found" }, { status: 404 })
