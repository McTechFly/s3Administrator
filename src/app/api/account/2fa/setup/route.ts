import { NextResponse } from "next/server"
import QRCode from "qrcode"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { isMultiUserMode } from "@/lib/auth-mode"
import { buildOtpauthUrl, generateTotpSecret } from "@/lib/totp"

/**
 * Starts 2FA enrollment: generates a fresh secret (stored but not yet enabled),
 * and returns an otpauth:// URL + QR code data URL the client can display.
 * The user must then submit a valid TOTP code to /api/account/2fa/enable.
 */
export async function POST() {
  if (!isMultiUserMode()) {
    return NextResponse.json({ error: "Not available in single-user mode" }, { status: 404 })
  }
  const session = await auth()
  if (!session?.user?.id) return NextResponse.json({ error: "Unauthorized" }, { status: 401 })

  const user = await prisma.user.findUnique({
    where: { id: session.user.id },
    select: { email: true, totpEnabled: true },
  })
  if (!user) return NextResponse.json({ error: "Not found" }, { status: 404 })
  if (user.totpEnabled) {
    return NextResponse.json(
      { error: "Two-factor authentication is already enabled. Disable it first to re-enroll." },
      { status: 409 },
    )
  }

  const secret = generateTotpSecret()
  // Store the pending secret immediately; it only becomes active when
  // totpEnabled is set to true after verification.
  await prisma.user.update({
    where: { id: session.user.id },
    data: { totpSecret: secret, totpEnabled: false, totpBackupCodes: null },
  })

  const otpauthUrl = buildOtpauthUrl(secret, user.email)
  const qrDataUrl = await QRCode.toDataURL(otpauthUrl, { errorCorrectionLevel: "M", margin: 1 })

  return NextResponse.json({ secret, otpauthUrl, qrDataUrl })
}
