import * as OTPAuth from "otpauth"
import bcrypt from "bcryptjs"
import { randomBytes } from "node:crypto"

const APP_ISSUER = "s3Administrator"

export function generateTotpSecret(): string {
  // 20 bytes → 32 base32 chars, recommended by RFC 6238.
  return new OTPAuth.Secret({ size: 20 }).base32
}

export function buildTotp(secret: string, accountLabel: string) {
  return new OTPAuth.TOTP({
    issuer: APP_ISSUER,
    label: accountLabel,
    algorithm: "SHA1",
    digits: 6,
    period: 30,
    secret: OTPAuth.Secret.fromBase32(secret),
  })
}

export function buildOtpauthUrl(secret: string, accountLabel: string): string {
  return buildTotp(secret, accountLabel).toString()
}

/**
 * Validates a 6-digit TOTP code with a ±1 time-step tolerance (30s).
 */
export function verifyTotpCode(secret: string, code: string): boolean {
  const clean = (code ?? "").trim().replace(/\s+/g, "")
  if (!/^\d{6}$/.test(clean)) return false
  const totp = buildTotp(secret, "verification")
  const delta = totp.validate({ token: clean, window: 1 })
  return delta !== null
}

export function generateBackupCodes(count = 10): string[] {
  const codes: string[] = []
  for (let i = 0; i < count; i++) {
    // 10 hex chars → formatted as XXXXX-XXXXX for readability.
    const raw = randomBytes(5).toString("hex").toUpperCase()
    codes.push(`${raw.slice(0, 5)}-${raw.slice(5, 10)}`)
  }
  return codes
}

export async function hashBackupCodes(codes: string[]): Promise<string[]> {
  return Promise.all(codes.map((code) => bcrypt.hash(code.replace(/-/g, ""), 10)))
}

export async function consumeBackupCode(
  storedHashesJson: string | null | undefined,
  submitted: string
): Promise<{ ok: boolean; remaining: string | null }> {
  if (!storedHashesJson) return { ok: false, remaining: storedHashesJson ?? null }
  let hashes: string[]
  try {
    const parsed = JSON.parse(storedHashesJson) as unknown
    if (!Array.isArray(parsed)) return { ok: false, remaining: storedHashesJson }
    hashes = parsed.filter((h): h is string => typeof h === "string")
  } catch {
    return { ok: false, remaining: storedHashesJson }
  }

  const candidate = submitted.trim().replace(/-/g, "").toUpperCase()
  if (!/^[A-F0-9]{10}$/.test(candidate)) return { ok: false, remaining: storedHashesJson }

  for (let i = 0; i < hashes.length; i++) {
    // eslint-disable-next-line no-await-in-loop
    const match = await bcrypt.compare(candidate, hashes[i]!)
    if (match) {
      const next = hashes.filter((_, idx) => idx !== i)
      return { ok: true, remaining: JSON.stringify(next) }
    }
  }
  return { ok: false, remaining: storedHashesJson }
}
