/**
 * Auth mode selection for the self-hosted build.
 *
 * - `single`: legacy community mode — no login, one shared "local" user is admin.
 * - `multi` : self-hosted multi-user — email/password auth, roles, admin panel,
 *             per-user buckets and user→user bucket shares.
 *
 * Enable multi-user mode by setting `AUTH_MODE=multi` in the environment.
 * Defaults to `single` to preserve upstream community behavior.
 */

export type AuthMode = "single" | "multi"

export function getAuthMode(): AuthMode {
  const v = (process.env.AUTH_MODE ?? "").trim().toLowerCase()
  if (v === "multi" || v === "multiuser" || v === "multi-user") return "multi"
  return "single"
}

export function isMultiUserMode(): boolean {
  return getAuthMode() === "multi"
}

export function isSingleUserMode(): boolean {
  return getAuthMode() === "single"
}
