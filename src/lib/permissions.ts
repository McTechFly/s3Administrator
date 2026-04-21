/**
 * Local self-hosted permissions helpers.
 */

export type Role = "admin" | "user"

export function isAdmin(role: string | null | undefined): boolean {
  return role === "admin"
}

export function assertAdmin(
  role: string | null | undefined,
): asserts role is "admin" {
  if (!isAdmin(role)) throw new Error("forbidden: admin role required")
}
