/** Local no-op audit-logger. */
/* eslint-disable @typescript-eslint/no-unused-vars */

export type RequestContext = {
  ipAddress: string | null
  userAgent: string | null
}

export function getRequestContext(_req?: unknown): RequestContext {
  return { ipAddress: null, userAgent: null }
}

export async function logUserAuditAction(_args: unknown): Promise<void> {
  // no-op in self-hosted edition
}

export async function logAuditAction(_args: unknown): Promise<void> {
  // no-op in self-hosted edition
}

const proxy = { getRequestContext, logUserAuditAction, logAuditAction }
export default proxy
