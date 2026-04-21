/** Local no-op plan-entitlements. No plan gating in self-hosted edition. */

export type PlanEntitlements = {
  slug: string
  source: string
  storageLimitBytes: number
  maxBuckets: number
  maxCredentials: number
  bucketLimit: number
  fileLimit: number
  canShareBuckets: boolean
  canScheduleTasks: boolean
  canUseBackups: boolean
}

const UNLIMITED: PlanEntitlements = {
  slug: "self-hosted",
  source: "self-hosted",
  storageLimitBytes: Number.POSITIVE_INFINITY,
  maxBuckets: Number.POSITIVE_INFINITY,
  maxCredentials: Number.POSITIVE_INFINITY,
  bucketLimit: Number.POSITIVE_INFINITY,
  fileLimit: Number.POSITIVE_INFINITY,
  canShareBuckets: true,
  canScheduleTasks: true,
  canUseBackups: true,
}

export async function getUserPlanEntitlements(_userId: string): Promise<PlanEntitlements> {
  return UNLIMITED
}

export default { getUserPlanEntitlements }
