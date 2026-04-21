/**
 * Local self-hosted org-context no-op. Orgs are deferred.
 */

export type OrgContext = {
  organizationId: string | null
  organizationSlug: string | null
  role: "owner" | "admin" | "member" | "viewer" | null
}

export async function getOrgContext(): Promise<OrgContext> {
  return { organizationId: null, organizationSlug: null, role: null }
}

export async function requireOrgContext(): Promise<OrgContext> {
  return getOrgContext()
}
