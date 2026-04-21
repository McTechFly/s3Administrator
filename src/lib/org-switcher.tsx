"use client"

// Orgs are deferred — renders nothing.
export function OrgSwitcher(_props: {
  activeOrgSlug?: string | null
  collapsed?: boolean
  onOrgChange?: (slug: string | null) => void
}) {
  return null
}
