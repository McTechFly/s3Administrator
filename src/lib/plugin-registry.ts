type PluginStatus = {
  auth: boolean
  billing: boolean
  admin: boolean
  audit: boolean
  marketing: boolean
}

let cached: PluginStatus | null = null

function probeModule(name: string): boolean {
  try {
    require.resolve(name)
    return true
  } catch {
    return false
  }
}

export function getInstalledPlugins(): PluginStatus {
  if (cached) return cached

  const cloudInstalled = probeModule("@s3administrator/cloud")

  cached = {
    auth: cloudInstalled,
    billing: cloudInstalled,
    admin: cloudInstalled,
    audit: cloudInstalled,
    marketing: cloudInstalled,
  }

  return cached
}

export function isPluginInstalled(name: keyof PluginStatus): boolean {
  return getInstalledPlugins()[name]
}
