#!/usr/bin/env node
/**
 * Edition Sync Validator
 *
 * Compares cloud package manifests against the community-setup registry
 * to detect stub path mismatches, method inconsistencies, type mismatches,
 * and interface contract divergences BEFORE they cause build failures.
 *
 * Usage:
 *   node scripts/validate-edition-sync.mjs
 *   EDITION_SYNC_STRICT=true node scripts/validate-edition-sync.mjs
 *
 * Environment variables:
 *   CLOUD_PACKAGES_DIR  — path to cloud-packages root (default: auto-detected)
 *   COMMUNITY_SETUP_DIR — path to community-setup root (default: auto-detected)
 *   EDITION_SYNC_STRICT — if "true", exit 1 on any FAIL (default: warn only)
 */

import { existsSync, readFileSync } from "node:fs"
import { dirname, resolve, join } from "node:path"
import { pathToFileURL, fileURLToPath } from "node:url"

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)
const PROJECT_ROOT = resolve(__dirname, "..")

const CLOUD_PACKAGES = ["auth", "billing", "admin", "audit", "marketing"]

// ── Output helpers ──────────────────────────────────────────────────

let failCount = 0
let warnCount = 0

function fail(msg) {
  failCount++
  console.error(`  FAIL  ${msg}`)
}

function warn(msg) {
  warnCount++
  console.warn(`  WARN  ${msg}`)
}

function info(msg) {
  console.log(`  INFO  ${msg}`)
}

function heading(title) {
  console.log(`\n── ${title} ${"─".repeat(Math.max(0, 60 - title.length))}`)
}

// ── Data source resolution ──────────────────────────────────────────

function resolveCloudPackagesDir() {
  if (process.env.CLOUD_PACKAGES_DIR) {
    const dir = resolve(process.env.CLOUD_PACKAGES_DIR)
    if (existsSync(dir)) return dir
  }

  // Monorepo sibling: codex-cloud/cloud-packages/
  const sibling = resolve(PROJECT_ROOT, "..", "cloud-packages")
  if (existsSync(sibling)) return sibling

  return null
}

function resolveCommunitySetupDir() {
  if (process.env.COMMUNITY_SETUP_DIR) {
    const dir = resolve(process.env.COMMUNITY_SETUP_DIR)
    if (existsSync(dir)) return dir
  }

  // Workspace sibling: s3admin-folders/community-setup/
  const workspaceSibling = resolve(PROJECT_ROOT, "..", "..", "community-setup")
  if (existsSync(workspaceSibling)) return workspaceSibling

  // Local package
  const localPkg = resolve(PROJECT_ROOT, "packages", "community-setup")
  if (existsSync(localPkg)) return localPkg

  // Installed in node_modules
  const nmPkg = resolve(PROJECT_ROOT, "node_modules", "@s3administrator", "community-setup")
  if (existsSync(nmPkg)) return nmPkg

  return null
}

// ── Cloud manifest loader ───────────────────────────────────────────

async function loadCloudManifests(cloudDir) {
  const manifests = []

  for (const pkg of CLOUD_PACKAGES) {
    const manifestPath = join(cloudDir, pkg, pkg, "manifest.js")
    if (!existsSync(manifestPath)) {
      warn(`Cloud manifest not found: ${manifestPath}`)
      continue
    }

    try {
      const url = pathToFileURL(manifestPath).href
      const mod = await import(url)
      const manifest = mod.manifest
      if (manifest && manifest.name && manifest.stubs) {
        manifests.push(manifest)
      } else {
        warn(`Invalid manifest structure in ${pkg}`)
      }
    } catch (e) {
      warn(`Failed to load manifest for ${pkg}: ${e.message}`)
    }
  }

  return manifests
}

// ── Community registry parser (regex-based, no tsx needed) ──────────

function parseCommunityRegistry(communityDir) {
  const registryPath = join(communityDir, "src", "registry.ts")
  if (!existsSync(registryPath)) {
    throw new Error(`Community registry not found: ${registryPath}`)
  }

  const source = readFileSync(registryPath, "utf-8")

  // Extract the array body between COMMUNITY_STUBS: StubEntry[] = [ ... ]
  const arrayMatch = source.match(/COMMUNITY_STUBS\s*:\s*StubEntry\[\]\s*=\s*\[/s)
  if (!arrayMatch) {
    // Try without type annotation
    const altMatch = source.match(/COMMUNITY_STUBS\s*=\s*\[/s)
    if (!altMatch) {
      throw new Error("Could not find COMMUNITY_STUBS array in registry.ts")
    }
  }

  const entries = []

  // Match each object literal in the array
  // Handles: { type: "route", path: "...", methods: ["GET", "POST"] }
  //          { type: "page", path: "..." }
  //          { type: "lib-critical", path: "...", content: SOME_CONST }
  //          { type: "skip", path: "..." }
  const entryRegex = /\{\s*type:\s*"([^"]+)"\s*,\s*path:\s*"([^"]+)"(?:\s*,\s*methods:\s*\[([^\]]*)\])?(?:\s*,\s*content:\s*\w+)?\s*\}/g

  let match
  while ((match = entryRegex.exec(source)) !== null) {
    const entry = {
      type: match[1],
      path: match[2],
    }

    if (match[3]) {
      entry.methods = match[3]
        .split(",")
        .map((m) => m.trim().replace(/"/g, ""))
        .filter(Boolean)
    }

    entries.push(entry)
  }

  if (entries.length === 0) {
    throw new Error("Parsed 0 entries from community registry — check format")
  }

  return entries
}

// ── Community stub content reader (for interface checks) ────────────

function readCommunityStubContent(communityDir, stubName) {
  // Map known lib-critical paths to their stub content files
  const stubFiles = {
    "src/lib/plan-entitlements.ts": "plan-entitlements.ts",
    "src/lib/audit-logger.ts": "audit-logger.ts",
    "src/lib/server-metrics.ts": "server-metrics.ts",
    "src/lib/analytics-consent.ts": "analytics-consent.ts",
    "src/lib/seo.ts": "seo.ts",
    "src/lib/seo-landing-pages.ts": "seo-landing-pages.ts",
  }

  const fileName = stubFiles[stubName]
  if (!fileName) return null

  const filePath = join(communityDir, "src", "stubs", fileName)
  if (!existsSync(filePath)) return null

  const source = readFileSync(filePath, "utf-8")

  // Extract template literal content: export const X_CONTENT = `...`
  const templateMatch = source.match(/=\s*`([^`]*)`/)
  if (!templateMatch) return null

  return templateMatch[1]
}

// ── Cloud lib source reader ─────────────────────────────────────────

function resolveCloudLibSource(cloudDir, fromPath) {
  // fromPath is like "@s3administrator/billing/lib/plan-entitlements"
  // → cloud-packages/billing/billing/lib/plan-entitlements.ts
  const parts = fromPath.replace("@s3administrator/", "").split("/")
  const pkgName = parts[0]
  const relPath = parts.slice(1).join("/")

  const candidates = [
    join(cloudDir, pkgName, pkgName, `${relPath}.ts`),
    join(cloudDir, pkgName, pkgName, `${relPath}.tsx`),
    join(cloudDir, pkgName, pkgName, `${relPath}/index.ts`),
  ]

  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return readFileSync(candidate, "utf-8")
    }
  }

  return null
}

// ── Interface field extraction ──────────────────────────────────────

function extractInterfaceFields(source, interfaceName) {
  // Find interface block: interface Name { ... }
  const regex = new RegExp(
    `interface\\s+${interfaceName}\\s*\\{([^}]*)\\}`,
    "s"
  )
  const match = source.match(regex)
  if (!match) return null

  const body = match[1]
  const fields = []

  // Match field declarations: name: type or name?: type
  const fieldRegex = /^\s*(\w+)\??\s*:/gm
  let fieldMatch
  while ((fieldMatch = fieldRegex.exec(body)) !== null) {
    fields.push(fieldMatch[1])
  }

  return fields
}

// ── Exported name extraction ────────────────────────────────────────

function extractExportedNames(source) {
  const names = new Set()

  // export function name / export async function name
  for (const m of source.matchAll(
    /export\s+(?:async\s+)?function\s+(\w+)/g
  )) {
    names.add(m[1])
  }

  // export const/let/var name
  for (const m of source.matchAll(/export\s+(?:const|let|var)\s+(\w+)/g)) {
    names.add(m[1])
  }

  // export type name / export interface name
  for (const m of source.matchAll(
    /export\s+(?:type|interface)\s+(\w+)/g
  )) {
    names.add(m[1])
  }

  return names
}

// ── Build lookup maps ───────────────────────────────────────────────

function buildCloudPathMap(manifests) {
  // Map: path → { packageName, exports?, type?, from? }
  const map = new Map()

  for (const manifest of manifests) {
    for (const stub of manifest.stubs) {
      map.set(stub.path, {
        packageName: manifest.name,
        exports: stub.exports,
        type: stub.type || "route", // default to route if has exports
        from: stub.from,
        source: "stubs",
      })
    }

    if (manifest.libOverrides) {
      for (const lib of manifest.libOverrides) {
        map.set(lib.path, {
          packageName: manifest.name,
          type: "lib",
          from: lib.from,
          source: "libOverrides",
        })
      }
    }
  }

  return map
}

function buildCommunityPathMap(entries) {
  const map = new Map()
  for (const entry of entries) {
    map.set(entry.path, entry)
  }
  return map
}

// ── Validation checks ───────────────────────────────────────────────

function checkStubPathCoverage(cloudMap, communityMap) {
  heading("Check 1: Stub Path Coverage")

  let issues = 0

  // Paths in cloud but missing from community
  for (const [path, cloud] of cloudMap) {
    if (!communityMap.has(path)) {
      fail(`Missing in community: ${path} (from ${cloud.packageName})`)
      issues++
    }
  }

  // Paths in community (non-skip) but not in cloud
  for (const [path, community] of communityMap) {
    if (community.type === "skip") continue
    if (!cloudMap.has(path)) {
      warn(`Community-only entry (not in any cloud manifest): ${path}`)
      issues++
    }
  }

  if (issues === 0) {
    info("All paths are in sync")
  }
}

function checkMethodConsistency(cloudMap, communityMap) {
  heading("Check 2: Route Method Consistency")

  let issues = 0

  for (const [path, cloud] of cloudMap) {
    if (!cloud.exports) continue // not a route with methods

    const community = communityMap.get(path)
    if (!community || community.type !== "route" || !community.methods) continue

    const cloudMethods = [...cloud.exports].sort().join(", ")
    const communityMethods = [...community.methods].sort().join(", ")

    if (cloudMethods !== communityMethods) {
      fail(
        `Method mismatch for ${path}:\n` +
          `         Cloud:     ${cloudMethods}\n` +
          `         Community: ${communityMethods}`
      )
      issues++
    }
  }

  if (issues === 0) {
    info("All route methods match")
  }
}

function checkTypeConsistency(cloudMap, communityMap) {
  heading("Check 3: Stub Type Consistency")

  let issues = 0

  for (const [path, cloud] of cloudMap) {
    const community = communityMap.get(path)
    if (!community) continue // already reported in check 1

    let expectedCommunityTypes
    if (cloud.source === "libOverrides") {
      expectedCommunityTypes = ["lib-critical", "lib-empty"]
    } else if (cloud.type === "page") {
      expectedCommunityTypes = ["page"]
    } else if (cloud.type === "layout") {
      expectedCommunityTypes = ["layout"]
    } else {
      expectedCommunityTypes = ["route"]
    }

    if (!expectedCommunityTypes.includes(community.type)) {
      warn(
        `Type mismatch for ${path}: cloud=${cloud.source === "libOverrides" ? "lib" : cloud.type}, community=${community.type}`
      )
      issues++
    }
  }

  if (issues === 0) {
    info("All stub types are consistent")
  }
}

function checkInterfaceContracts(cloudDir, communityDir, cloudMap) {
  heading("Check 4: Interface Contracts (lib-critical)")

  let issues = 0
  let checked = 0

  // Find all lib override entries with cloud sources
  for (const [path, cloud] of cloudMap) {
    if (cloud.source !== "libOverrides") continue

    const communityContent = readCommunityStubContent(communityDir, path)
    if (!communityContent) continue // not a lib-critical stub

    const cloudSource = resolveCloudLibSource(cloudDir, cloud.from)
    if (!cloudSource) {
      warn(`Could not read cloud source for ${cloud.from}`)
      continue
    }

    checked++

    // Compare exported names
    const cloudExports = extractExportedNames(cloudSource)
    const communityExports = extractExportedNames(communityContent)

    const missingInCommunity = [...cloudExports].filter(
      (n) => !communityExports.has(n)
    )
    const extraInCommunity = [...communityExports].filter(
      (n) => !cloudExports.has(n)
    )

    if (missingInCommunity.length > 0) {
      fail(
        `${path}: community stub missing exports: ${missingInCommunity.join(", ")}`
      )
      issues++
    }

    if (extraInCommunity.length > 0) {
      warn(
        `${path}: community stub has extra exports: ${extraInCommunity.join(", ")}`
      )
      issues++
    }

    // Special check for PlanEntitlements interface
    if (path === "src/lib/plan-entitlements.ts") {
      const cloudFields = extractInterfaceFields(
        cloudSource,
        "PlanEntitlements"
      )
      const communityFields = extractInterfaceFields(
        communityContent,
        "PlanEntitlements"
      )

      if (cloudFields && communityFields) {
        const cloudSet = new Set(cloudFields)
        const communitySet = new Set(communityFields)

        const missingFields = cloudFields.filter((f) => !communitySet.has(f))
        const extraFields = communityFields.filter((f) => !cloudSet.has(f))

        if (missingFields.length > 0) {
          fail(
            `PlanEntitlements: community interface missing fields: ${missingFields.join(", ")}`
          )
          issues++
        }

        if (extraFields.length > 0) {
          warn(
            `PlanEntitlements: community interface has extra fields: ${extraFields.join(", ")}\n` +
              `         (This may cause issues if shared code starts using these fields)`
          )
          issues++
        }
      }
    }
  }

  if (checked === 0) {
    info("No lib-critical stubs to check (community stub content not available)")
  } else if (issues === 0) {
    info(`Checked ${checked} lib-critical interfaces — all consistent`)
  }
}

// ── Main ────────────────────────────────────────────────────────────

export async function validateEditionSync(options = {}) {
  console.log("\n╔══════════════════════════════════════════════════════════╗")
  console.log("║           Edition Sync Validation                      ║")
  console.log("╚══════════════════════════════════════════════════════════╝")

  const cloudDir =
    options.cloudPackagesDir || resolveCloudPackagesDir()
  const communityDir =
    options.communitySetupDir || resolveCommunitySetupDir()

  if (!cloudDir) {
    info("Cloud packages directory not found — skipping validation")
    info("Set CLOUD_PACKAGES_DIR to enable")
    console.log("")
    return true
  }

  if (!communityDir) {
    info("Community setup directory not found — skipping validation")
    info("Set COMMUNITY_SETUP_DIR to enable")
    console.log("")
    return true
  }

  info(`Cloud packages: ${cloudDir}`)
  info(`Community setup: ${communityDir}`)

  // Load data sources
  const manifests = await loadCloudManifests(cloudDir)
  if (manifests.length === 0) {
    warn("No cloud manifests loaded — skipping validation")
    console.log("")
    return true
  }

  info(`Loaded ${manifests.length} cloud manifests`)

  let communityEntries
  try {
    communityEntries = parseCommunityRegistry(communityDir)
  } catch (e) {
    warn(`Could not parse community registry: ${e.message}`)
    console.log("")
    return true
  }

  info(`Parsed ${communityEntries.length} community registry entries`)

  // Build lookup maps
  const cloudMap = buildCloudPathMap(manifests)
  const communityMap = buildCommunityPathMap(communityEntries)

  // Run all checks
  checkStubPathCoverage(cloudMap, communityMap)
  checkMethodConsistency(cloudMap, communityMap)
  checkTypeConsistency(cloudMap, communityMap)
  checkInterfaceContracts(cloudDir, communityDir, cloudMap)

  // Summary
  heading("Summary")

  if (failCount === 0 && warnCount === 0) {
    console.log("  ✓ All checks passed — community and cloud are in sync\n")
    return true
  }

  if (failCount > 0) {
    console.error(
      `  ✗ ${failCount} failure(s), ${warnCount} warning(s)\n`
    )
    return false
  }

  console.warn(`  ~ ${warnCount} warning(s), 0 failures\n`)
  return true
}

// ── CLI entry point ─────────────────────────────────────────────────

const isDirectRun = process.argv[1]
  ? resolve(process.argv[1]) === __filename
  : false

if (isDirectRun) {
  // Reset counters for CLI invocation
  failCount = 0
  warnCount = 0

  try {
    const passed = await validateEditionSync()
    const strict =
      process.env.CI === "true" ||
      process.env.EDITION_SYNC_STRICT === "true"

    if (!passed) {
      if (strict) {
        console.error(
          "Edition sync validation failed (strict mode). Fix the issues above.\n"
        )
        process.exit(1)
      } else {
        console.warn(
          "Edition sync validation has failures. Set EDITION_SYNC_STRICT=true to enforce.\n"
        )
      }
    }
  } catch (e) {
    console.error("Edition sync validation error:", e)
    process.exit(2)
  }
}
