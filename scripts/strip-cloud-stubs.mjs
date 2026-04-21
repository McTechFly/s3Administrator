#!/usr/bin/env node
/**
 * One-shot migration: replace remaining `@s3administrator/cloud/*` stub files with
 * local self-hosted no-op implementations so the project can type-check and build
 * without the private cloud package.
 *
 * Kept intentionally simple — each replacement produces a placeholder that either:
 *   - renders a "Not available" notice (pages)
 *   - returns 404 (API routes)
 *   - exports inert shims (lib files)
 */
import { readFileSync, writeFileSync } from "node:fs"
import { execSync } from "node:child_process"
import { extname, dirname, relative } from "node:path"

const ROOT = process.cwd()
const STUB_MARK = '"@s3administrator/cloud'

function findStubFiles() {
  const out = execSync(
    `grep -lR --include='*.ts' --include='*.tsx' '${STUB_MARK}' src || true`,
    { cwd: ROOT, encoding: "utf8" },
  )
  return out.split("\n").filter(Boolean)
}

function isAllStub(content) {
  // A file is a "pure stub" if every non-empty, non-comment line is either a re-export
  // from @s3administrator/cloud/... or the generated marker / `export const`.
  const lines = content.split("\n").map((l) => l.trim())
  let sawCloud = false
  for (const l of lines) {
    if (!l) continue
    if (l.startsWith("//")) continue
    if (l.startsWith("export") && l.includes("@s3administrator/cloud")) {
      sawCloud = true
      continue
    }
    if (l.startsWith('export const dynamic')) continue
    return false
  }
  return sawCloud
}

function classify(filePath) {
  if (filePath.includes("src/app/api/")) return "api-route"
  if (filePath.endsWith("layout.tsx")) return "layout"
  if (filePath.endsWith("page.tsx")) return "page"
  return "lib"
}

function pageReplacement(filePath) {
  const isMarketing = filePath.includes("(marketing)")
  const isTeam = filePath.includes("teams")
  const isBilling = filePath.includes("billing") || filePath.includes("pricing") || filePath.includes("subscriptions")
  const isAuditOrAdmin = filePath.includes("admin/") || filePath.includes("audit-logs")

  let title = "Not available"
  let desc = "This page is not available in the self-hosted multi-user edition."

  if (isBilling) {
    title = "Billing disabled"
    desc = "Billing is handled by the hosting administrator in the self-hosted edition."
  } else if (isTeam) {
    title = "Teams not available"
    desc = "Organizations / teams are not enabled in this edition. Use direct user-to-user bucket shares instead."
  } else if (isAuditOrAdmin) {
    title = "Coming soon"
    desc = "This admin page is not yet implemented in the self-hosted multi-user edition."
  } else if (isMarketing) {
    title = "Self-hosted"
    desc = "This marketing page is disabled in the self-hosted edition."
  }

  return `import Link from "next/link"

export default function NotAvailablePage() {
  return (
    <div className="mx-auto max-w-xl px-6 py-16 text-center">
      <h1 className="text-2xl font-semibold mb-2">${title}</h1>
      <p className="text-sm text-muted-foreground mb-6">${desc}</p>
      <Link href="/dashboard" className="text-sm text-primary hover:underline">
        Back to dashboard
      </Link>
    </div>
  )
}
`
}

function layoutReplacement() {
  return `export default function PassthroughLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>
}
`
}

function apiRouteReplacement(content) {
  // Figure out which verbs the stub was re-exporting.
  const verbs = []
  for (const v of ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]) {
    const re = new RegExp(`\\b${v}\\b`)
    if (re.test(content)) verbs.push(v)
  }
  const uniqueVerbs = [...new Set(verbs)]
  if (uniqueVerbs.length === 0) uniqueVerbs.push("GET")

  const exports = uniqueVerbs
    .map(
      (v) =>
        `export const ${v} = () => NextResponse.json({ error: "Not available in this edition" }, { status: 404 })`,
    )
    .join("\n")

  return `import { NextResponse } from "next/server"

${exports}
`
}

function libReplacement(filePath) {
  const base = filePath.split("/").pop()
  // For the very few lib files still stubbed, emit harmless no-op shims.
  // Most are marketing/billing/audit helpers that are no longer used after
  // the dashboard stops importing them.
  return `/**
 * Local no-op replacement for the cloud stub: ${base}
 * The feature this helper backed is not available in the self-hosted edition.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
export const __unavailable = true
const handler = {
  get: (_t: any, prop: string) => {
    return (..._args: any[]) => {
      throw new Error(
        \`[\${'${base}'}] not available in self-hosted edition — called "\${prop}"\`,
      )
    }
  },
}
const proxy: any = new Proxy({}, handler)
export default proxy
`
}

const files = findStubFiles()
const skip = new Set([
  // Files that already have real local implementations (dynamic import / probe, not static stub)
  "src/lib/auth.ts",
  "src/lib/plugin-registry.ts",
])

let replaced = 0
for (const rel of files) {
  if (skip.has(rel)) continue
  const abs = `${ROOT}/${rel}`
  const content = readFileSync(abs, "utf8")
  if (!isAllStub(content)) {
    // Mixed file: do not auto-replace, user may want to handle manually.
    console.log("SKIP (mixed content):", rel)
    continue
  }
  const kind = classify(rel)
  let out
  if (kind === "page") out = pageReplacement(rel)
  else if (kind === "layout") out = layoutReplacement()
  else if (kind === "api-route") out = apiRouteReplacement(content)
  else out = libReplacement(rel)
  writeFileSync(abs, out, "utf8")
  replaced++
}

console.log(`\nReplaced ${replaced} stub files out of ${files.length} candidates.`)
