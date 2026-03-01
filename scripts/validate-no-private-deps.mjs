#!/usr/bin/env node
/**
 * Validates that package.json does not contain any @s3administrator/* packages.
 *
 * Cloud-only packages must ONLY be installed via the Dockerfile's conditional
 * install block, never declared in package.json. Having them in package.json
 * breaks community builds because npm ci tries to fetch them from the public
 * registry.
 */

import { readFileSync } from "node:fs"
import { resolve, dirname } from "node:path"
import { fileURLToPath } from "node:url"

const __dirname = dirname(fileURLToPath(import.meta.url))
const PROJECT_ROOT = resolve(__dirname, "..")

const PRIVATE_SCOPE = "@s3administrator/"

const pkg = JSON.parse(
  readFileSync(resolve(PROJECT_ROOT, "package.json"), "utf-8")
)

const sections = ["dependencies", "devDependencies", "optionalDependencies"]
const violations = []

for (const section of sections) {
  const deps = pkg[section]
  if (!deps) continue
  for (const name of Object.keys(deps)) {
    if (name.startsWith(PRIVATE_SCOPE)) {
      violations.push({ section, name, version: deps[name] })
    }
  }
}

if (violations.length > 0) {
  console.error(
    "\n  ERROR: package.json contains private @s3administrator/* packages.\n"
  )
  console.error(
    "  These packages must NOT be in package.json — they are installed\n" +
      "  conditionally in docker/Dockerfile for cloud builds only.\n" +
      "  Having them here breaks community builds (npm ci → E404).\n"
  )
  for (const v of violations) {
    console.error(`    ${v.section}: "${v.name}": "${v.version}"`)
  }
  console.error(
    "\n  Fix: Remove the above entries from package.json and run npm install.\n"
  )
  process.exit(1)
}

console.log("  OK  No @s3administrator/* packages in package.json")
