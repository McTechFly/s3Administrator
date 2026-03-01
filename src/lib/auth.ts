import NextAuth from "next-auth"
import Credentials from "next-auth/providers/credentials"
import { isCommunityEdition } from "@/lib/edition"
import { prisma } from "@/lib/db"

const LOCAL_USER = {
  id: "local",
  name: "Local User",
  email: "local@localhost",
  role: "admin",
}

const LOCAL_HOSTNAMES = new Set(["localhost", "127.0.0.1", "0.0.0.0", "::1"])
const COMMUNITY_SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000

function normalizeUrlEnvVar(key: "AUTH_URL" | "NEXT_PUBLIC_SITE_URL") {
  const raw = process.env[key]?.trim()
  if (!raw) return

  if (raw.startsWith("http://") || raw.startsWith("https://")) return

  const hostPort = raw.split("/")[0] || raw
  const hostname = hostPort.split(":")[0]?.toLowerCase() || ""
  const protocol = LOCAL_HOSTNAMES.has(hostname) ? "http" : "https"
  const normalized = `${protocol}://${raw}`
  process.env[key] = normalized

  console.warn(`${key} did not include protocol; normalized to ${normalized}`)
}

// Auth.js expects absolute URL env values. Normalize bare hostnames like "localhost".
normalizeUrlEnvVar("AUTH_URL")
normalizeUrlEnvVar("NEXT_PUBLIC_SITE_URL")

async function ensureLocalUser() {
  try {
    await prisma.user.upsert({
      where: { id: LOCAL_USER.id },
      update: {},
      create: {
        id: LOCAL_USER.id,
        name: LOCAL_USER.name,
        email: LOCAL_USER.email,
        role: LOCAL_USER.role,
      },
    })
  } catch {
    // If LOCAL_USER.email is already taken by another row, still guarantee
    // that the canonical local id exists for all FK writes.
    await prisma.user.upsert({
      where: { id: LOCAL_USER.id },
      update: {},
      create: {
        id: LOCAL_USER.id,
        name: LOCAL_USER.name,
        email: `local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}@community.local`,
        role: LOCAL_USER.role,
      },
    })
  }
}

type SessionLike = {
  user?: {
    id?: string
    role?: string
    name?: string | null
    email?: string | null
  }
  expires?: string
} | null

function hasSessionUserId(session: SessionLike): boolean {
  return typeof session?.user?.id === "string" && session.user.id.length > 0
}

function buildCommunitySession(): NonNullable<SessionLike> {
  return {
    user: {
      id: LOCAL_USER.id,
      role: LOCAL_USER.role,
      name: LOCAL_USER.name,
      email: LOCAL_USER.email,
    },
    expires: new Date(Date.now() + COMMUNITY_SESSION_TTL_MS).toISOString(),
  }
}

async function resolveCommunitySessionFallback(
  session: SessionLike
): Promise<SessionLike> {
  if (!isCommunityEdition()) return session
  if (hasSessionUserId(session)) return session
  await ensureLocalUser()
  return buildCommunitySession()
}

function buildCommunityAuth() {
  return NextAuth({
    providers: [
      Credentials({
        credentials: {},
        async authorize() {
          await ensureLocalUser()
          return LOCAL_USER
        },
      }),
    ],
    callbacks: {
      async jwt({ token }) {
        await ensureLocalUser()
        token.id = LOCAL_USER.id
        token.role = LOCAL_USER.role
        return token
      },
      async session({ session }) {
        await ensureLocalUser()
        session.user.id = LOCAL_USER.id
        session.user.role = LOCAL_USER.role
        return session
      },
    },
    session: { strategy: "jwt" },
    pages: { signIn: "/login" },
    secret: process.env.AUTH_SECRET || "community-edition-secret",
  })
}

type AuthModule = ReturnType<typeof buildCommunityAuth>

let _authModule: AuthModule | null = null

const _ready: Promise<AuthModule> = isCommunityEdition()
  ? Promise.resolve(buildCommunityAuth())
  : (async () => {
      try {
        const pkg = "@s3administrator/cloud/auth"
        const mod = await import(pkg)
        return mod as unknown as AuthModule
      } catch {
        console.warn("@s3administrator/cloud/auth not available - falling back to community auth")
        return buildCommunityAuth()
      }
    })()

_ready.then((m) => {
  _authModule = m
})

export const handlers = {
  GET: async (...args: Parameters<AuthModule["handlers"]["GET"]>) => {
    const m = await _ready
    return m.handlers.GET(...args)
  },
  POST: async (...args: Parameters<AuthModule["handlers"]["POST"]>) => {
    const m = await _ready
    return m.handlers.POST(...args)
  },
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const auth: AuthModule["auth"] = ((...args: any[]) => {
  // auth() has two calling conventions in Auth.js v5:
  //   1. auth()              → returns Promise<Session> (server components / route handlers)
  //   2. auth(callback)      → returns middleware function (proxy.ts)
  // For case 2 the outer call can happen at import time (before _ready settles),
  // so we return a wrapper that awaits _ready on each request.
  if (args.length > 0 && typeof args[0] === "function") {
    if (_authModule) {
      return (_authModule.auth as (...a: unknown[]) => unknown)(...args)
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (async (req: any, ctx: any) => {
      const m = await _ready
      const innerAuth = m.auth as (...a: unknown[]) => unknown
      const middleware = innerAuth(...args) as (r: unknown, c: unknown) => unknown
      return middleware(req, ctx)
    }) as unknown
  }

  // For plain auth() calls used by API routes/server components, ensure that
  // community mode always resolves to a local session even with no cookie.
  return (async () => {
    const m = _authModule ?? (await _ready)
    const innerAuth = m.auth as (...a: unknown[]) => Promise<SessionLike>
    const session = await innerAuth(...args)
    if (args.length === 0) {
      return resolveCommunitySessionFallback(session)
    }
    return session
  })() as unknown
}) as AuthModule["auth"]

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const signIn: AuthModule["signIn"] = (async (...args: any[]) => {
  const m = await _ready
  return (m.signIn as (...a: unknown[]) => unknown)(...args)
}) as AuthModule["signIn"]

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const signOut: AuthModule["signOut"] = (async (...args: any[]) => {
  const m = await _ready
  return (m.signOut as (...a: unknown[]) => unknown)(...args)
}) as AuthModule["signOut"]
