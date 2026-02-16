import { auth } from "@/lib/auth"
import { isCommunityEdition } from "@/lib/edition"
import { NextResponse } from "next/server"
import { logSystemEvent } from "@/lib/system-logger"

const LOG_API_REQUESTS =
  (process.env.SYSTEM_LOG_API_REQUESTS ?? "true").toLowerCase() !== "false"

function isInternalTaskProcessRequest(pathname: string, request: { headers: Headers; nextUrl: URL }) {
  if (pathname !== "/api/tasks/process") return false

  const internalToken = (process.env.TASK_ENGINE_INTERNAL_TOKEN ?? "").trim()
  if (!internalToken) return false

  const requestToken = (request.headers.get("x-task-engine-token") ?? "").trim()
  if (requestToken !== internalToken) return false

  const requestedUserId = (request.nextUrl.searchParams.get("userId") ?? "").trim()
  return requestedUserId.length > 0
}

function shouldLogApiRequest(pathname: string) {
  if (!LOG_API_REQUESTS) return false
  if (!pathname.startsWith("/api/")) return false

  // Avoid writing a log entry for each log fetch request.
  if (pathname === "/api/admin/logs") return false

  return true
}

// In community mode, skip all auth checks — single-user, no login required.
const cloudMiddleware = auth((req) => {
  const { pathname } = req.nextUrl
  const isPublicAnalyticsEndpoint = pathname === "/api/analytics/events"

  if (shouldLogApiRequest(pathname)) {
    void logSystemEvent({
      source: "app",
      level: "info",
      message: "api_request",
      route: pathname,
      metadata: {
        method: req.method,
        authenticated: Boolean(req.auth?.user?.id),
      },
    })
  }

  // Analytics ingestion is intentionally public so unauthenticated pages
  // (like /login) can submit page/click events without auth redirects/CORS noise.
  if (isPublicAnalyticsEndpoint) {
    return NextResponse.next()
  }

  if (isInternalTaskProcessRequest(pathname, req)) {
    return NextResponse.next()
  }

  if (!req.auth) {
    void logSystemEvent({
      source: "app",
      level: "warn",
      message: "auth_required_redirect",
      route: pathname,
      metadata: {
        method: req.method,
      },
    })
    return NextResponse.redirect(new URL("/login", req.url))
  }

  if (
    (pathname.startsWith("/admin") || pathname.startsWith("/api/admin")) &&
    req.auth.user?.role !== "admin"
  ) {
    void logSystemEvent({
      source: "app",
      level: "warn",
      message: "admin_access_denied_redirect",
      route: pathname,
      metadata: {
        method: req.method,
      },
    })
    return NextResponse.redirect(new URL("/dashboard", req.url))
  }

  return NextResponse.next()
})

export default isCommunityEdition()
  ? () => NextResponse.next()
  : cloudMiddleware

export const config = {
  matcher: [
    "/dashboard/:path*",
    "/settings/:path*",
    "/billing/:path*",
    "/api/s3/:path*",
    "/api/tasks/:path*",
    "/api/analytics/:path*",
    "/api/audit/:path*",
    "/admin/:path*",
    "/api/admin/:path*",
  ],
}
