"use client"

import Link from "next/link"
import { usePathname, useSearchParams } from "next/navigation"
import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import {
  TooltipProvider,
} from "@/components/ui/tooltip"
import {
  Database,
  LayoutDashboard,
  HardDrive,
  FileSearch,
  ChevronsLeft,
  ChevronsRight,
  ExternalLink,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { ThemeSwitcher } from "@/components/theme-switcher"
import { SidebarBucketList } from "@/components/dashboard/sidebar-bucket-list"

interface BucketStatsItem {
  name: string
  totalSize: number
  fileCount: number
  credentialId: string
}

interface Bucket {
  name: string
  credentialId: string
}

interface Credential {
  id: string
  label: string
}

export function DemoSidebar({
  signupUrl,
  className,
  collapsible = true,
}: {
  signupUrl: string
  className?: string
  collapsible?: boolean
}) {
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const [isCollapsed, setIsCollapsed] = useState(false)
  const sidebarCollapsed = collapsible && isCollapsed
  const isOverviewActive =
    pathname === "/demo" &&
    !searchParams.get("bucket") &&
    !searchParams.get("prefix")

  const { data: buckets = [], isLoading: bucketsLoading } = useQuery<Bucket[]>({
    queryKey: ["demo-buckets"],
    queryFn: async () => {
      const res = await fetch("/api/demo/s3/buckets?all=true")
      if (!res.ok) return []
      const data = await res.json()
      return data.buckets ?? []
    },
  })

  const { data: credentials = [] } = useQuery<Credential[]>({
    queryKey: ["demo-credentials"],
    queryFn: async () => {
      const res = await fetch("/api/demo/s3/credentials")
      if (!res.ok) return []
      return res.json()
    },
  })

  const { data: bucketStats = [] } = useQuery<BucketStatsItem[]>({
    queryKey: ["demo-bucket-stats"],
    queryFn: async () => {
      const res = await fetch("/api/demo/s3/bucket-stats?all=true")
      if (!res.ok) return []
      const data = await res.json()
      return data.buckets ?? []
    },
  })

  return (
    <TooltipProvider>
      <div
        className={cn(
          "relative z-50 flex h-full min-w-0 flex-col border-r bg-muted/30 transition-[width] duration-200",
          sidebarCollapsed ? "w-20 lg:w-20" : "w-80 lg:w-96",
          className
        )}
      >
        <div
          className={cn(
            "flex h-14 items-center border-b",
            sidebarCollapsed ? "justify-between px-2" : "justify-between px-4"
          )}
        >
          <Link
            href="/demo"
            className={cn(
              "flex items-center font-semibold",
              sidebarCollapsed ? "justify-center px-1" : "gap-2"
            )}
          >
            <Database className="h-5 w-5" />
            {!sidebarCollapsed && <span>S3 Administrator</span>}
          </Link>
          {sidebarCollapsed ? (
            collapsible && (
              <Button
                variant="ghost"
                size="icon"
                className="h-8 w-8"
                onClick={() => setIsCollapsed(false)}
                title="Expand sidebar"
                aria-label="Expand sidebar"
              >
                <ChevronsRight className="h-4 w-4" />
              </Button>
            )
          ) : (
            <div className="flex items-center gap-1">
              <ThemeSwitcher />
              {collapsible && (
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-8 w-8"
                  onClick={() => setIsCollapsed(true)}
                  title="Collapse sidebar"
                  aria-label="Collapse sidebar"
                >
                  <ChevronsLeft className="h-4 w-4" />
                </Button>
              )}
            </div>
          )}
        </div>

        {sidebarCollapsed ? (
          <div className="flex-1 overflow-y-auto p-2">
            <div className="space-y-1">
              <Link
                href="/demo"
                title="Overview"
                className={cn(
                  "flex justify-center rounded-md px-2 py-2 hover:bg-accent",
                  isOverviewActive && "bg-accent"
                )}
              >
                <LayoutDashboard className="h-4 w-4" />
              </Link>
              <Link
                href="/demo/search"
                title="Search All Files"
                className={cn(
                  "flex justify-center rounded-md px-2 py-2 hover:bg-accent",
                  pathname === "/demo/search" && "bg-accent"
                )}
              >
                <FileSearch className="h-4 w-4" />
              </Link>
            </div>
          </div>
        ) : (
          <SidebarBucketList
            buckets={buckets}
            bucketsLoading={bucketsLoading}
            credentials={credentials}
            bucketStats={bucketStats}
            isSyncingAll={false}
            syncIssueByBucketKey={{}}
            onSyncAll={() => {}}
            onOpenSettings={() => {}}
            basePath="/demo"
          />
        )}

        <Separator />
        {sidebarCollapsed ? (
          <div className="space-y-1 p-2">
            <div className="flex justify-center pb-1">
              <ThemeSwitcher />
            </div>
          </div>
        ) : (
          <div className="shrink-0 space-y-0.5 p-2 pb-4 sm:space-y-1 sm:p-3">
            <Link
              href="/demo"
              className={cn(
                "flex items-center gap-2 rounded-md px-2 py-1 text-xs hover:bg-accent sm:py-1.5 sm:text-sm",
                isOverviewActive && "bg-accent"
              )}
            >
              <LayoutDashboard className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
              Overview
            </Link>

            <Link
              href="/demo/search"
              className={cn(
                "flex items-center gap-2 rounded-md px-2 py-1 text-xs hover:bg-accent sm:py-1.5 sm:text-sm",
                pathname === "/demo/search" && "bg-accent"
              )}
            >
              <FileSearch className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
              Search All Files
            </Link>

            <Separator className="my-2" />

            <a
              href={signupUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 rounded-md bg-primary px-2 py-1.5 text-xs font-medium text-primary-foreground hover:bg-primary/90 sm:py-2 sm:text-sm"
            >
              <ExternalLink className="h-3.5 w-3.5 sm:h-4 sm:w-4" />
              Try S3 Administrator
            </a>
          </div>
        )}
      </div>
    </TooltipProvider>
  )
}
