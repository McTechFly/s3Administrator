"use client"

import { useSearchParams, useRouter } from "next/navigation"
import { useInfiniteQuery, useQuery } from "@tanstack/react-query"
import { useState, useCallback, Suspense, useMemo, useEffect, useRef } from "react"
import { Topbar } from "@/components/dashboard/topbar"
import { FileBrowser } from "@/components/dashboard/file-browser"
import { GalleryBrowser } from "@/components/dashboard/gallery-browser"
import { GalleryLightbox } from "@/components/dashboard/gallery-lightbox"
import { FilePreviewDialog } from "@/components/dashboard/file-preview-dialog"
import { getPreviewType } from "@/lib/media"
import { MultiSelectToolbar } from "@/components/dashboard/multi-select-toolbar"
import { EmptyState } from "@/components/dashboard/empty-state"
import { Loader2, Database } from "lucide-react"
import { toast } from "sonner"
import type { GalleryResponse, S3Object } from "@/types"

const API_PREFIX = "/api/demo/s3"
const BASE_PATH = "/demo"

interface Credential {
  id: string
}

interface BucketRef {
  name: string
  credentialId: string
}

function getBaseNameFromKey(key: string, isFolder: boolean): string {
  const normalized = isFolder && key.endsWith("/") ? key.slice(0, -1) : key
  const parts = normalized.split("/").filter(Boolean)
  return parts[parts.length - 1] ?? normalized
}

function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof HTMLElement)) return false
  if (target.isContentEditable) return true
  return Boolean(target.closest("input, textarea, [contenteditable='true']"))
}

function DemoDashboardContent() {
  const searchParams = useSearchParams()
  const router = useRouter()

  const bucket = searchParams.get("bucket") || ""
  const prefix = searchParams.get("prefix") || ""
  const credentialId = searchParams.get("credentialId") || undefined

  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set())
  const [searchQuery, setSearchQuery] = useState("")
  const [sortBy, setSortBy] = useState<"name" | "size" | "lastModified">(() => {
    if (typeof window !== "undefined") {
      const saved = localStorage.getItem("s3admin:demo:sortBy")
      if (saved === "name" || saved === "size" || saved === "lastModified") return saved
    }
    return "name"
  })
  const [sortDir, setSortDir] = useState<"asc" | "desc">(() => {
    if (typeof window !== "undefined") {
      const saved = localStorage.getItem("s3admin:demo:sortDir")
      if (saved === "asc" || saved === "desc") return saved
    }
    return "asc"
  })
  const [viewMode, setViewMode] = useState<"list" | "gallery">(() => {
    if (typeof window !== "undefined") {
      const saved = localStorage.getItem("s3admin:demo:viewMode")
      if (saved === "list" || saved === "gallery") return saved
    }
    return "list"
  })
  const [showVersions, setShowVersions] = useState(false)
  const [lightboxIndex, setLightboxIndex] = useState<number | null>(null)
  const selectionAnchorRef = useRef<string | null>(null)
  const [previewFile, setPreviewFile] = useState<S3Object | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ["demo-objects", bucket, prefix, credentialId],
    queryFn: async () => {
      if (!bucket) return { folders: [], files: [] }
      const params = new URLSearchParams({ bucket })
      if (prefix) params.set("prefix", prefix)
      if (credentialId) params.set("credentialId", credentialId)
      const res = await fetch(`${API_PREFIX}/objects?${params}`)
      if (!res.ok) throw new Error("Failed to load objects")
      return res.json() as Promise<{ folders: S3Object[]; files: S3Object[] }>
    },
    enabled: !!bucket,
  })

  const {
    data: galleryData,
    isLoading: galleryLoading,
    isFetchingNextPage,
    fetchNextPage,
    hasNextPage,
  } = useInfiniteQuery<GalleryResponse>({
    queryKey: ["demo-gallery", bucket, prefix, credentialId],
    initialPageParam: null as string | null,
    queryFn: async ({ pageParam }) => {
      const cursor = typeof pageParam === "string" ? pageParam : null
      const params = new URLSearchParams({
        bucket,
        limit: "25",
        mediaType: "all",
      })
      if (prefix) params.set("prefix", prefix)
      if (credentialId) params.set("credentialId", credentialId)
      if (cursor) params.set("cursor", cursor)
      const res = await fetch(`${API_PREFIX}/gallery?${params}`)
      if (!res.ok) throw new Error("Failed to load gallery")
      return res.json() as Promise<GalleryResponse>
    },
    getNextPageParam: (lastPage) => (lastPage.nextCursor ? lastPage.nextCursor : undefined),
    enabled: !!bucket && viewMode === "gallery",
  })

  const { data: credentials = [], isLoading: credentialsLoading } = useQuery<Credential[]>({
    queryKey: ["demo-credentials-check"],
    queryFn: async () => {
      const res = await fetch(`${API_PREFIX}/credentials`)
      if (!res.ok) return []
      return res.json()
    },
    enabled: !bucket,
  })

  const { data: availableBuckets = [], isLoading: bucketsLoading } = useQuery<BucketRef[]>({
    queryKey: ["demo-buckets-check"],
    queryFn: async () => {
      const res = await fetch(`${API_PREFIX}/buckets?all=true`)
      if (!res.ok) return []
      const payload = await res.json()
      return (payload?.buckets ?? []) as BucketRef[]
    },
    enabled: !bucket,
  })

  const { data: bucketSettings } = useQuery<{
    settings: { versioning: { status: string } }
  }>({
    queryKey: ["demo-bucket-settings", credentialId ?? "", bucket],
    queryFn: async () => {
      const params = new URLSearchParams({ bucket })
      if (credentialId) params.set("credentialId", credentialId)
      const res = await fetch(`${API_PREFIX}/bucket-settings?${params}`)
      if (!res.ok) return { settings: { versioning: { status: "unversioned" } } }
      return res.json()
    },
    enabled: !!bucket,
  })

  const versioningEnabled =
    bucketSettings?.settings?.versioning?.status === "enabled" ||
    bucketSettings?.settings?.versioning?.status === "suspended"

  interface VersionsListResponse {
    versions: {
      key: string
      versionId: string
      size: number
      lastModifiedUtc: string
      isLatest: boolean
      isDeleteMarker: boolean
    }[]
    pagination: { hasMore: boolean }
  }

  const { data: versionsData } = useQuery<VersionsListResponse>({
    queryKey: ["demo-versions-list", bucket, prefix, credentialId],
    queryFn: async () => {
      const params = new URLSearchParams({ bucket, limit: "500" })
      if (prefix) params.set("prefix", prefix)
      if (credentialId) params.set("credentialId", credentialId)
      const res = await fetch(`${API_PREFIX}/versions/list?${params}`)
      if (!res.ok) throw new Error("Failed to load versions")
      return res.json() as Promise<VersionsListResponse>
    },
    enabled: !!bucket && showVersions && versioningEnabled,
  })

  const allItems = useMemo(() => {
    const base: S3Object[] = [...(data?.folders ?? []), ...(data?.files ?? [])]
    if (!showVersions || !versionsData?.versions) return base

    const extraItems: S3Object[] = []
    for (const v of versionsData.versions) {
      if (v.isLatest && !v.isDeleteMarker) continue
      extraItems.push({
        key: v.key,
        size: v.size,
        lastModified: v.lastModifiedUtc,
        isFolder: false,
        versionId: v.versionId,
        isLatest: v.isLatest,
        isDeleteMarker: v.isDeleteMarker,
      })
    }

    const withVersionInfo = base.map((item) => {
      if (item.isFolder) return item
      const currentVersion = versionsData.versions.find(
        (v) => v.key === item.key && v.isLatest && !v.isDeleteMarker
      )
      if (currentVersion) {
        return { ...item, versionId: currentVersion.versionId, isLatest: true }
      }
      return item
    })

    return [...withVersionInfo, ...extraItems]
  }, [data?.files, data?.folders, showVersions, versionsData])

  const galleryItems = useMemo(
    () => galleryData?.pages.flatMap((page) => page.items) ?? [],
    [galleryData?.pages]
  )

  const filteredItems = useMemo(() => {
    if (!searchQuery) return allItems
    return allItems.filter((item) =>
      item.key.toLowerCase().includes(searchQuery.toLowerCase())
    )
  }, [allItems, searchQuery])

  const sortedItems = useMemo(() => [...filteredItems].sort((a, b) => {
    if (a.isFolder && !b.isFolder) return -1
    if (!a.isFolder && b.isFolder) return 1

    let cmp = 0
    if (sortBy === "name") {
      cmp = a.key.localeCompare(b.key)
    } else if (sortBy === "size") {
      cmp = a.size - b.size
    } else {
      cmp =
        new Date(a.lastModified).getTime() -
        new Date(b.lastModified).getTime()
    }
    return sortDir === "asc" ? cmp : -cmp
  }), [filteredItems, sortBy, sortDir])

  const filteredGalleryItems = useMemo(() => {
    if (!searchQuery) return galleryItems
    return galleryItems.filter((item) =>
      item.key.toLowerCase().includes(searchQuery.toLowerCase())
    )
  }, [galleryItems, searchQuery])

  const sortedGalleryItems = useMemo(() => [...filteredGalleryItems].sort((a, b) => {
    if (a.isFolder && !b.isFolder) return -1
    if (!a.isFolder && b.isFolder) return 1

    let cmp = 0
    if (sortBy === "name") {
      cmp = a.key.localeCompare(b.key)
    } else if (sortBy === "size") {
      cmp = a.size - b.size
    } else {
      cmp =
        new Date(a.lastModified).getTime() -
        new Date(b.lastModified).getTime()
    }
    return sortDir === "asc" ? cmp : -cmp
  }), [filteredGalleryItems, sortBy, sortDir])

  const visibleSelectionKeys = useMemo(
    () =>
      viewMode === "gallery"
        ? sortedGalleryItems.map((item) => item.key)
        : sortedItems.map((item) => item.key),
    [sortedGalleryItems, sortedItems, viewMode]
  )

  const lightboxItems = useMemo(
    () => sortedGalleryItems.filter((item) => !item.isFolder && Boolean(item.mediaType)),
    [sortedGalleryItems]
  )

  useEffect(() => {
    localStorage.setItem("s3admin:demo:viewMode", viewMode)
  }, [viewMode])

  useEffect(() => {
    localStorage.setItem("s3admin:demo:sortBy", sortBy)
  }, [sortBy])

  useEffect(() => {
    localStorage.setItem("s3admin:demo:sortDir", sortDir)
  }, [sortDir])

  useEffect(() => {
    setSelectedKeys(new Set())
    setLightboxIndex(null)
    setPreviewFile(null)
    selectionAnchorRef.current = null
  }, [bucket, prefix, credentialId, viewMode])

  useEffect(() => {
    if (!selectionAnchorRef.current) return
    if (!visibleSelectionKeys.includes(selectionAnchorRef.current)) {
      selectionAnchorRef.current = null
    }
  }, [visibleSelectionKeys])

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (!(event.metaKey || event.ctrlKey)) return
      if (event.key.toLowerCase() !== "a") return
      if (isEditableTarget(event.target)) return
      if (lightboxIndex !== null || previewFile !== null) return

      event.preventDefault()

      if (visibleSelectionKeys.length === 0) {
        setSelectedKeys(new Set())
        selectionAnchorRef.current = null
        return
      }

      setSelectedKeys(new Set(visibleSelectionKeys))
      selectionAnchorRef.current =
        visibleSelectionKeys[visibleSelectionKeys.length - 1] ?? null
    }

    window.addEventListener("keydown", onKeyDown)
    return () => window.removeEventListener("keydown", onKeyDown)
  }, [visibleSelectionKeys, lightboxIndex, previewFile])

  function handleNavigate(folderKey: string) {
    const params = new URLSearchParams({ bucket })
    if (folderKey) params.set("prefix", folderKey)
    if (credentialId) params.set("credentialId", credentialId)
    router.push(`${BASE_PATH}?${params}`)
    setSelectedKeys(new Set())
    selectionAnchorRef.current = null
  }

  function handleSelect(key: string, options?: { shiftKey?: boolean }) {
    setSelectedKeys((prev) => {
      const next = new Set(prev)
      const shouldSelect = !prev.has(key)
      const anchorKey = selectionAnchorRef.current

      if (options?.shiftKey && anchorKey) {
        const anchorIndex = visibleSelectionKeys.indexOf(anchorKey)
        const currentIndex = visibleSelectionKeys.indexOf(key)

        if (anchorIndex !== -1 && currentIndex !== -1) {
          const [start, end] =
            anchorIndex <= currentIndex
              ? [anchorIndex, currentIndex]
              : [currentIndex, anchorIndex]

          for (const rangeKey of visibleSelectionKeys.slice(start, end + 1)) {
            if (shouldSelect) {
              next.add(rangeKey)
            } else {
              next.delete(rangeKey)
            }
          }

          selectionAnchorRef.current = key
          return next
        }
      }

      if (shouldSelect) {
        next.add(key)
      } else {
        next.delete(key)
      }

      selectionAnchorRef.current = key
      return next
    })
  }

  function handleSelectAll() {
    setSelectedKeys((prev) => {
      if (visibleSelectionKeys.length === 0) {
        selectionAnchorRef.current = null
        return new Set()
      }

      const allSelected = visibleSelectionKeys.every((key) => prev.has(key))
      if (allSelected) {
        selectionAnchorRef.current = null
        return new Set()
      }

      selectionAnchorRef.current =
        visibleSelectionKeys[visibleSelectionKeys.length - 1] ?? null
      return new Set(visibleSelectionKeys)
    })
  }

  function handleSort(column: "name" | "size" | "lastModified") {
    if (sortBy === column) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortBy(column)
      setSortDir("asc")
    }
  }

  function triggerBrowserDownload(url: string, filename?: string) {
    const link = document.createElement("a")
    link.href = url
    if (filename) {
      link.download = filename
    }
    link.rel = "noopener noreferrer"
    link.style.display = "none"
    document.body.appendChild(link)
    link.click()
    link.remove()
  }

  async function handleDownload(keys: string[]) {
    for (const key of keys) {
      try {
        const res = await fetch(`${API_PREFIX}/download`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ bucket, credentialId, key }),
        })
        if (!res.ok) throw new Error("Failed to create download URL")

        const { url, filename } = await res.json()
        triggerBrowserDownload(url, filename)
      } catch {
        toast.error(`Failed to download ${key}`)
      }
    }
  }

  function handlePreview(file: S3Object) {
    if (file.isFolder) return

    const ext = file.key.includes(".")
      ? file.key.slice(file.key.lastIndexOf(".") + 1)
      : ""
    const type = getPreviewType(ext)

    if (!type) {
      void handleDownload([file.key])
      return
    }

    if (type === "image" || type === "video") {
      void handleDownload([file.key])
      return
    }

    setPreviewFile(file)
  }

  const noop = useCallback(() => {
    toast.info("This action is not available in demo mode")
  }, [])

  if (!bucket) {
    if (credentialsLoading || bucketsLoading) {
      return (
        <div className="flex h-full items-center justify-center p-8">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )
    }

    if (credentials.length === 0) {
      return (
        <div className="flex h-full items-center justify-center p-8">
          <EmptyState type="no-credentials" />
        </div>
      )
    }

    if (availableBuckets.length === 0) {
      return (
        <div className="flex h-full items-center justify-center p-8">
          <EmptyState type="no-buckets" />
        </div>
      )
    }

    return (
      <div className="flex h-full flex-col items-center justify-center gap-4 p-8 text-center">
        <Database className="h-12 w-12 text-muted-foreground" />
        <div>
          <h2 className="text-lg font-semibold">Welcome to the Demo</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Select a bucket from the sidebar to browse files.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex h-full flex-col">
      <Topbar
        bucket={bucket}
        prefix={prefix}
        credentialId={credentialId}
        onSearch={setSearchQuery}
        onUpload={noop}
        onSync={noop}
        onCreateFolder={noop}
        onSort={handleSort}
        sortBy={sortBy}
        sortDir={sortDir}
        viewMode={viewMode}
        onViewModeChange={setViewMode}
        showVersions={showVersions}
        onShowVersionsChange={versioningEnabled ? setShowVersions : undefined}
        readOnly
        basePath={BASE_PATH}
      />

      {selectedKeys.size > 0 && (
        <MultiSelectToolbar
          selectedCount={selectedKeys.size}
          onDelete={noop}
          onDownload={() => {
            if (viewMode !== "gallery") {
              void handleDownload(Array.from(selectedKeys))
              return
            }

            const fileKeys = sortedGalleryItems
              .filter((item) => selectedKeys.has(item.key) && !item.isFolder)
              .map((item) => item.key)

            if (fileKeys.length === 0) {
              toast.error("Select at least one file to download")
              return
            }

            void handleDownload(fileKeys)
          }}
          onClear={() => {
            setSelectedKeys(new Set())
            selectionAnchorRef.current = null
          }}
          readOnly
        />
      )}

      {viewMode === "gallery" ? (
        <GalleryBrowser
          items={sortedGalleryItems}
          credentialId={credentialId ?? ""}
          bucket={bucket}
          isLoading={galleryLoading}
          isFetchingNextPage={isFetchingNextPage}
          hasNextPage={Boolean(hasNextPage)}
          selectedKeys={selectedKeys}
          onSelect={(item, options) => handleSelect(item.key, options)}
          onSelectAllVisible={handleSelectAll}
          onNavigate={(item) => {
            if (!item.isFolder) return
            handleNavigate(item.key)
          }}
          onOpenPreview={(item) => {
            if (item.isFolder) {
              handleNavigate(item.key)
              return
            }

            const ext = item.key.includes(".")
              ? item.key.slice(item.key.lastIndexOf(".") + 1)
              : ""
            const pType = getPreviewType(ext)
            if (!pType) {
              void handleDownload([item.key])
              return
            }

            if (pType !== "image" && pType !== "video") {
              setPreviewFile({
                key: item.key,
                size: item.size,
                lastModified: item.lastModified,
                isFolder: false,
              })
              return
            }

            const index = lightboxItems.findIndex((entry) => entry.key === item.key)
            if (index >= 0) setLightboxIndex(index)
          }}
          onDownload={(item) => {
            if (item.isFolder) return
            void handleDownload([item.key])
          }}
          onDelete={noop}
          onLoadMore={() => {
            if (!hasNextPage || isFetchingNextPage) return
            void fetchNextPage()
          }}
          readOnly
        />
      ) : (
        <FileBrowser
          prefix={prefix}
          files={sortedItems}
          isLoading={isLoading}
          selectedKeys={selectedKeys}
          onSelect={(file, options) => handleSelect(file.key, options)}
          onSelectAll={handleSelectAll}
          onNavigate={(file) => handleNavigate(file.key)}
          onRename={noop}
          onDelete={noop}
          onDownload={(file) => handleDownload([file.key])}
          onPreview={handlePreview}
          showVersions={showVersions}
          readOnly
        />
      )}

      <GalleryLightbox
        open={lightboxIndex !== null}
        onOpenChange={(open) => {
          if (!open) setLightboxIndex(null)
        }}
        items={lightboxItems}
        currentIndex={lightboxIndex ?? 0}
        bucket={bucket}
        credentialId={credentialId}
        onNavigate={(index) => setLightboxIndex(index)}
      />

      {previewFile && (
        <FilePreviewDialog
          open={previewFile !== null}
          onOpenChange={(open) => {
            if (!open) setPreviewFile(null)
          }}
          fileKey={previewFile.key}
          fileName={getBaseNameFromKey(previewFile.key, false)}
          fileSize={previewFile.size}
          bucket={bucket}
          credentialId={credentialId}
          onDownload={() => {
            void handleDownload([previewFile.key])
          }}
          apiPrefix={API_PREFIX}
        />
      )}
    </div>
  )
}

export default function DemoPage() {
  return (
    <Suspense>
      <DemoDashboardContent />
    </Suspense>
  )
}
