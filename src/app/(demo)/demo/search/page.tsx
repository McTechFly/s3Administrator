"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { LayoutGrid, List, Loader2, Search } from "lucide-react"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { FileBrowser } from "@/components/dashboard/file-browser"
import { GalleryBrowser } from "@/components/dashboard/gallery-browser"
import { GalleryLightbox } from "@/components/dashboard/gallery-lightbox"
import { MultiSelectToolbar } from "@/components/dashboard/multi-select-toolbar"
import { getPreviewType } from "@/lib/media"
import { toast } from "sonner"
import type { MediaType, PreviewType, S3Object } from "@/types"

const API_PREFIX = "/api/demo/s3"
const DEMO_SEARCH_VIEW_MODE_STORAGE_KEY = "s3admin:demo:search:view-mode"

interface SearchResult {
  id: string
  key: string
  bucket: string
  credentialId: string
  extension: string
  mediaType: MediaType | null
  previewUrl: string | null
  isVideo: boolean
  size: number
  lastModified: string
}

interface SearchResponse {
  results: SearchResult[]
  total: number
}

interface SearchItem extends S3Object {
  id: string
  bucket: string
  credentialId: string
  extension: string
  mediaType: MediaType | null
  previewType: PreviewType | null
  previewUrl: string | null
  isVideo: boolean
}

function toSearchItem(result: SearchResult): SearchItem {
  return {
    id: `${result.credentialId}::${result.bucket}::${result.key}`,
    key: result.key,
    size: result.size,
    lastModified: result.lastModified,
    isFolder: false,
    bucket: result.bucket,
    credentialId: result.credentialId,
    extension: result.extension,
    mediaType: result.mediaType,
    previewType: getPreviewType(result.extension),
    previewUrl: result.previewUrl,
    isVideo: result.isVideo,
  }
}

export default function DemoSearchPage() {
  const [query, setQuery] = useState("")
  const [debouncedQuery, setDebouncedQuery] = useState("")
  const [viewMode, setViewMode] = useState<"list" | "gallery">(() => {
    if (typeof window === "undefined") return "list"
    const saved = window.localStorage.getItem(DEMO_SEARCH_VIEW_MODE_STORAGE_KEY)
    return saved === "gallery" ? "gallery" : "list"
  })
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set())
  const [lightboxIndex, setLightboxIndex] = useState<number | null>(null)
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  const selectionAnchorRef = useRef<string | null>(null)

  const { data, isLoading } = useQuery<SearchResponse>({
    queryKey: ["demo-search", debouncedQuery],
    queryFn: async () => {
      const params = new URLSearchParams({ q: debouncedQuery, take: "100" })
      const res = await fetch(`${API_PREFIX}/search?${params}`)
      if (!res.ok) throw new Error("Search failed")
      return res.json()
    },
    enabled: debouncedQuery.length >= 2,
  })

  const items = useMemo<SearchItem[]>(
    () => (data?.results ?? []).map((result) => toSearchItem(result)),
    [data]
  )

  const visibleRowIds = useMemo(() => items.map((item) => item.id), [items])
  const selectedItems = useMemo(
    () => items.filter((item) => selectedKeys.has(item.id)),
    [items, selectedKeys]
  )
  const lightboxItems = useMemo(
    () => items.filter((item) => Boolean(item.mediaType)),
    [items]
  )

  useEffect(() => {
    window.localStorage.setItem(DEMO_SEARCH_VIEW_MODE_STORAGE_KEY, viewMode)
  }, [viewMode])

  useEffect(() => {
    if (!selectionAnchorRef.current) return
    if (!visibleRowIds.includes(selectionAnchorRef.current)) {
      selectionAnchorRef.current = null
    }
  }, [visibleRowIds])

  useEffect(() => {
    if (lightboxIndex === null) return
    if (lightboxIndex < 0 || lightboxIndex >= lightboxItems.length) {
      setLightboxIndex(null)
    }
  }, [lightboxIndex, lightboxItems.length])

  function resetSelection() {
    setSelectedKeys(new Set())
    selectionAnchorRef.current = null
  }

  function handleSearchChange(value: string) {
    setQuery(value)
    resetSelection()
    setLightboxIndex(null)
    if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current)
    debounceTimerRef.current = setTimeout(() => {
      setDebouncedQuery(value)
    }, 400)
  }

  function handleSelect(item: SearchItem, options?: { shiftKey?: boolean }) {
    const id = item.id

    setSelectedKeys((prev) => {
      const next = new Set(prev)
      const shouldSelect = !prev.has(id)
      const anchorId = selectionAnchorRef.current

      if (options?.shiftKey && anchorId) {
        const anchorIndex = visibleRowIds.indexOf(anchorId)
        const currentIndex = visibleRowIds.indexOf(id)

        if (anchorIndex !== -1 && currentIndex !== -1) {
          const [start, end] =
            anchorIndex <= currentIndex
              ? [anchorIndex, currentIndex]
              : [currentIndex, anchorIndex]

          for (const rangeId of visibleRowIds.slice(start, end + 1)) {
            if (shouldSelect) {
              next.add(rangeId)
            } else {
              next.delete(rangeId)
            }
          }

          selectionAnchorRef.current = id
          return next
        }
      }

      if (shouldSelect) {
        next.add(id)
      } else {
        next.delete(id)
      }

      selectionAnchorRef.current = id
      return next
    })
  }

  function handleSelectAll() {
    setSelectedKeys((prev) => {
      if (visibleRowIds.length === 0) {
        selectionAnchorRef.current = null
        return new Set()
      }

      const allSelected = visibleRowIds.every((id) => prev.has(id))
      if (allSelected) {
        selectionAnchorRef.current = null
        return new Set()
      }

      selectionAnchorRef.current = visibleRowIds[visibleRowIds.length - 1] ?? null
      return new Set(visibleRowIds)
    })
  }

  async function handleDownload(item: SearchItem) {
    try {
      const res = await fetch(`${API_PREFIX}/download`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bucket: item.bucket,
          credentialId: item.credentialId,
          key: item.key,
        }),
      })
      if (!res.ok) throw new Error("Download failed")
      const { url, filename } = await res.json()
      const link = document.createElement("a")
      link.href = url
      if (filename) link.download = filename
      link.rel = "noopener noreferrer"
      link.style.display = "none"
      document.body.appendChild(link)
      link.click()
      link.remove()
    } catch {
      toast.error("Failed to download file")
    }
  }

  async function handleBulkDownload() {
    for (const item of selectedItems) {
      await handleDownload(item)
    }
  }

  const noop = () => {
    toast.info("This action is not available in demo mode")
  }

  return (
    <div className="flex h-full flex-col">
      <div className="border-b px-4 py-3">
        <div className="flex flex-wrap items-center gap-3">
          <div className="min-w-0 flex-1">
            <h1 className="mb-3 text-lg font-semibold">Search Files</h1>
            <div className="relative max-w-md">
              <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input
                placeholder="Search across all buckets..."
                value={query}
                onChange={(e) => handleSearchChange(e.target.value)}
                className="pl-9"
              />
            </div>
          </div>

          <div className="flex items-center overflow-hidden rounded-md border">
            <Button
              type="button"
              variant={viewMode === "list" ? "secondary" : "ghost"}
              size="sm"
              className="h-9 rounded-none border-0"
              onClick={() => {
                setViewMode("list")
                resetSelection()
                setLightboxIndex(null)
              }}
            >
              <List className="mr-1.5 h-4 w-4" />
              List
            </Button>
            <Button
              type="button"
              variant={viewMode === "gallery" ? "secondary" : "ghost"}
              size="sm"
              className="h-9 rounded-none border-0"
              onClick={() => {
                setViewMode("gallery")
                resetSelection()
                setLightboxIndex(null)
              }}
            >
              <LayoutGrid className="mr-1.5 h-4 w-4" />
              Gallery
            </Button>
          </div>
        </div>

        {data?.total != null && debouncedQuery.length >= 2 && (
          <p className="mt-2 text-sm text-muted-foreground">
            {data.total} {data.total === 1 ? "result" : "results"}
          </p>
        )}
      </div>

      {selectedKeys.size > 0 && (
        <MultiSelectToolbar
          selectedCount={selectedKeys.size}
          onDelete={noop}
          onDownload={() => void handleBulkDownload()}
          onClear={resetSelection}
          readOnly
        />
      )}

      {isLoading && debouncedQuery.length >= 2 ? (
        <div className="flex flex-1 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : debouncedQuery.length < 2 ? (
        <div className="flex flex-1 items-center justify-center">
          <p className="text-sm text-muted-foreground">
            Type at least 2 characters to search
          </p>
        </div>
      ) : items.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <p className="text-sm text-muted-foreground">
            No files found for {`"${debouncedQuery}"`}
          </p>
        </div>
      ) : viewMode === "gallery" ? (
        <GalleryBrowser
          items={items}
          isLoading={false}
          isFetchingNextPage={false}
          hasNextPage={false}
          selectedKeys={selectedKeys}
          getItemSelectionKey={(item) => (item as SearchItem).id}
          getItemBucket={(item) => (item as SearchItem).bucket}
          getItemCredentialId={(item) => (item as SearchItem).credentialId}
          onSelect={(item, options) => handleSelect(item as SearchItem, options)}
          onSelectAllVisible={handleSelectAll}
          onNavigate={noop}
          onOpenPreview={(item) => {
            const searchItem = item as SearchItem
            const index = lightboxItems.findIndex((entry) => entry.id === searchItem.id)
            if (index >= 0) {
              setLightboxIndex(index)
              return
            }
            void handleDownload(searchItem)
          }}
          onDownload={(item) => void handleDownload(item as SearchItem)}
          onDelete={noop}
          onLoadMore={() => {}}
          readOnly
        />
      ) : (
        <FileBrowser
          prefix=""
          files={items}
          isLoading={false}
          selectedKeys={selectedKeys}
          onSelect={(file, options) => handleSelect(file as SearchItem, options)}
          onSelectAll={handleSelectAll}
          onNavigate={noop}
          onRename={noop}
          onDelete={noop}
          onDownload={(file) => void handleDownload(file as SearchItem)}
          getRowId={(file) => (file as SearchItem).id}
          getLocationLabel={(file) => (file as SearchItem).bucket}
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
        apiPrefix={API_PREFIX}
        getItemBucket={(item) => (item as SearchItem).bucket}
        getItemCredentialId={(item) => (item as SearchItem).credentialId}
        onNavigate={(index) => setLightboxIndex(index)}
        onDownload={(item) => void handleDownload(item as SearchItem)}
      />
    </div>
  )
}
