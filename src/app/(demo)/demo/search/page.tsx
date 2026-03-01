"use client"

import { useRef, useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { Search, Loader2 } from "lucide-react"
import { Input } from "@/components/ui/input"
import { FileBrowser } from "@/components/dashboard/file-browser"
import { toast } from "sonner"
import type { S3Object } from "@/types"

const API_PREFIX = "/api/demo/s3"

interface SearchResult {
  id: string
  key: string
  bucket: string
  credentialId: string
  extension: string
  size: number
  lastModified: string
}

interface SearchResponse {
  results: SearchResult[]
  total: number
}

export default function DemoSearchPage() {
  const [query, setQuery] = useState("")
  const [debouncedQuery, setDebouncedQuery] = useState("")
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set())
  const debounceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

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

  const results: S3Object[] = (data?.results ?? []).map((r) => ({
    key: r.key,
    size: r.size,
    lastModified: r.lastModified,
    isFolder: false,
  }))

  function handleSearchChange(value: string) {
    setQuery(value)
    if (debounceTimerRef.current) clearTimeout(debounceTimerRef.current)
    debounceTimerRef.current = setTimeout(() => {
      setDebouncedQuery(value)
    }, 400)
  }

  async function handleDownload(file: S3Object) {
    try {
      const result = data?.results.find((r) => r.key === file.key)
      if (!result) return
      const res = await fetch(`${API_PREFIX}/download`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          bucket: result.bucket,
          credentialId: result.credentialId,
          key: result.key,
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

  const noop = () => {
    toast.info("This action is not available in demo mode")
  }

  return (
    <div className="flex h-full flex-col">
      <div className="border-b px-4 py-3">
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
        {data?.total != null && debouncedQuery.length >= 2 && (
          <p className="mt-2 text-sm text-muted-foreground">
            {data.total} {data.total === 1 ? "result" : "results"}
          </p>
        )}
      </div>

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
      ) : (
        <FileBrowser
          prefix=""
          files={results}
          isLoading={false}
          selectedKeys={selectedKeys}
          onSelect={(file) => {
            setSelectedKeys((prev) => {
              const next = new Set(prev)
              if (next.has(file.key)) {
                next.delete(file.key)
              } else {
                next.add(file.key)
              }
              return next
            })
          }}
          onSelectAll={() => {
            setSelectedKeys((prev) => {
              if (prev.size === results.length) return new Set()
              return new Set(results.map((r) => r.key))
            })
          }}
          onNavigate={noop}
          onRename={noop}
          onDelete={noop}
          onDownload={handleDownload}
          readOnly
        />
      )}
    </div>
  )
}
