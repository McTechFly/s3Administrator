import type { MetadataRoute } from "next"
import { seoLandingPages } from "@/lib/seo-landing-pages"
import { absoluteUrl } from "@/lib/site-url"

const STATIC_MARKETING_PATHS = [
  { path: "/", changeFrequency: "weekly" as const, priority: 1, updatedAt: "2026-02-15" },
  { path: "/pricing", changeFrequency: "monthly" as const, priority: 0.8, updatedAt: "2026-02-15" },
  { path: "/features", changeFrequency: "monthly" as const, priority: 0.8, updatedAt: "2026-02-15" },
  { path: "/providers", changeFrequency: "monthly" as const, priority: 0.8, updatedAt: "2026-02-15" },
  { path: "/compare", changeFrequency: "monthly" as const, priority: 0.8, updatedAt: "2026-02-15" },
  { path: "/open-source", changeFrequency: "monthly" as const, priority: 0.6, updatedAt: "2026-02-15" },
  { path: "/self-hosted", changeFrequency: "monthly" as const, priority: 0.6, updatedAt: "2026-02-15" },
  { path: "/security", changeFrequency: "monthly" as const, priority: 0.6, updatedAt: "2026-02-15" },
] as const

export default function sitemap(): MetadataRoute.Sitemap {
  const staticPages: MetadataRoute.Sitemap = STATIC_MARKETING_PATHS.map((entry) => ({
    url: absoluteUrl(entry.path),
    lastModified: new Date(entry.updatedAt),
    changeFrequency: entry.changeFrequency,
    priority: entry.priority,
  }))

  const landingPages: MetadataRoute.Sitemap = seoLandingPages.map((page) => ({
    url: absoluteUrl(`/${page.category}/${page.slug}`),
    ...("updatedAt" in page && page.updatedAt
      ? { lastModified: new Date(page.updatedAt as string) }
      : {}),
    changeFrequency: "weekly",
    priority: 0.7,
  }))

  return [...staticPages, ...landingPages]
}
