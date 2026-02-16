import type { MetadataRoute } from "next"
import { getSiteUrl, absoluteUrl } from "@/lib/site-url"

const PRIVATE_PATHS = [
  "/admin/",
  "/dashboard/",
  "/settings/",
  "/billing/",
  "/api/",
  "/login",
  "/register",
]

const AI_TRAINING_BOTS = [
  "GPTBot",
  "Google-Extended",
  "CCBot",
  "anthropic-ai",
  "Claude-Web",
  "Bytespider",
  "Diffbot",
  "Applebot-Extended",
  "FacebookBot",
  "PerplexityBot",
]

export default function robots(): MetadataRoute.Robots {
  return {
    rules: [
      {
        userAgent: "*",
        allow: "/",
        disallow: PRIVATE_PATHS,
      },
      {
        userAgent: "OAI-SearchBot",
        allow: "/",
        disallow: PRIVATE_PATHS,
      },
      {
        userAgent: "ChatGPT-User",
        allow: "/",
        disallow: PRIVATE_PATHS,
      },
      ...AI_TRAINING_BOTS.map((bot) => ({
        userAgent: bot,
        disallow: ["/"],
      })),
    ],
    sitemap: absoluteUrl("/sitemap.xml"),
    host: getSiteUrl(),
  }
}
