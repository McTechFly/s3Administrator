import type { NextConfig } from "next"

const nextConfig: NextConfig = {
  output: "standalone",
  transpilePackages: [
    "@s3administrator/auth",
    "@s3administrator/billing",
    "@s3administrator/admin",
    "@s3administrator/audit",
    "@s3administrator/marketing",
  ],
  experimental: {
    // Storadera (and potentially other S3-compatible providers) require
    // proxy uploads where multipart chunks pass through the server.
    // Default is 10 MB which truncates larger chunks silently.
    proxyClientMaxBodySize: "200mb",
  },
}

export default nextConfig
