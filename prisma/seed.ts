import { PrismaClient } from "@prisma/client"
import Stripe from "stripe"

const prisma = new PrismaClient()

const edition = (process.env.NEXT_PUBLIC_EDITION || process.env.EDITION || "community")
  .trim()
  .toLowerCase()
const isCommunity = edition !== "cloud"

function getStripe(): Stripe | null {
  const environment = process.env.ENVIRONMENT?.trim().toUpperCase()
  if (environment !== "COMMUNITY" && environment !== "CLOUD") {
    throw new Error('ENVIRONMENT must be set to either "COMMUNITY" or "CLOUD".')
  }

  const key = process.env[`STRIPE_SECRET_KEY_${environment}`] || process.env.STRIPE_SECRET_KEY
  if (!key) return null
  return new Stripe(key, { apiVersion: "2026-01-28.clover" })
}

const GB = BigInt(1024 ** 3)

const defaultPlans = [
  {
    slug: "free",
    name: "Free",
    priceMonthly: 0,
    bucketLimit: 10,
    fileLimit: 10000,
    storageLimitBytes: BigInt(50) * GB,
    auditLogs: false,
    thumbnailCache: false,
    features: [
      "Up to 10,000 cached files",
      "Up to 10 buckets",
      "Up to 50 GB storage",
      "Recursive delete",
      "Multiple upload",
    ],
    sortOrder: 0,
  },
  {
    slug: "starter",
    name: "Starter",
    priceMonthly: 300,
    bucketLimit: 50,
    fileLimit: 50000,
    storageLimitBytes: BigInt(50) * GB,
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Everything in Free",
      "Preview thumbnails",
      "Copy folder to folder",
      "Copy bucket to bucket",
      "Audit logs",
      "Search all files",
      "Up to 50,000 cached files",
      "Up to 50 buckets",
      "Up to 50 GB storage",
    ],
    sortOrder: 1,
  },
  {
    slug: "pro",
    name: "Pro",
    priceMonthly: 900,
    bucketLimit: 1000,
    fileLimit: 500000,
    storageLimitBytes: BigInt(500) * GB,
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Everything in Starter",
      "Sync tasks",
      "Up to 500,000 cached files",
      "Up to 1,000 buckets",
      "Up to 500 GB storage",
    ],
    sortOrder: 2,
  },
  {
    slug: "enterprise",
    name: "Enterprise",
    priceMonthly: 0,
    bucketLimit: 1000,
    fileLimit: 0,
    storageLimitBytes: BigInt(0),
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Everything in Pro",
      "Unlimited cached files",
      "Unlimited storage",
      "Dedicated support",
      "Custom integrations",
      "SLA",
    ],
    sortOrder: 3,
  },
]

async function main() {
  const stripe = isCommunity ? null : getStripe()

  console.log(`Seeding plans for ${isCommunity ? "community" : "cloud"} edition...`)
  if (!stripe && !isCommunity) {
    console.log("  STRIPE_SECRET_KEY not set — skipping Stripe price creation")
  }

  for (const plan of defaultPlans) {
    const existing = await prisma.plan.findUnique({ where: { slug: plan.slug } })

    // Create Stripe product+price for paid plans that don't have one yet
    let stripePriceId = existing?.stripePriceId ?? null
    if (stripe && plan.priceMonthly > 0 && !stripePriceId) {
      console.log(`  Creating Stripe product+price for ${plan.slug}...`)
      const product = await stripe.products.create({
        name: plan.name,
        metadata: { slug: plan.slug },
      })
      const price = await stripe.prices.create({
        product: product.id,
        unit_amount: plan.priceMonthly,
        currency: "usd",
        recurring: { interval: "month" },
        metadata: { slug: plan.slug },
      })
      stripePriceId = price.id
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (prisma.plan as any).upsert({
      where: { slug: plan.slug },
      update: {
        name: plan.name,
        bucketLimit: plan.bucketLimit,
        fileLimit: plan.fileLimit,
        storageLimitBytes: plan.storageLimitBytes,
        auditLogs: plan.auditLogs,
        thumbnailCache: plan.thumbnailCache,
        features: plan.features,
        sortOrder: plan.sortOrder,
        ...(stripePriceId && !existing?.stripePriceId ? { stripePriceId } : {}),
      },
      create: { ...plan, stripePriceId },
    })
    console.log(`  Upserted plan: ${plan.slug}${stripePriceId ? ` (price: ${stripePriceId})` : ""}`)
  }

  console.log("Seeding complete.")
}

main()
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
  .finally(() => prisma.$disconnect())
