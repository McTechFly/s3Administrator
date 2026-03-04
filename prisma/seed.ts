import { PrismaClient } from "@prisma/client"
import { PrismaPg } from "@prisma/adapter-pg"
import Stripe from "stripe"

const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: process.env.DATABASE_URL! }) })

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
  return new Stripe(key, { apiVersion: "2026-02-25.clover" })
}

const GB = BigInt(1024 ** 3)

// ── Individual Plans ────────────────────────────────────────────────
const individualPlans = [
  {
    slug: "free",
    name: "Free",
    type: "individual" as const,
    priceMonthly: 0,
    seatPriceMonthly: null,
    maxSeats: null,
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
    type: "individual" as const,
    priceMonthly: 500,
    seatPriceMonthly: null,
    maxSeats: null,
    bucketLimit: 50,
    fileLimit: 50000,
    storageLimitBytes: BigInt(50) * GB,
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Everything in Free",
      "Copy folder to folder",
      "Copy bucket to bucket",
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
    type: "individual" as const,
    priceMonthly: 1200,
    seatPriceMonthly: null,
    maxSeats: null,
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
    type: "individual" as const,
    priceMonthly: 0,
    seatPriceMonthly: null,
    maxSeats: null,
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

// ── Team Plans ──────────────────────────────────────────────────────
const teamPlans = [
  {
    slug: "team-startup",
    name: "Startup",
    type: "team" as const,
    priceMonthly: 0,
    seatPriceMonthly: 1500,
    maxSeats: 10,
    bucketLimit: 50,
    fileLimit: 50000,
    storageLimitBytes: BigInt(100) * GB,
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Up to 10 team members",
      "Shared S3 credentials",
      "Copy folder to folder",
      "Copy bucket to bucket",
      "Search all files",
      "Up to 50,000 cached files",
      "Up to 50 buckets",
      "Up to 100 GB storage",
    ],
    sortOrder: 10,
  },
  {
    slug: "team-scaling",
    name: "Scaling",
    type: "team" as const,
    priceMonthly: 0,
    seatPriceMonthly: 2500,
    maxSeats: 50,
    bucketLimit: 1000,
    fileLimit: 500000,
    storageLimitBytes: BigInt(500) * GB,
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Everything in Startup",
      "Up to 50 team members",
      "Viewer role",
      "Sync tasks",
      "Up to 500,000 cached files",
      "Up to 1,000 buckets",
      "Up to 500 GB storage",
    ],
    sortOrder: 11,
  },
  {
    slug: "team-enterprise",
    name: "Enterprise",
    type: "team" as const,
    priceMonthly: 0,
    seatPriceMonthly: null,
    maxSeats: null,
    bucketLimit: 1000,
    fileLimit: 0,
    storageLimitBytes: BigInt(0),
    auditLogs: true,
    thumbnailCache: true,
    features: [
      "Everything in Scaling",
      "Unlimited team members",
      "Unlimited cached files",
      "Unlimited storage",
      "Dedicated support",
      "Custom integrations",
      "SLA",
    ],
    sortOrder: 12,
  },
]

// ── Dynamic Pricing Template Plans (FK anchors, not shown to users) ──
const customPlans = [
  {
    slug: "custom-individual",
    name: "Custom Individual",
    type: "individual" as const,
    priceMonthly: 0,
    seatPriceMonthly: null,
    maxSeats: null,
    bucketLimit: 100,
    fileLimit: 0,
    storageLimitBytes: BigInt(0),
    auditLogs: true,
    thumbnailCache: true,
    features: [],
    sortOrder: 99,
    isActive: false,
  },
  {
    slug: "custom-team",
    name: "Custom Team",
    type: "team" as const,
    priceMonthly: 0,
    seatPriceMonthly: null,
    maxSeats: null,
    bucketLimit: 100,
    fileLimit: 0,
    storageLimitBytes: BigInt(0),
    auditLogs: true,
    thumbnailCache: true,
    features: [],
    sortOrder: 99,
    isActive: false,
  },
]

const defaultPlans = [...individualPlans, ...teamPlans, ...customPlans]

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
    const billablePrice = plan.seatPriceMonthly ?? plan.priceMonthly
    if (stripe && billablePrice > 0 && !stripePriceId) {
      console.log(`  Creating Stripe product+price for ${plan.slug}...`)
      const product = await stripe.products.create({
        name: `${plan.type === "team" ? "Team: " : ""}${plan.name}`,
        metadata: { slug: plan.slug, type: plan.type },
      })
      const price = await stripe.prices.create({
        product: product.id,
        unit_amount: billablePrice,
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
        type: plan.type,
        priceMonthly: plan.priceMonthly,
        seatPriceMonthly: plan.seatPriceMonthly,
        maxSeats: plan.maxSeats,
        bucketLimit: plan.bucketLimit,
        fileLimit: plan.fileLimit,
        storageLimitBytes: plan.storageLimitBytes,
        auditLogs: plan.auditLogs,
        thumbnailCache: plan.thumbnailCache,
        features: plan.features,
        sortOrder: plan.sortOrder,
        ...("isActive" in plan ? { isActive: plan.isActive } : {}),
        ...(stripePriceId && !existing?.stripePriceId ? { stripePriceId } : {}),
      },
      create: { ...plan, stripePriceId },
    })
    console.log(`  Upserted plan: ${plan.slug} (${plan.type})${stripePriceId ? ` (price: ${stripePriceId})` : ""}`)
  }

  console.log("Seeding complete.")
}

main()
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
  .finally(() => prisma.$disconnect())
