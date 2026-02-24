import { NextRequest, NextResponse } from "next/server"
import { communityGuard } from "@/lib/api-guard"
import { auth } from "@/lib/auth"
import { prisma } from "@/lib/db"
import { stripe } from "@/lib/stripe"
import { rateLimitByUser, rateLimitResponse } from "@/lib/rate-limit"
import { enforceObjectTransferPolicyForUser } from "@/lib/transfer-task-policy"
import { ACTIVE_SUBSCRIPTION_STATUSES } from "@/lib/subscription-status"
import Stripe from "stripe"
import { z } from "zod/v4"

const updateUserSchema = z.object({
  tier: z.string().trim().min(1).max(64).optional(),
  role: z.enum(["user", "admin"]).optional(),
})

const MANUAL_SUBSCRIPTION_PREFIX = "manual_"
const MANUAL_PERIOD_MS = 365 * 24 * 60 * 60 * 1000

function getPeriodDates(sub: Awaited<ReturnType<typeof stripe.subscriptions.retrieve>>) {
  const item = sub.items.data[0]
  return {
    start: new Date((item?.current_period_start ?? 0) * 1000),
    end: new Date((item?.current_period_end ?? 0) * 1000),
  }
}

function isManualStripeSubscriptionId(stripeSubscriptionId: string | null | undefined): boolean {
  return Boolean(
    stripeSubscriptionId && stripeSubscriptionId.startsWith(MANUAL_SUBSCRIPTION_PREFIX)
  )
}

function buildManualStripeSubscriptionId(userId: string, planSlug: string): string {
  return `${MANUAL_SUBSCRIPTION_PREFIX}${userId}_${planSlug}_${Date.now()}`
}

async function cancelStripeSubscriptionSafely(stripeSubscriptionId: string): Promise<void> {
  try {
    await stripe.subscriptions.cancel(stripeSubscriptionId)
  } catch (error) {
    const missingStripeSubscription =
      error instanceof Stripe.errors.StripeInvalidRequestError &&
      error.code === "resource_missing"
    const alreadyCanceledStripeSubscription =
      error instanceof Stripe.errors.StripeInvalidRequestError &&
      error.code === "subscription_already_canceled"

    if (!missingStripeSubscription && !alreadyCanceledStripeSubscription) {
      throw error
    }
  }
}

export async function PATCH(
  req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const guard = communityGuard()
  if (guard) return guard

  const session = await auth()
  if (!session?.user?.id || session.user.role !== "admin") {
    return NextResponse.json({ error: "Forbidden" }, { status: 403 })
  }

  const rl = rateLimitByUser(session.user.id, "admin", 30)
  if (!rl.success) return rateLimitResponse(rl.retryAfterSeconds)

  const { id } = await params
  const body = await req.json()
  const parsed = updateUserSchema.safeParse(body)
  if (!parsed.success) {
    return NextResponse.json({ error: "Invalid input" }, { status: 400 })
  }

  const user = await prisma.user.findUnique({
    where: { id },
    select: {
      id: true,
      email: true,
      role: true,
      stripeCustomerId: true,
      subscriptions: {
        where: { status: { in: [...ACTIVE_SUBSCRIPTION_STATUSES] } },
        orderBy: [{ currentPeriodEnd: "desc" }, { createdAt: "desc" }],
        take: 1,
        select: {
          id: true,
          stripeSubscriptionId: true,
          status: true,
          currentPeriodStart: true,
          currentPeriodEnd: true,
          planId: true,
          plan: {
            select: {
              id: true,
              slug: true,
              stripePriceId: true,
            },
          },
        },
      },
    },
  })

  if (!user) {
    return NextResponse.json({ error: "User not found" }, { status: 404 })
  }

  if (parsed.data.tier !== undefined) {
    let keepSubscriptionId: string | null = null
    const targetPlan = await prisma.plan.findUnique({
      where: { slug: parsed.data.tier },
      select: { id: true, slug: true, stripePriceId: true, isActive: true },
    })
    if (!targetPlan || !targetPlan.isActive) {
      return NextResponse.json({ error: "Plan not found or inactive" }, { status: 400 })
    }

    const activeSubscription = user.subscriptions[0]
    const activeStripeSubscriptionId = activeSubscription?.stripeSubscriptionId ?? null
    const activeIsManual = isManualStripeSubscriptionId(activeStripeSubscriptionId)

    if (targetPlan.slug === "free") {
      if (activeStripeSubscriptionId && !activeIsManual) {
        await cancelStripeSubscriptionSafely(activeStripeSubscriptionId)
      }

      if (activeSubscription) {
        await prisma.subscription.update({
          where: { id: activeSubscription.id },
          data: {
            status: "canceled",
            cancelAtPeriodEnd: false,
            canceledAt: new Date(),
          },
        })
      }
    } else if (!targetPlan.stripePriceId) {
      if (activeStripeSubscriptionId && !activeIsManual) {
        await cancelStripeSubscriptionSafely(activeStripeSubscriptionId)
      }

      const now = new Date()
      const periodEnd = new Date(now.getTime() + MANUAL_PERIOD_MS)
      const manualStripeCustomerId = user.stripeCustomerId ?? `${MANUAL_SUBSCRIPTION_PREFIX}${user.id}`

      if (activeSubscription && activeIsManual) {
        const updatedManualSubscription = await prisma.subscription.update({
          where: { id: activeSubscription.id },
          data: {
            planId: targetPlan.id,
            stripeCustomerId: manualStripeCustomerId,
            status: "active",
            currentPeriodStart: now,
            currentPeriodEnd: periodEnd,
            cancelAtPeriodEnd: false,
            canceledAt: null,
          },
        })
        keepSubscriptionId = updatedManualSubscription.id
      } else {
        const manualSubscription = await prisma.subscription.create({
          data: {
            userId: user.id,
            planId: targetPlan.id,
            stripeSubscriptionId: buildManualStripeSubscriptionId(user.id, targetPlan.slug),
            stripeCustomerId: manualStripeCustomerId,
            status: "active",
            currentPeriodStart: now,
            currentPeriodEnd: periodEnd,
            cancelAtPeriodEnd: false,
            canceledAt: null,
          },
        })
        keepSubscriptionId = manualSubscription.id
      }
    } else {
      let customerId = user.stripeCustomerId
      if (!customerId) {
        const customer = await stripe.customers.create({
          email: user.email,
          metadata: { userId: user.id },
        })
        customerId = customer.id
        await prisma.user.update({
          where: { id: user.id },
          data: { stripeCustomerId: customerId },
        })
      }

      if (activeStripeSubscriptionId && !activeIsManual) {
        const stripeSub = await stripe.subscriptions.retrieve(activeStripeSubscriptionId)
        const itemId = stripeSub.items.data[0]?.id
        if (!itemId) {
          return NextResponse.json(
            { error: "Active Stripe subscription item not found" },
            { status: 500 }
          )
        }

        const updatedStripeSub = await stripe.subscriptions.update(activeStripeSubscriptionId, {
          items: [{ id: itemId, price: targetPlan.stripePriceId }],
          proration_behavior: "create_prorations",
          metadata: { userId: user.id, planId: targetPlan.id, tier: targetPlan.slug },
        })

        const period = getPeriodDates(updatedStripeSub)
        const updatedSubscription = await prisma.subscription.update({
          where: { id: activeSubscription.id },
          data: {
            planId: targetPlan.id,
            status: updatedStripeSub.status,
            currentPeriodStart: period.start,
            currentPeriodEnd: period.end,
            cancelAtPeriodEnd: updatedStripeSub.cancel_at_period_end,
            canceledAt: updatedStripeSub.canceled_at
              ? new Date(updatedStripeSub.canceled_at * 1000)
              : null,
          },
        })
        keepSubscriptionId = updatedSubscription.id
      } else {
        const createdStripeSub = await stripe.subscriptions.create({
          customer: customerId!,
          items: [{ price: targetPlan.stripePriceId }],
          metadata: { userId: user.id, planId: targetPlan.id, tier: targetPlan.slug },
        })

        const period = getPeriodDates(createdStripeSub)
        const syncedSubscription = await prisma.subscription.upsert({
          where: { stripeSubscriptionId: createdStripeSub.id },
          create: {
            userId: user.id,
            planId: targetPlan.id,
            stripeSubscriptionId: createdStripeSub.id,
            stripeCustomerId: customerId!,
            status: createdStripeSub.status,
            currentPeriodStart: period.start,
            currentPeriodEnd: period.end,
            cancelAtPeriodEnd: createdStripeSub.cancel_at_period_end,
            canceledAt: createdStripeSub.canceled_at
              ? new Date(createdStripeSub.canceled_at * 1000)
              : null,
          },
          update: {
            planId: targetPlan.id,
            stripeCustomerId: customerId!,
            status: createdStripeSub.status,
            currentPeriodStart: period.start,
            currentPeriodEnd: period.end,
            cancelAtPeriodEnd: createdStripeSub.cancel_at_period_end,
            canceledAt: createdStripeSub.canceled_at
              ? new Date(createdStripeSub.canceled_at * 1000)
              : null,
          },
        })
        keepSubscriptionId = syncedSubscription.id
      }
    }

    await prisma.subscription.updateMany({
      where: {
        userId: user.id,
        status: { in: [...ACTIVE_SUBSCRIPTION_STATUSES] },
        ...(keepSubscriptionId
          ? {
              NOT: {
                id: keepSubscriptionId,
              },
            }
          : {}),
      },
      data: {
        status: "canceled",
        cancelAtPeriodEnd: false,
        canceledAt: new Date(),
      },
    })

    await enforceObjectTransferPolicyForUser(user.id)
  }

  if (parsed.data.role !== undefined) {
    await prisma.user.update({
      where: { id },
      data: { role: parsed.data.role },
    })
  }

  const refreshed = await prisma.user.findUnique({
    where: { id },
    select: {
      id: true,
      role: true,
      subscriptions: {
        where: { status: { in: [...ACTIVE_SUBSCRIPTION_STATUSES] } },
        orderBy: [{ currentPeriodEnd: "desc" }, { createdAt: "desc" }],
        take: 1,
        select: {
          plan: { select: { slug: true } },
        },
      },
    },
  })

  return NextResponse.json({
    id,
    role: refreshed?.role ?? parsed.data.role ?? user.role,
    tier: refreshed?.subscriptions[0]?.plan.slug ?? "free",
  })
}

export async function DELETE(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const guard = communityGuard()
  if (guard) return guard

  const session = await auth()
  if (!session?.user?.id || session.user.role !== "admin") {
    return NextResponse.json({ error: "Forbidden" }, { status: 403 })
  }

  const rl = rateLimitByUser(session.user.id, "admin", 30)
  if (!rl.success) return rateLimitResponse(rl.retryAfterSeconds)

  const { id } = await params

  if (id === session.user.id) {
    return NextResponse.json({ error: "Cannot delete yourself" }, { status: 400 })
  }

  const user = await prisma.user.findUnique({
    where: { id },
    select: {
      stripeCustomerId: true,
      subscriptions: { where: { status: { in: [...ACTIVE_SUBSCRIPTION_STATUSES] } } },
    },
  })

  if (!user) {
    return NextResponse.json({ error: "User not found" }, { status: 404 })
  }

  for (const sub of user.subscriptions) {
    if (isManualStripeSubscriptionId(sub.stripeSubscriptionId)) {
      continue
    }

    await cancelStripeSubscriptionSafely(sub.stripeSubscriptionId)
  }

  if (user.stripeCustomerId) {
    try {
      await stripe.customers.del(user.stripeCustomerId)
    } catch {}
  }

  await prisma.user.delete({ where: { id } })

  return NextResponse.json({ success: true })
}
