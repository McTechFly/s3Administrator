import type { Metadata } from "next"
import Link from "next/link"
import { prisma } from "@/lib/db"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

export const metadata: Metadata = { title: "Admin" }
export const dynamic = "force-dynamic"

export default async function AdminHomePage() {
  const [userCount, adminCount, credCount, shareCount] = await Promise.all([
    prisma.user.count(),
    prisma.user.count({ where: { role: "admin" } }),
    prisma.s3Credential.count(),
    prisma.bucketShare.count(),
  ])

  const stats = [
    { label: "Users", value: userCount, href: "/admin/users" },
    { label: "Admins", value: adminCount, href: "/admin/users" },
    { label: "Connected buckets", value: credCount, href: null },
    { label: "Active shares", value: shareCount, href: null },
  ] as const

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-semibold">Admin panel</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Manage users, roles and shared buckets.
        </p>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((s) => (
          <Card key={s.label}>
            <CardHeader className="pb-2">
              <CardDescription>{s.label}</CardDescription>
              <CardTitle className="text-3xl tabular-nums">{s.value}</CardTitle>
            </CardHeader>
            <CardContent>
              {s.href ? (
                <Link href={s.href} className="text-sm text-primary hover:underline">
                  Manage →
                </Link>
              ) : null}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
