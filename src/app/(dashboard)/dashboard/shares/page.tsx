import type { Metadata } from "next"
import { redirect } from "next/navigation"
import { auth } from "@/lib/auth"
import { SharesManager } from "@/components/dashboard/shares-manager"

export const metadata: Metadata = { title: "Shared buckets" }
export const dynamic = "force-dynamic"

export default async function SharesPage() {
  const session = await auth()
  if (!session?.user?.id) redirect("/login")
  return (
    <div className="mx-auto max-w-5xl px-6 py-8 space-y-6">
      <div>
        <h1 className="text-2xl font-semibold">Shared buckets</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Share your connected buckets with other users without exposing your
          access keys, and see what has been shared with you.
        </p>
      </div>
      <SharesManager />
    </div>
  )
}
