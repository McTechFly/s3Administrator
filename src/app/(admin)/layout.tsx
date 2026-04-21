import { redirect } from "next/navigation"
import Link from "next/link"
import { auth } from "@/lib/auth"
import { isAdmin } from "@/lib/permissions"

const NAV = [
  { href: "/admin", label: "Overview" },
  { href: "/admin/users", label: "Users" },
]

export default async function AdminLayout({ children }: { children: React.ReactNode }) {
  const session = await auth()
  if (!session?.user?.id) redirect("/login")
  if (!isAdmin(session.user.role)) redirect("/dashboard")

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b">
        <div className="mx-auto max-w-6xl px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-6">
            <Link href="/dashboard" className="font-semibold">← Dashboard</Link>
            <nav className="flex items-center gap-4 text-sm text-muted-foreground">
              {NAV.map((n) => (
                <Link key={n.href} href={n.href} className="hover:text-foreground transition-colors">
                  {n.label}
                </Link>
              ))}
            </nav>
          </div>
          <span className="text-xs px-2 py-1 rounded-md bg-primary/10 text-primary border border-primary/20">
            Admin
          </span>
        </div>
      </div>
      <main className="mx-auto max-w-6xl px-6 py-8">{children}</main>
    </div>
  )
}
