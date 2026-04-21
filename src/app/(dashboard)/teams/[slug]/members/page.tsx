import Link from "next/link"

export default function NotAvailablePage() {
  return (
    <div className="mx-auto max-w-xl px-6 py-16 text-center">
      <h1 className="text-2xl font-semibold mb-2">Teams not available</h1>
      <p className="text-sm text-muted-foreground mb-6">Organizations / teams are not enabled in this edition. Use direct user-to-user bucket shares instead.</p>
      <Link href="/dashboard" className="text-sm text-primary hover:underline">
        Back to dashboard
      </Link>
    </div>
  )
}
