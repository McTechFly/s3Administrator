import Link from "next/link"

export default function NotAvailablePage() {
  return (
    <div className="mx-auto max-w-xl px-6 py-16 text-center">
      <h1 className="text-2xl font-semibold mb-2">Coming soon</h1>
      <p className="text-sm text-muted-foreground mb-6">This admin page is not yet implemented in the self-hosted multi-user edition.</p>
      <Link href="/dashboard" className="text-sm text-primary hover:underline">
        Back to dashboard
      </Link>
    </div>
  )
}
