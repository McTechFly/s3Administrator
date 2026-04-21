import Link from "next/link"

export default function NotAvailablePage() {
  return (
    <div className="mx-auto max-w-xl px-6 py-16 text-center">
      <h1 className="text-2xl font-semibold mb-2">Not available</h1>
      <p className="text-sm text-muted-foreground mb-6">This page is not available in the self-hosted multi-user edition.</p>
      <Link href="/dashboard" className="text-sm text-primary hover:underline">
        Back to dashboard
      </Link>
    </div>
  )
}
