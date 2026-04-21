import Link from "next/link"

export default function NotAvailablePage() {
  return (
    <div className="mx-auto max-w-xl px-6 py-16 text-center">
      <h1 className="text-2xl font-semibold mb-2">Billing disabled</h1>
      <p className="text-sm text-muted-foreground mb-6">Billing is handled by the hosting administrator in the self-hosted edition.</p>
      <Link href="/dashboard" className="text-sm text-primary hover:underline">
        Back to dashboard
      </Link>
    </div>
  )
}
