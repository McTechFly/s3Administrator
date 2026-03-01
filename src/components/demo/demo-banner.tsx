"use client"

export function DemoBanner() {
  return (
    <div className="sticky top-0 z-50 border-b bg-amber-50 px-4 py-2 text-center text-sm dark:bg-amber-950/30">
      You are viewing a <strong>read-only demo</strong>.{" "}
      <a
        href="https://s3administrator.com/pricing"
        className="underline hover:text-amber-900 dark:hover:text-amber-200"
      >
        Sign up free
      </a>{" "}
      to manage your own S3 storage.
    </div>
  )
}
