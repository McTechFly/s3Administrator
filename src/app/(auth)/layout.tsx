import Link from "next/link"
import Image from "next/image"
import { ThemeSwitcher } from "@/components/theme-switcher"

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen flex flex-col bg-background">
      <header className="flex items-center justify-between px-6 py-4 border-b">
        <Link href="/" className="flex items-center gap-2 font-semibold">
          <Image src="/icon.svg" alt="S3 Administrator" width={24} height={24} />
          <span>S3 Administrator</span>
        </Link>
        <ThemeSwitcher />
      </header>
      <main className="flex-1 flex items-center justify-center px-4 py-10">
        <div className="w-full max-w-md">{children}</div>
      </main>
    </div>
  )
}
