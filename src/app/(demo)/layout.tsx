import { absoluteUrl } from "@/lib/site-url"
import { DemoLayoutShell } from "@/components/demo/demo-layout-shell"

export default function DemoLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <DemoLayoutShell signupUrl={absoluteUrl("/login")}>
      {children}
    </DemoLayoutShell>
  )
}
