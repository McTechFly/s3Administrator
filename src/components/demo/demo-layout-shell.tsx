"use client"

import { Suspense, useState } from "react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { PanelLeft } from "lucide-react"
import { DemoSidebar } from "@/components/demo/demo-sidebar"
import { DemoBanner } from "@/components/demo/demo-banner"
import { Button } from "@/components/ui/button"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"

function DemoLayoutContent({
  children,
  signupUrl,
}: {
  children: React.ReactNode
  signupUrl: string
}) {
  const [mobileNavOpen, setMobileNavOpen] = useState(false)

  return (
    <div className="flex h-screen flex-col overflow-hidden">
      <DemoBanner signupUrl={signupUrl} />
      <div className="flex min-h-0 flex-1">
        <aside className="hidden md:block">
          <Suspense>
            <DemoSidebar signupUrl={signupUrl} />
          </Suspense>
        </aside>
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <header className="flex h-12 items-center border-b bg-background/95 px-2.5 md:hidden">
            <Sheet open={mobileNavOpen} onOpenChange={setMobileNavOpen}>
              <SheetTrigger asChild>
                <Button variant="ghost" size="icon" className="h-8 w-8">
                  <PanelLeft className="h-4 w-4" />
                  <span className="sr-only">Open navigation</span>
                </Button>
              </SheetTrigger>
              <SheetContent
                side="left"
                showCloseButton={false}
                className="z-[90] w-[84vw] max-w-[320px] border-r p-0"
              >
                <SheetTitle className="sr-only">Navigation</SheetTitle>
                <SheetDescription className="sr-only">
                  Demo sidebar navigation.
                </SheetDescription>
                <Suspense>
                  <DemoSidebar
                    signupUrl={signupUrl}
                    className="w-full border-r-0 lg:w-full"
                    collapsible={false}
                  />
                </Suspense>
              </SheetContent>
            </Sheet>
            <p className="ml-2 text-sm font-semibold">S3 Administrator Demo</p>
          </header>
          <main className="flex-1 overflow-auto">{children}</main>
        </div>
      </div>
    </div>
  )
}

const queryClient = new QueryClient()

export function DemoLayoutShell({
  children,
  signupUrl,
}: {
  children: React.ReactNode
  signupUrl: string
}) {
  return (
    <QueryClientProvider client={queryClient}>
      <DemoLayoutContent signupUrl={signupUrl}>{children}</DemoLayoutContent>
    </QueryClientProvider>
  )
}
