import type { Metadata } from "next"
import { ProfilePageClient } from "@/components/dashboard/profile-page-client"

export const metadata: Metadata = { title: "Profile" }

export default function ProfilePage() {
  return <ProfilePageClient />
}
