/** Local no-op SEO landing pages config. */

export type SeoLandingCategory = string

export type SeoLandingFaqItem = { question: string; answer: string }
export type SeoLandingLink = { href: string; label: string }

export type SeoLandingPageConfig = {
  slug: string
  title: string
  description: string
  category: SeoLandingCategory
  keywords: string[]
  h1: string
  intro: string
  problemPoints: string[]
  solutionPoints: string[]
  proofPoints: string[]
  relatedLinks: SeoLandingLink[]
  faq: SeoLandingFaqItem[]
  hero?: { heading: string; subheading?: string }
  sections?: unknown[]
}

export const seoLandingPages: SeoLandingPageConfig[] = []

export default { seoLandingPages }
