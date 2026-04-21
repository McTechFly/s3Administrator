/** Local analytics-consent shim. */

export type ConsentChoice = "all" | "analytics" | "essential"

export const CONSENT_STORAGE_KEY = "s3a-consent"

export const CONSENT_BY_CHOICE: Record<
  ConsentChoice,
  Record<string, "granted" | "denied">
> = {
  all: {
    ad_storage: "granted",
    analytics_storage: "granted",
    ad_user_data: "granted",
    ad_personalization: "granted",
  },
  analytics: {
    ad_storage: "denied",
    analytics_storage: "granted",
    ad_user_data: "denied",
    ad_personalization: "denied",
  },
  essential: {
    ad_storage: "denied",
    analytics_storage: "denied",
    ad_user_data: "denied",
    ad_personalization: "denied",
  },
}

export const EEA_COUNTRY_CODES: readonly string[] = [
  "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT",
  "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE","IS","LI","NO",
]
