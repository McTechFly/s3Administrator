/**
 * Local no-op replacement for the cloud stub: pricing-configurator.tsx
 * The feature this helper backed is not available in the self-hosted edition.
 */
/* eslint-disable @typescript-eslint/no-explicit-any */
export const __unavailable = true
const handler = {
  get: (_t: any, prop: string) => {
    return (..._args: any[]) => {
      throw new Error(
        `[${'pricing-configurator.tsx'}] not available in self-hosted edition — called "${prop}"`,
      )
    }
  },
}
const proxy: any = new Proxy({}, handler)
export default proxy
