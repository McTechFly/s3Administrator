import { defineConfig } from "prisma/config"
import { PrismaPg } from "@prisma/adapter-pg"

export default defineConfig({
  earlyAccess: true,
  schema: "prisma/schema.prisma",
  migrate: {
    async adapter(env) {
      return new PrismaPg(env.DATABASE_URL)
    },
  },
})
