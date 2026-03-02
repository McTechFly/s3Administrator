-- AlterTable: add type column to Plan
ALTER TABLE "Plan" ADD COLUMN "type" TEXT NOT NULL DEFAULT 'individual';

-- Backfill: mark existing plans with seatPriceMonthly as team plans
UPDATE "Plan" SET "type" = 'team' WHERE "seatPriceMonthly" IS NOT NULL AND "seatPriceMonthly" > 0;
