-- Add dynamic pricing fields to Subscription
ALTER TABLE "Subscription" ADD COLUMN "customFileLimit" INTEGER;
ALTER TABLE "Subscription" ADD COLUMN "customStorageLimitBytes" BIGINT;
ALTER TABLE "Subscription" ADD COLUMN "customBucketLimit" INTEGER;
ALTER TABLE "Subscription" ADD COLUMN "dynamicStripePriceId" TEXT;

-- Add dynamic pricing fields to OrganizationSubscription
ALTER TABLE "OrganizationSubscription" ADD COLUMN "customFileLimit" INTEGER;
ALTER TABLE "OrganizationSubscription" ADD COLUMN "customStorageLimitBytes" BIGINT;
ALTER TABLE "OrganizationSubscription" ADD COLUMN "customBucketLimit" INTEGER;
ALTER TABLE "OrganizationSubscription" ADD COLUMN "dynamicStripePriceId" TEXT;
ALTER TABLE "OrganizationSubscription" ADD COLUMN "baseFileLimit" INTEGER;
ALTER TABLE "OrganizationSubscription" ADD COLUMN "baseStorageLimitGb" INTEGER;
