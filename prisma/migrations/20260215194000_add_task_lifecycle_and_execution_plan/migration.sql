ALTER TABLE "BackgroundTask"
  ADD COLUMN "lifecycleState" TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN "executionPlan" JSONB,
  ADD COLUMN "executionHistory" JSONB,
  ADD COLUMN "runCount" INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN "dedupeKey" TEXT,
  ADD COLUMN "isRecurring" BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN "scheduleIntervalSeconds" INTEGER,
  ADD COLUMN "lastRunAt" TIMESTAMP(3),
  ADD COLUMN "pausedAt" TIMESTAMP(3);

CREATE INDEX "BackgroundTask_userId_lifecycleState_status_nextRunAt_idx"
  ON "BackgroundTask"("userId", "lifecycleState", "status", "nextRunAt");

CREATE INDEX "BackgroundTask_userId_dedupeKey_lifecycleState_status_idx"
  ON "BackgroundTask"("userId", "dedupeKey", "lifecycleState", "status");
