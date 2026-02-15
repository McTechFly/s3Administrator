CREATE TABLE "BackgroundTaskRun" (
  "id" TEXT NOT NULL,
  "taskId" TEXT NOT NULL,
  "userId" TEXT NOT NULL,
  "runNumber" INTEGER NOT NULL,
  "attempt" INTEGER NOT NULL DEFAULT 1,
  "status" TEXT NOT NULL,
  "startedAt" TIMESTAMP(3) NOT NULL,
  "finishedAt" TIMESTAMP(3),
  "error" TEXT,
  "metrics" JSONB,
  "workerId" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "BackgroundTaskRun_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "BackgroundTaskEvent" (
  "id" TEXT NOT NULL,
  "taskId" TEXT NOT NULL,
  "runId" TEXT,
  "userId" TEXT NOT NULL,
  "at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "eventType" TEXT NOT NULL,
  "message" TEXT NOT NULL,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "BackgroundTaskEvent_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "BackgroundTaskRun_taskId_runNumber_key"
  ON "BackgroundTaskRun"("taskId", "runNumber");

CREATE INDEX "BackgroundTaskRun_taskId_runNumber_idx"
  ON "BackgroundTaskRun"("taskId", "runNumber" DESC);

CREATE INDEX "BackgroundTaskRun_userId_startedAt_idx"
  ON "BackgroundTaskRun"("userId", "startedAt" DESC);

CREATE INDEX "BackgroundTaskRun_startedAt_idx"
  ON "BackgroundTaskRun"("startedAt");

CREATE INDEX "BackgroundTaskEvent_runId_at_idx"
  ON "BackgroundTaskEvent"("runId", "at" DESC);

CREATE INDEX "BackgroundTaskEvent_taskId_at_idx"
  ON "BackgroundTaskEvent"("taskId", "at" DESC);

CREATE INDEX "BackgroundTaskEvent_userId_at_idx"
  ON "BackgroundTaskEvent"("userId", "at" DESC);

CREATE INDEX "BackgroundTask_due_active_created_idx"
  ON "BackgroundTask"("nextRunAt", "createdAt", "userId")
  WHERE "lifecycleState" = 'active' AND "status" IN ('pending', 'in_progress');

CREATE INDEX "FileMetadata_userId_credentialId_bucket_key_idx"
  ON "FileMetadata"("userId", "credentialId", "bucket", "key");

CREATE INDEX "FileMetadata_userId_bucket_key_idx"
  ON "FileMetadata"("userId", "bucket", "key");

DO $$
BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
EXCEPTION
  WHEN insufficient_privilege THEN
    RAISE NOTICE 'Skipping pg_trgm extension creation due to insufficient privilege';
END
$$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
    CREATE INDEX "FileMetadata_filename_trgm_idx"
      ON "FileMetadata"
      USING GIN (LOWER(regexp_replace("key", '^.*/', '')) gin_trgm_ops);
  END IF;
END
$$;

ALTER TABLE "BackgroundTaskRun"
  ADD CONSTRAINT "BackgroundTaskRun_taskId_fkey"
  FOREIGN KEY ("taskId") REFERENCES "BackgroundTask"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "BackgroundTaskRun"
  ADD CONSTRAINT "BackgroundTaskRun_userId_fkey"
  FOREIGN KEY ("userId") REFERENCES "User"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "BackgroundTaskEvent"
  ADD CONSTRAINT "BackgroundTaskEvent_taskId_fkey"
  FOREIGN KEY ("taskId") REFERENCES "BackgroundTask"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "BackgroundTaskEvent"
  ADD CONSTRAINT "BackgroundTaskEvent_runId_fkey"
  FOREIGN KEY ("runId") REFERENCES "BackgroundTaskRun"("id")
  ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE "BackgroundTaskEvent"
  ADD CONSTRAINT "BackgroundTaskEvent_userId_fkey"
  FOREIGN KEY ("userId") REFERENCES "User"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;
