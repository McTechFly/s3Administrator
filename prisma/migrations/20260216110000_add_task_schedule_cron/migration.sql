ALTER TABLE "BackgroundTask"
  ADD COLUMN "scheduleCron" TEXT;

UPDATE "BackgroundTask"
SET "scheduleCron" = '* * * * *'
WHERE "isRecurring" = true
  AND "scheduleCron" IS NULL
  AND "scheduleIntervalSeconds" = 60;
