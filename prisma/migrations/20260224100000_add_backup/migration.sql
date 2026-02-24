-- CreateTable
CREATE TABLE "Backup" (
    "id" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "sizeBytes" BIGINT NOT NULL DEFAULT 0,
    "status" TEXT NOT NULL DEFAULT 'ok',
    "error" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Backup_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Backup_key_key" ON "Backup"("key");

-- CreateIndex
CREATE INDEX "Backup_createdAt_idx" ON "Backup"("createdAt" DESC);
