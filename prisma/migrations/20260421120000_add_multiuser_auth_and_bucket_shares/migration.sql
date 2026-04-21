-- Multi-user self-hosted auth + direct bucket sharing

-- User: password hash (bcrypt), activation flag, last login timestamp
ALTER TABLE "User"
  ADD COLUMN "passwordHash" TEXT,
  ADD COLUMN "isActive"     BOOLEAN NOT NULL DEFAULT true,
  ADD COLUMN "lastLoginAt"  TIMESTAMP(3);

-- Direct user→user bucket share
CREATE TABLE "BucketShare" (
  "id"              TEXT NOT NULL,
  "credentialId"    TEXT NOT NULL,
  "ownerUserId"     TEXT NOT NULL,
  "targetUserId"    TEXT NOT NULL,
  "bucket"          TEXT,
  "permissionLevel" TEXT NOT NULL DEFAULT 'read',
  "createdAt"       TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt"       TIMESTAMP(3) NOT NULL,

  CONSTRAINT "BucketShare_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "BucketShare_credentialId_targetUserId_bucket_key"
  ON "BucketShare"("credentialId", "targetUserId", "bucket");

CREATE INDEX "BucketShare_targetUserId_idx" ON "BucketShare"("targetUserId");
CREATE INDEX "BucketShare_ownerUserId_idx"  ON "BucketShare"("ownerUserId");
CREATE INDEX "BucketShare_credentialId_idx" ON "BucketShare"("credentialId");

ALTER TABLE "BucketShare"
  ADD CONSTRAINT "BucketShare_credentialId_fkey"
  FOREIGN KEY ("credentialId") REFERENCES "S3Credential"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "BucketShare"
  ADD CONSTRAINT "BucketShare_ownerUserId_fkey"
  FOREIGN KEY ("ownerUserId") REFERENCES "User"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "BucketShare"
  ADD CONSTRAINT "BucketShare_targetUserId_fkey"
  FOREIGN KEY ("targetUserId") REFERENCES "User"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;
