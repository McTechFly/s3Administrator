import { spawn } from "node:child_process"
import { createGzip } from "node:zlib"
import { Readable, PassThrough } from "node:stream"
import { S3Client, DeleteObjectCommand } from "@aws-sdk/client-s3"
import { Upload } from "@aws-sdk/lib-storage"
import { prisma } from "@/lib/db"

export interface BackupConfig {
  endpoint: string
  accessKey: string
  secretKey: string
  bucket: string
  scheduleCron: string
}

export function getBackupConfig(): BackupConfig | null {
  const endpoint = (process.env.BACKUP_S3_ENDPOINT ?? "").trim()
  const accessKey = (process.env.BACKUP_S3_ACCESS_KEY ?? "").trim()
  const secretKey = (process.env.BACKUP_S3_SECRET_KEY ?? "").trim()
  const bucket = (process.env.BACKUP_S3_BUCKET ?? "").trim()

  if (!endpoint || !accessKey || !secretKey || !bucket) return null

  return {
    endpoint,
    accessKey,
    secretKey,
    bucket,
    scheduleCron: (process.env.BACKUP_SCHEDULE_CRON ?? "0 */3 * * *").trim(),
  }
}

function buildS3Client(config: BackupConfig): S3Client {
  const endpoint = config.endpoint.startsWith("http")
    ? config.endpoint
    : `https://${config.endpoint}`

  return new S3Client({
    endpoint,
    region: "us-east-1",
    credentials: {
      accessKeyId: config.accessKey,
      secretAccessKey: config.secretKey,
    },
    forcePathStyle: true,
  })
}

export async function runBackup(): Promise<void> {
  const config = getBackupConfig()
  if (!config) {
    throw new Error("Backup is not configured — set BACKUP_S3_ENDPOINT, BACKUP_S3_ACCESS_KEY, BACKUP_S3_SECRET_KEY, BACKUP_S3_BUCKET")
  }

  const databaseUrl = process.env.DATABASE_URL
  if (!databaseUrl) throw new Error("DATABASE_URL is not set")

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-")
  const key = `backups/${timestamp}.sql.gz`

  let sizeBytes = 0

  try {
    const s3 = buildS3Client(config)
    const passthrough = new PassThrough()

    // Count bytes as they pass through
    passthrough.on("data", (chunk: Buffer) => {
      sizeBytes += chunk.length
    })

    const pg = spawn("pg_dump", [databaseUrl, "--no-password", "-Fc"], {
      stdio: ["ignore", "pipe", "pipe"],
    })

    let pgError = ""
    pg.stderr.on("data", (d: Buffer) => {
      pgError += d.toString()
    })

    const gzip = createGzip()
    const sourceStream = Readable.from(pg.stdout)
    sourceStream.pipe(gzip).pipe(passthrough)

    const upload = new Upload({
      client: s3,
      params: {
        Bucket: config.bucket,
        Key: key,
        Body: passthrough,
        ContentType: "application/gzip",
      },
    })

    await upload.done()

    // Wait for pg_dump to exit cleanly
    await new Promise<void>((resolve, reject) => {
      pg.on("close", (code) => {
        if (code === 0) resolve()
        else reject(new Error(`pg_dump exited with code ${code}: ${pgError.trim()}`))
      })
      pg.on("error", reject)
    })

    await prisma.backup.create({
      data: { key, sizeBytes: BigInt(sizeBytes), status: "ok" },
    })
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    await prisma.backup.create({
      data: { key, sizeBytes: BigInt(sizeBytes), status: "failed", error: message.slice(0, 500) },
    })
    throw err
  }
}

export async function deleteBackupFromS3(key: string): Promise<void> {
  const config = getBackupConfig()
  if (!config) throw new Error("Backup S3 not configured")

  const s3 = buildS3Client(config)
  await s3.send(new DeleteObjectCommand({ Bucket: config.bucket, Key: key }))
}
