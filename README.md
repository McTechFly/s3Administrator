# S3 Administrator

Open-source S3 file manager for Hetzner, AWS, Cloudflare R2, and any S3-compatible storage provider. Browse, upload, download, move, and manage files across multiple buckets and providers from a single dashboard.


## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=s3administrator/s3Administrator&type=Date)](https://star-history.com/#s3administrator/s3Administrator&Date)

## Features

- **File Management** - Browse, upload, download, delete, move, and rename files
- **Multi-Provider** - Connect to AWS S3, Hetzner Object Storage, Cloudflare R2, or any S3-compatible endpoint
- **Multi-Bucket** - Manage multiple buckets across multiple credentials
- **Gallery View** - Visual gallery with image previews and video thumbnail generation
- **Background Tasks** - Copy, move, sync, and migrate files between buckets
- **Global Search** - Search across all indexed files and buckets
- **Folder Operations** - Create folders, recursive delete, batch operations
- **Encrypted Credentials** - S3 keys are encrypted at rest with AES-256-GCM
- **Self-Hosted** - Run on your own infrastructure with Docker Compose

## Quick Start

```bash
git clone https://github.com/s3administrator/s3Administrator.git
cd s3Administrator

# Copy the example env and fill in the values
cp .env.community.example .env

# Generate required secrets
openssl rand -base64 32   # → paste into AUTH_SECRET
openssl rand -hex 32      # → paste into ENCRYPTION_MASTER_KEY
openssl rand -hex 16      # → paste into ENCRYPTION_SALT

# Build, migrate, and seed
make community-setup

# Start the stack
make community-start

# Open http://localhost
```

## Community vs Cloud

| Feature | Community (Self-Hosted) | Cloud (s3administrator.com) |
|---------|:-:|:-:|
| File browsing & management | Yes | Yes |
| Multi-provider & multi-bucket | Yes | Yes |
| Gallery view & thumbnails | Yes | Yes |
| Background tasks & sync | Yes | Yes |
| Global file search | Yes | Yes |
| Folder operations | Yes | Yes |
| Encrypted credentials | Yes | Yes |
| **Multi-user auth (OAuth, email)** | - | Yes |
| **Billing & subscription plans** | - | Yes |
| **Audit logs** | - | Yes |
| **Admin panel** | - | Yes |
| **Managed hosting & updates** | - | Yes |
| **Support & SLA** | - | Yes |

The community edition is a single-user, no-auth tool designed for personal or internal use. All S3 management features are fully unlocked.

For multi-user support, audit logs, and managed hosting, see [s3administrator.com](https://www.s3administrator.com).

## Make Commands

Run `make help` to see all available commands.

### Community

| Command | Description |
|---------|-------------|
| `make community-setup` | Build images, start DB, run migrations & seed |
| `make community-start` | Start the stack (app + worker + db + proxy) |
| `make community-stop` | Stop containers |
| `make community-restart` | Rebuild and restart app |
| `make community-restart-full` | Rebuild and restart app + worker |
| `make community-migrate` | Run migrations & seed |
| `make community-local` | Start DB and run Next.js locally |
| `make community-reset` | Destroy DB volume and start fresh |

### Cloud

| Command | Description |
|---------|-------------|
| `make cloud-setup` | Build images, start DB, run migrations & seed |
| `make cloud-start` | Start the stack (app + worker + db + proxy) |
| `make cloud-stop` | Stop containers |
| `make cloud-restart` | Rebuild and restart app |
| `make cloud-restart-full` | Rebuild and restart app + worker |
| `make cloud-migrate` | Run migrations & seed |
| `make cloud-local` | Start DB and run Next.js locally in cloud mode |

### Utilities

| Command | Description |
|---------|-------------|
| `make log PROFILE=community` | Tail app logs |
| `make log-worker PROFILE=community` | Tail worker logs |
| `make stripe-listen` | Forward Stripe webhooks locally |

## Development

```bash
# Copy env and fill in values
cp .env.community.example .env

# Start DB, install deps, run locally
make community-local

## Gallery Mode and Video Thumbnails

The dashboard supports List and Gallery view modes.

- Gallery uses infinite scrolling with recursive listing
- Image previews loaded from signed S3 URLs
- Video thumbnails generated asynchronously via ffmpeg

Optional environment variables for thumbnail storage:

```bash
THUMBNAIL_S3_ENDPOINT=
THUMBNAIL_S3_REGION=
THUMBNAIL_S3_ACCESS_KEY=
THUMBNAIL_S3_SECRET_KEY=
THUMBNAIL_S3_BUCKET=
THUMBNAIL_MAX_WIDTH=480
THUMBNAIL_URL_TTL_SECONDS=3600
```

## Security Scanning (pre-commit)

Pre-commit hooks are configured in `.pre-commit-config.yaml`:

- Semgrep (`p/security-audit`)
- OSV scanner (`package-lock.json`)
- SonarQube (optional, runs when env vars are set)

```bash
pre-commit install
pre-commit run --all-files
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE).
