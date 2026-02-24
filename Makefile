.PHONY: help \
cloud-setup cloud-start cloud-start-prod cloud-stop cloud-restart cloud-restart-full cloud-migrate cloud-local cloud-check-migrations cloud-reset \
community-setup community-start community-stop community-restart community-restart-full community-migrate community-local community-reset \
log log-worker stripe-listen

DC = docker compose --env-file .env -f docker/docker-compose.yml
DC_COMMUNITY = COMPOSE_PROJECT_NAME=s3admin-community ENVIRONMENT=COMMUNITY NEXT_PUBLIC_EDITION=community $(DC)
DC_CLOUD = COMPOSE_PROJECT_NAME=s3admin-cloud ENVIRONMENT=CLOUD NEXT_PUBLIC_EDITION=cloud $(DC)
PROFILE ?= community

help: ## Show available commands
	@echo ""
	@echo "  Cloud"
	@echo "  ──────────────────────────────────────"
	@grep -E '^cloud-[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Community"
	@echo "  ──────────────────────────────────────"
	@grep -E '^community-[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Utilities"
	@echo "  ──────────────────────────────────────"
	@grep -E '^(log|log-worker|stripe-listen):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─── Cloud ───────────────────────────────────────────────

cloud-setup: ## Build images, start DB, run migrations & seed for cloud mode
	@. ./.env 2>/dev/null; \
	if [ "$$POSTGRES_PASSWORD" = "password" ] || [ -z "$$POSTGRES_PASSWORD" ]; then \
	echo "ERROR: Set a strong POSTGRES_PASSWORD in .env"; exit 1; \
	fi
	$(DC_CLOUD) build app tools
	$(DC_CLOUD) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_CLOUD) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma migrate deploy
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma db seed
	@echo "\n✓ Cloud setup complete."

cloud-check-migrations: ## Verify cloud DB schema is up to date (fails if migrations are pending)
	$(DC_CLOUD) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_CLOUD) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma migrate status
	@echo "✓ Cloud migration status is healthy."

cloud-start: ## Validate migrations, then start cloud stack (app + worker + db + proxy)
	@. ./.env 2>/dev/null; \
	if [ "$$POSTGRES_PASSWORD" = "password" ] || [ -z "$$POSTGRES_PASSWORD" ]; then \
	echo "ERROR: Set a strong POSTGRES_PASSWORD in .env"; exit 1; \
	fi
	$(DC_CLOUD) build tools
	$(MAKE) cloud-check-migrations
	$(DC_CLOUD) up -d app worker db proxy
	@echo "✓ Cloud stack is running."

cloud-start-prod: ## Alias for cloud-start with migration validation
	@. ./.env 2>/dev/null; \
	if [ "$$POSTGRES_PASSWORD" = "password" ] || [ -z "$$POSTGRES_PASSWORD" ]; then \
	echo "ERROR: Set a strong POSTGRES_PASSWORD in .env"; exit 1; \
	fi
	$(DC_CLOUD) build tools
	$(MAKE) cloud-check-migrations
	$(DC_CLOUD) up -d app worker db proxy
	@echo "✓ Cloud stack is running."

cloud-stop: ## Stop cloud containers
	$(DC_CLOUD) down
	@echo "✓ Cloud stopped."

cloud-restart: ## Rebuild app, validate migrations, then restart app
	$(DC_CLOUD) build app
	$(DC_CLOUD) build tools
	$(MAKE) cloud-check-migrations
	$(DC_CLOUD) up -d app
	@echo "✓ Cloud app restarted."

cloud-restart-full: ## Rebuild app + tools, validate migrations, then restart app + worker
	$(DC_CLOUD) build app tools
	$(MAKE) cloud-check-migrations
	$(DC_CLOUD) up -d app worker
	@echo "✓ Cloud app + worker restarted."

cloud-reset: ## Reset cloud: destroy DB volume and restart fresh
	$(DC_CLOUD) down -v
	$(DC_CLOUD) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_CLOUD) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma migrate deploy
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma db seed
	@echo "✓ Cloud environment reset."

cloud-migrate: ## Run migrations & seed on cloud database
	$(DC_CLOUD) up db -d
	@until $(DC_CLOUD) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_CLOUD) build tools
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma migrate deploy
	$(DC_CLOUD) run --rm -T tools npx --no-install prisma db seed
	@echo "✓ Cloud migrations applied & seeded."

cloud-local: ## Start DB and run local Next.js in cloud mode using .env
	$(DC_CLOUD) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_CLOUD) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	@set -a; . ./.env; set +a; ENVIRONMENT=CLOUD NEXT_PUBLIC_EDITION=cloud npm run dev

# ─── Community ───────────────────────────────────────────

community-setup: ## Build app/tools images, start DB, run migrations & seed for community mode
	$(DC_COMMUNITY) build app tools
	$(DC_COMMUNITY) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_COMMUNITY) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_COMMUNITY) run --rm -T tools npx --no-install prisma migrate deploy
	$(DC_COMMUNITY) run --rm -T tools npx --no-install prisma db seed
	@echo "\n✓ Community setup complete."

community-start: ## Start community stack (app + worker + db + proxy)
	$(DC_COMMUNITY) up -d app worker db proxy
	@echo "✓ Community stack is running."

community-restart: ## Fast restart: rebuild app image and restart app
	$(DC_COMMUNITY) build app
	$(DC_COMMUNITY) up -d app
	@echo "✓ Community app restarted."

community-restart-full: ## Full restart: rebuild app + tools images and restart app + worker
	$(DC_COMMUNITY) build app tools
	$(DC_COMMUNITY) up -d app worker
	@echo "✓ Community app + worker restarted."

community-local: ## Start DB container and run local Next.js server using .env
	$(DC_COMMUNITY) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_COMMUNITY) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	@set -a; . ./.env; set +a; ENVIRONMENT=COMMUNITY NEXT_PUBLIC_EDITION=community npm run dev

community-stop: ## Stop community containers
	$(DC_COMMUNITY) down
	@echo "✓ Community stopped."

community-reset: ## Reset community: destroy DB volume and restart
	$(DC_COMMUNITY) down -v
	$(DC_COMMUNITY) up db -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(DC_COMMUNITY) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_COMMUNITY) run --rm -T tools npx --no-install prisma migrate deploy
	$(DC_COMMUNITY) run --rm -T tools npx --no-install prisma db seed
	@echo "✓ Community environment reset."

community-migrate: ## Run migrations & seed on community database
	$(DC_COMMUNITY) up db -d
	@until $(DC_COMMUNITY) exec -T db pg_isready -U s3admin -d s3_admin -q 2>/dev/null; do sleep 1; done
	$(DC_COMMUNITY) build tools
	$(DC_COMMUNITY) run --rm -T tools npx --no-install prisma migrate deploy
	$(DC_COMMUNITY) run --rm -T tools npx --no-install prisma db seed
	@echo "✓ Community migrations applied & seeded."

log: ## Tail app service logs (set PROFILE=community|cloud, default: community)
	@if [ "$(PROFILE)" = "cloud" ]; then \
	$(DC_CLOUD) logs -f --tail=200 app; \
	elif [ "$(PROFILE)" = "community" ]; then \
	$(DC_COMMUNITY) logs -f --tail=200 app; \
	else \
	echo "ERROR: PROFILE must be either 'community' or 'cloud' (got: $(PROFILE))"; \
	exit 1; \
	fi

log-worker: ## Tail worker service logs (set PROFILE=community|cloud, default: community)
	@if [ "$(PROFILE)" = "cloud" ]; then \
	$(DC_CLOUD) logs -f --tail=200 worker; \
	elif [ "$(PROFILE)" = "community" ]; then \
	$(DC_COMMUNITY) logs -f --tail=200 worker; \
	else \
	echo "ERROR: PROFILE must be either 'community' or 'cloud' (got: $(PROFILE))"; \
	exit 1; \
	fi

stripe-listen: ## Listen Stripe webhooks and forward to AUTH_URL/api/stripe/webhook
	@command -v stripe >/dev/null 2>&1 || { echo "ERROR: Stripe CLI not found. Install from https://docs.stripe.com/stripe-cli"; exit 1; }
	@set -a; . ./.env; set +a; \
	BASE_URL="$${AUTH_URL:-$${NEXT_PUBLIC_SITE_URL:-http://localhost}}"; \
	case "$$BASE_URL" in \
		http://*|https://*) ;; \
		*) BASE_URL="http://$$BASE_URL" ;; \
	esac; \
	BASE_URL="$${BASE_URL%/}"; \
	echo "Forwarding Stripe webhooks to $$BASE_URL/api/stripe/webhook"; \
	stripe listen --forward-to "$$BASE_URL/api/stripe/webhook"
