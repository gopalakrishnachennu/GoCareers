# Deploy on Oracle (Always Free) + Coolify

This repo supports a $0 deployment by running Docker services on an Oracle Always Free VM and managing them via Coolify.

## What gets deployed

- **Django web** (Gunicorn) on port `8000`
- **Postgres** (container)
- **Redis** (container)
- **Celery worker** (container)
- **Celery beat** (container)

## DNS (Namecheap)

Create A records:

- `@` → your VM public IPv4
- `www` → same IP (optional)

## Environment variables (Coolify)

Use these in the Coolify UI (do not commit secrets):

- `DEBUG=0`
- `SECRET_KEY=<strong random>`
- `ALLOWED_HOSTS=chennu.co,www.chennu.co`
- `CSRF_TRUSTED_ORIGINS=https://chennu.co,https://www.chennu.co`
- `SITE_URL=https://chennu.co`
- `DATABASE_URL=postgres://…` (Coolify-managed Postgres)
- `CELERY_BROKER_URL=redis://…`
- `CELERY_RESULT_BACKEND=redis://…`
- `CELERY_TASK_ALWAYS_EAGER=0`

Template: `.env.production.example`

## Deploy options

### Option A: Coolify builds from repo (simplest)

- Configure the application to build a Docker image from this repo.
- Start command is handled by `scripts/entrypoint.sh` (runs migrations + collectstatic + Gunicorn).

### Option B: Use docker-compose.prod.yml (stack-style)

- Deploy `docker-compose.prod.yml` so Coolify runs:
  - `web`, `db`, `redis`, `celery_worker`, `celery_beat`

## Rollback strategy

Recommended: publish images to GHCR and deploy a pinned tag.

- This repo includes a GitHub workflow: `.github/workflows/docker-image.yml`
- It publishes images tagged by:
  - commit SHA (`sha-…`)
  - release tag (`vX.Y.Z`)

In Coolify, switch the deployed image tag to a previous one to roll back.

