# Database Backup Plan — Cloudflare R2 (Zero Cost)

**Status:** PARKED. Resume after Mapbox/country resolution work is complete.

**Goal:** Free, automated, encrypted, offsite Postgres backups with 14-day retention.

**Total ongoing cost:** $0 / month (Cloudflare R2 free tier).

---

## Why Cloudflare R2 Free Tier

| Resource | Free quota | Our usage | Headroom |
|---|---|---|---|
| Storage | 10 GB | ~5 GB (14 daily snapshots, compressed + encrypted) | 50% |
| Class A ops (writes) | 1,000,000 / month | ~30 (one daily upload) | 99.997% |
| Class B ops (reads) | 10,000,000 / month | ~0 (only on restore) | 100% |
| **Egress / downloads** | **Unlimited** | n/a | ∞ |

A 1 GB raw Postgres dump compresses to ~250 MB after `gzip` + `gpg`. 14 days × 250 MB = 3.5 GB → safely inside 10 GB free tier.

---

## Why Not Other Options

| Option | Why rejected |
|---|---|
| Google Drive | ToS gray area for automated DB backups, OAuth token expiry, rate limits, slow restores |
| Hetzner Storage Box | €3.20/month — not free |
| Hetzner VPS snapshots | €0.012/GB/month — not free, single-vendor risk |
| Backblaze B2 | $0.006/GB/mo + egress fees — cheaper than nothing but R2 free tier wins for this size |
| AWS S3 / DO Spaces | Higher per-GB cost, not free |
| Local-only backup on Hetzner VPS | Free, but zero offsite protection — same disk failure kills both |

---

## Architecture

```
┌──────────────────┐    cron 3am    ┌──────────────────────────┐
│ Hetzner VPS      │ ────────────► │ Postgres pg_dump         │
│ (consulting-db)  │                │ → gzip                    │
└──────────────────┘                │ → gpg --symmetric         │
                                    │ → tee                     │
                                    │   ├─► /opt/backups/local  │ (14d local retention)
                                    │   └─► R2 S3 API           │ (14d offsite retention)
                                    └──────────────────────────┘
                                                ▲
                                                │ aws s3 cp
                                                ▼
                                    ┌──────────────────────────┐
                                    │ Cloudflare R2 bucket     │
                                    │ chenn-db-backups/db/     │
                                    │   2026-05-07.sql.gz.gpg  │
                                    │   2026-05-08.sql.gz.gpg  │
                                    │   ...                     │
                                    └──────────────────────────┘
```

3-2-1 backup rule:
- **3 copies** — production DB + Hetzner local + R2
- **2 different media** — Hetzner local disk + Cloudflare R2 object storage
- **1 offsite** — R2 (different vendor entirely)

---

## One-Time Setup (10 minutes, $0)

### Step 1 — Create Cloudflare account
- https://dash.cloudflare.com → Sign up (free, no credit card)
- Skip plan selection (free Workers / Pages plan is fine)

### Step 2 — Create R2 bucket
- R2 sidebar → Create bucket
- Name: `chenn-db-backups`
- Location: `Automatic` (or pick EU for GDPR alignment)
- Click Create — bucket is empty and ready

### Step 3 — Create API token
- R2 → "Manage R2 API Tokens" → Create API token
- Name: `chenn-db-backup-writer`
- Permissions: `Object Read & Write`
- Specify bucket: `chenn-db-backups` (scope to one bucket only)
- TTL: forever (or rotate yearly)
- Click Create — Cloudflare shows you these ONE TIME:
  - **Access Key ID**
  - **Secret Access Key**
  - **Endpoint URL** (looks like `https://<account-id>.r2.cloudflarestorage.com`)
  - **Account ID**
- Save them in your password manager immediately.

### Step 4 — Generate backup encryption passphrase
- Use a 32+ character random string. Recommended: 1Password / Bitwarden generator.
- Store this passphrase in 2 places: password manager AND a sealed envelope in a safe.
- If you lose this passphrase, **the backups become permanently unreadable**. There is no recovery.

### Step 5 — Add 5 GitHub repository secrets
GitHub → repo → Settings → Secrets and variables → Actions → New repository secret:

| Secret name | Value |
|---|---|
| `R2_ACCESS_KEY_ID` | from Step 3 |
| `R2_SECRET_ACCESS_KEY` | from Step 3 |
| `R2_ENDPOINT` | from Step 3 (e.g. `https://abc123.r2.cloudflarestorage.com`) |
| `R2_BUCKET` | `chenn-db-backups` |
| `BACKUP_GPG_PASSPHRASE` | the strong passphrase from Step 4 |

When all 5 secrets exist, the workflow is ready to wire up.

---

## Workflows To Build

### Workflow 1 — Daily backup (cron)
**File:** `.github/workflows/backup-database.yml`
**Schedule:** `cron: "0 3 * * *"` (3 AM UTC daily)
**Triggers:** scheduled + manual `workflow_dispatch`

```yaml
name: Backup — Database to R2 (daily)
on:
  schedule:
    - cron: "0 3 * * *"
  workflow_dispatch:

jobs:
  backup:
    runs-on: ubuntu-latest
    steps:
      - uses: appleboy/ssh-action@v1.0.3
        with:
          host: ${{ secrets.VPS_HOST }}
          username: ${{ secrets.VPS_USER }}
          key: ${{ secrets.VPS_SSH_KEY }}
          command_timeout: 30m
          script_stop: true
          envs: R2_ACCESS_KEY_ID,R2_SECRET_ACCESS_KEY,R2_ENDPOINT,R2_BUCKET,BACKUP_GPG_PASSPHRASE
          script: |
            set -euo pipefail
            cd /opt/consulting

            # Sanity: ensure backup dir + tools exist
            mkdir -p /opt/backups/db
            which aws >/dev/null 2>&1 || { sudo apt-get install -y awscli; }
            which gpg >/dev/null 2>&1 || { sudo apt-get install -y gnupg; }

            STAMP=$(date +%F)
            FILE=/opt/backups/db/${STAMP}.sql.gz.gpg

            # 1. Dump → compress → encrypt → tee local + upload R2
            docker compose -f docker-compose.prod.yml --env-file .env.production \
              exec -T db pg_dump -U postgres consulting \
              | gzip -9 \
              | gpg --batch --yes --symmetric --cipher-algo AES256 \
                    --passphrase "$BACKUP_GPG_PASSPHRASE" \
              > "$FILE"

            # 2. Upload to R2
            export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
            export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
            aws s3 cp "$FILE" "s3://${R2_BUCKET}/db/${STAMP}.sql.gz.gpg" \
              --endpoint-url "$R2_ENDPOINT"

            # 3. Local retention: 14 days
            find /opt/backups/db -name '*.sql.gz.gpg' -mtime +14 -delete

            # 4. R2 retention: 14 days
            aws s3 ls "s3://${R2_BUCKET}/db/" --endpoint-url "$R2_ENDPOINT" \
              | awk '$1 < "'$(date -d '14 days ago' +%F)'" { print $4 }' \
              | xargs -I{} aws s3 rm "s3://${R2_BUCKET}/db/{}" --endpoint-url "$R2_ENDPOINT"

            # 5. Report
            SIZE=$(du -h "$FILE" | cut -f1)
            echo "✅ Backup complete: ${STAMP} (${SIZE})"
            aws s3 ls "s3://${R2_BUCKET}/db/" --endpoint-url "$R2_ENDPOINT" --human-readable
```

### Workflow 2 — Restore from R2 (manual only)
**File:** `.github/workflows/restore-database-from-r2.yml`
**Triggers:** `workflow_dispatch` only — never auto

```yaml
name: Restore — Database from R2 (manual)
on:
  workflow_dispatch:
    inputs:
      backup_date:
        description: "Backup date YYYY-MM-DD (e.g. 2026-05-07)"
        required: true
      confirm:
        description: "Type RESTORE to confirm — this WILL overwrite production data"
        required: true
        default: ""
```

Safety: the `confirm` input must equal exactly `RESTORE` or the workflow exits without doing anything. Restore steps:
1. Download `${backup_date}.sql.gz.gpg` from R2 to VPS
2. `gpg --decrypt | gunzip | psql -U postgres consulting`
3. Run `python manage.py migrate` to be sure schema is current
4. Restart web/worker containers

### Workflow 3 — Smoke-test restore (weekly)
**File:** `.github/workflows/test-restore-from-r2.yml`
**Schedule:** `cron: "0 5 * * 0"` (Sunday 5 AM UTC)

Critical: a backup that has never been restored is not a backup. This workflow:
1. Downloads latest backup from R2
2. Decrypts with passphrase
3. Pipes through `pg_restore --list` (no actual write — just verifies the dump is parseable and the passphrase works)
4. Reports backup file size and row counts from the dump table-of-contents
5. Fails loudly if any step errors → you get a GitHub email

This catches:
- GPG passphrase rotated but not updated in secrets
- R2 credentials revoked
- pg_dump producing corrupt output
- Disk-full on Hetzner truncating the local dump

---

## Restore Runbook

When you need to actually recover:

```bash
# Option A — via GitHub Actions UI (easiest)
1. Go to Actions → "Restore — Database from R2"
2. Click "Run workflow"
3. Enter date (e.g. 2026-05-07)
4. Type RESTORE in confirm box
5. Click green button
6. Wait ~5-10 min
7. Verify by checking /admin/ or RawJob count

# Option B — manual from your laptop
ssh user@hetzner
cd /opt/consulting
aws s3 cp s3://chenn-db-backups/db/2026-05-07.sql.gz.gpg /tmp/restore.sql.gz.gpg \
  --endpoint-url https://<account-id>.r2.cloudflarestorage.com
gpg --decrypt --batch --passphrase "$BACKUP_GPG_PASSPHRASE" /tmp/restore.sql.gz.gpg \
  | gunzip \
  | docker compose -f docker-compose.prod.yml --env-file .env.production \
      exec -T db psql -U postgres consulting
docker compose restart web celery_worker celery_beat celery_harvest
```

---

## Verification Checklist (after first run)

- [ ] First scheduled backup ran successfully (check Actions tab)
- [ ] File appears in R2 bucket (check Cloudflare dashboard)
- [ ] File size is reasonable (100-500 MB for current DB)
- [ ] Local copy at `/opt/backups/db/YYYY-MM-DD.sql.gz.gpg` exists on VPS
- [ ] Smoke-test restore workflow runs Sunday and succeeds
- [ ] Manual restore tested ONCE end-to-end (do this in a Friday afternoon — never under fire)
- [ ] GPG passphrase stored in 2 separate places (password manager + offline)
- [ ] R2 credentials rotation reminder set for next year

---

## When To Resume This Work

**Prerequisite:** Mapbox/country resolution is working — i.e. you've completed:
- New Mapbox token rotated and saved via GUI
- Provider toggled ON in `/harvest/engine/`
- 60k unknown-country jobs processed through Mapbox
- Scope distribution looks healthy

**After that, this is a 30-minute job:**
- 10 min — you do the Cloudflare R2 setup (Steps 1-5 above)
- 15 min — I build all 3 workflows from this plan
- 5 min — first manual run, verify file lands in R2

---

## Future Considerations (out of scope for v1)

- **Streaming replication** — for zero-RPO DR (continuous replication to a hot standby). Costs money and operational complexity. Only worth it after revenue justifies it.
- **Point-in-time recovery (PITR)** — Postgres WAL shipping to R2. Useful for "restore to 14:32:07 right before someone ran DROP TABLE." Adds complexity. Add when DB grows past ~20GB.
- **Cross-region backup copy** — copy to a second R2 region or to Backblaze B2 for double-vendor resilience. Worth it once revenue is at risk from data loss.
- **Incremental dumps with `pg_basebackup` + WAL archive** — faster backups for larger DBs. Move to this when full dumps exceed 5GB or 30 minutes.

---

**Saved:** 2026-05-07
**Owner:** chennu
**Re-trigger:** "Build the R2 backup workflow now" once Mapbox work is done and Cloudflare credentials are ready.
