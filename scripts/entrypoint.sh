#!/usr/bin/env sh
set -eu

# Run DB migrations and collectstatic on boot.
# Safe to run repeatedly in containerized deployments.

python manage.py migrate --noinput
python manage.py collectstatic --noinput

WEB_CONCURRENCY="${WEB_CONCURRENCY:-2}"
WEB_TIMEOUT="${WEB_TIMEOUT:-120}"

exec gunicorn config.wsgi:application \
  --bind 0.0.0.0:8000 \
  --workers "$WEB_CONCURRENCY" \
  --timeout "$WEB_TIMEOUT" \
  --access-logfile - \
  --error-logfile -

