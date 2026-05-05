#!/usr/bin/env sh
set -eu

# Migrations: all services (worker/beat need DB too).
python manage.py migrate --noinput

# Static files: web only (default path = no override from compose).
if [ "$#" -eq 0 ]; then
  python manage.py collectstatic --noinput
fi

WEB_CONCURRENCY="${WEB_CONCURRENCY:-2}"
WEB_TIMEOUT="${WEB_TIMEOUT:-120}"

# docker-compose passes `command:` as args (e.g. celery worker / beat).
# Without args we run Gunicorn (web service).
if [ "$#" -gt 0 ]; then
  exec "$@"
fi

exec gunicorn config.wsgi:application \
  --bind 0.0.0.0:8000 \
  --workers "$WEB_CONCURRENCY" \
  --timeout "$WEB_TIMEOUT" \
  --access-logfile - \
  --error-logfile -
