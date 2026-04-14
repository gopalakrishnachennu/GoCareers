import os

from celery import Celery
from celery.schedules import crontab

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

app = Celery("config")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

# Periodic tasks (enable Celery Beat in production).
app.conf.beat_schedule = {
    "weekly-consultant-pipeline-digest": {
        "task": "core.tasks.send_weekly_consultant_pipeline_digest_task",
        "schedule": crontab(hour=8, minute=0, day_of_week=1),  # Monday 08:00 UTC
    },
}

