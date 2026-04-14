from celery import shared_task
from datetime import timedelta
from io import BytesIO

from django.db.models import Q, Count
from django.utils import timezone
from django.core.mail import EmailMessage, send_mail
from django.conf import settings
from django.urls import reverse

from core.email_ingest import fetch_unseen_and_process
from core.models import PlatformConfig
from submissions.models import ApplicationSubmission
from users.models import User, UserEmailNotificationPreferences


@shared_task
def poll_email_ingest_task():
    # Background poll. All enable/disable logic is inside the ingest function via PlatformConfig.
    return fetch_unseen_and_process(dry_run=False, max_messages=50)


@shared_task
def send_weekly_executive_report_task():
    """
    Weekly executive report summarising placements, pipeline, and quality metrics.
    Sends a simple PDF summary to all active admin users.
    """
    config = PlatformConfig.load()

    # Collect recipients: all active admins / superusers with email.
    recipient_qs = User.objects.filter(
        is_active=True,
    ).filter(
        (Q(is_superuser=True) | Q(role=User.Role.ADMIN))
    )
    recipients = [u.email for u in recipient_qs if u.email]
    if not recipients:
        return {"sent": False, "reason": "no_recipients"}

    AS = ApplicationSubmission
    now = timezone.now()
    start_week = now - timedelta(days=7)
    prev_week_start = start_week - timedelta(days=7)

    placements_this_week = AS.objects.filter(
        status=AS.Status.OFFER,
        updated_at__gte=start_week,
    ).count()
    placements_last_week = AS.objects.filter(
        status=AS.Status.OFFER,
        updated_at__gte=prev_week_start,
        updated_at__lt=start_week,
    ).count()

    interviews_scheduled = AS.objects.filter(
        status__in=[AS.Status.INTERVIEW, AS.Status.OFFER],
        updated_at__gte=start_week,
    ).count()
    offers_pending = AS.objects.filter(status=AS.Status.OFFER).count()

    # Simple "bench": consultants with no active submissions
    active_statuses = [AS.Status.IN_PROGRESS, AS.Status.APPLIED, AS.Status.INTERVIEW, AS.Status.OFFER]
    total_consultants = User.objects.filter(role=User.Role.CONSULTANT, is_active=True).count()
    bench_consultants = User.objects.filter(
        role=User.Role.CONSULTANT, is_active=True, consultant_profile__submissions__isnull=True
    ).count()

    # Submission quality per employee this week
    quality_rows = []
    employee_ids = (
        AS.objects.filter(
            submitted_by__role=User.Role.EMPLOYEE,
            created_at__gte=start_week,
        )
        .values_list("submitted_by", flat=True)
        .distinct()
    )
    for emp_id in employee_ids:
        emp = User.objects.filter(pk=emp_id).first()
        if not emp:
            continue
        emp_subs = AS.objects.filter(submitted_by=emp, created_at__gte=start_week)
        total = emp_subs.count()
        interviews = emp_subs.filter(status__in=[AS.Status.INTERVIEW, AS.Status.OFFER]).count()
        quality = round((interviews / total) * 100) if total else 0
        quality_rows.append((emp.get_full_name() or emp.username, total, interviews, quality))
    quality_rows.sort(key=lambda r: r[3], reverse=True)
    top_quality = quality_rows[0] if quality_rows else None

    # Build a very simple PDF using reportlab
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas
    except Exception:
        return {"sent": False, "reason": "reportlab_missing"}

    buffer = BytesIO()
    p = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 50

    def line(text, dy=18, bold=False):
        nonlocal y
        if y < 50:
            p.showPage()
            y = height - 50
        if bold:
            p.setFont("Helvetica-Bold", 11)
        else:
            p.setFont("Helvetica", 10)
        p.drawString(40, y, text)
        y -= dy

    line("GoCareers Weekly Executive Report", bold=True)
    line(now.strftime("Generated on %Y-%m-%d %H:%M %Z"))
    y -= 10

    line("1. Placements & Revenue", bold=True)
    line(f"Placements this week: {placements_this_week}")
    line(f"Placements last week: {placements_last_week}")
    line("Revenue this month vs target: (not yet configured in this build)")
    y -= 6

    line("2. Pipeline", bold=True)
    line(f"Interviews scheduled (this week): {interviews_scheduled}")
    line(f"Offers pending (current): {offers_pending}")
    y -= 6

    line("3. Bench", bold=True)
    line(f"Total consultants: {total_consultants}")
    line(f"Bench count (very rough): {bench_consultants}")
    line("Bench cost: (depends on your internal rate card)")
    y -= 6

    line("4. Submission Quality (this week)", bold=True)
    if top_quality:
        name, total_sub, intr, q = top_quality
        line(f"Top quality employee: {name}")
        line(f"Submissions: {total_sub}, Interviews: {intr}, Quality score: {q}%")
    else:
        line("No employee submissions recorded this week.")
    y -= 6

    if quality_rows:
        line("Employee quality breakdown:", bold=True)
        for name, total_sub, intr, q in quality_rows[:10]:
            line(f"- {name}: {total_sub} submissions, {intr} interviews → {q}% quality")

    p.showPage()
    p.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()

    subject = "GoCareers Weekly Executive Report"
    body = (
        "Attached is the weekly executive report summarising placements, pipeline, bench, "
        "and submission quality. This file was generated automatically by GoCareers."
    )
    email = EmailMessage(subject, body, to=recipients)
    filename = now.strftime("chenn-weekly-report-%Y%m%d.pdf")
    email.attach(filename, pdf_bytes, "application/pdf")
    email.send(fail_silently=True)

    return {
        "sent": True,
        "recipients": recipients,
        "placements_this_week": placements_this_week,
        "placements_last_week": placements_last_week,
        "interviews_scheduled": interviews_scheduled,
        "offers_pending": offers_pending,
    }


@shared_task
def send_weekly_consultant_pipeline_digest_task():
    """
    Weekly email to consultants: counts by pipeline stage (last 7 days + active).
    Respects UserEmailNotificationPreferences.email_submissions.
    """
    AS = ApplicationSubmission
    now = timezone.now()
    week_ago = now - timedelta(days=7)
    base = getattr(settings, "SITE_URL", "").rstrip("/")

    consultants = User.objects.filter(
        role=User.Role.CONSULTANT,
        is_active=True,
        consultant_profile__isnull=False,
    ).select_related("consultant_profile")

    sent = 0
    for user in consultants:
        prefs, _ = UserEmailNotificationPreferences.objects.get_or_create(user=user)
        if not prefs.email_submissions or not user.email:
            continue
        cp = user.consultant_profile
        active = AS.objects.filter(consultant=cp, is_archived=False)
        recent = active.filter(updated_at__gte=week_ago)
        lines = [
            f"Pipeline snapshot for {user.get_full_name() or user.username}",
            "",
            f"Active submissions (total): {active.count()}",
            f"Updated in the last 7 days: {recent.count()}",
        ]
        by_status = active.values("status").order_by("status").annotate(n=Count("id"))

        for row in by_status:
            label = dict(AS.Status.choices).get(row["status"], row["status"])
            lines.append(f"  {label}: {row['n']}")

        path = reverse("submission-list")
        lines.extend(["", f"Open submissions: {base}{path}"])

        try:
            send_mail(
                subject="Your weekly pipeline summary",
                message="\n".join(lines),
                from_email=getattr(settings, "DEFAULT_FROM_EMAIL", None) or "noreply@localhost",
                recipient_list=[user.email],
                fail_silently=True,
            )
            sent += 1
        except Exception:
            continue

    return {"sent_count": sent}


