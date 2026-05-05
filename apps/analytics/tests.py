from django.test import TestCase, Client
from django.urls import reverse
from users.models import User, EmployeeProfile
from core.models import EmployeeDesignation


class AnalyticsDateRangeTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.employee = User.objects.create_user(
            username='emp1', password='testpass', role=User.Role.EMPLOYEE
        )
        ep = EmployeeProfile.objects.create(user=self.employee, company_name='Test Co')
        ep.designation = EmployeeDesignation.objects.get(slug='senior_recruiter')
        ep.save(update_fields=['designation'])

    def test_analytics_dashboard_all_time(self):
        self.client.login(username='emp1', password='testpass')
        url = reverse('analytics-dashboard')
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        self.assertIn('date_range_label', resp.context)
        self.assertEqual(resp.context['date_range_label'], 'All time')
        self.assertIn('board_health_rows', resp.context)
        self.assertIn('harvest_summary', resp.context)
        self.assertContains(resp, 'Analytics Control Tower')
        self.assertContains(resp, 'Job Boards')
        self.assertContains(resp, 'JD & Field Coverage')
        self.assertContains(resp, 'Failures & Blockers')

    def test_analytics_dashboard_last_7_days(self):
        self.client.login(username='emp1', password='testpass')
        url = reverse('analytics-dashboard') + '?range=7'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context.get('date_range_label'), 'Last 7 days')
        self.assertEqual(resp.context.get('date_range'), '7')
        self.assertIn('board_risk_chart_json', resp.context)

    def test_analytics_dashboard_last_30_days(self):
        self.client.login(username='emp1', password='testpass')
        url = reverse('analytics-dashboard') + '?range=30'
        resp = self.client.get(url)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.context.get('date_range_label'), 'Last 30 days')
        self.assertEqual(resp.context.get('date_range'), '30')
        self.assertIn('ops_health_rows', resp.context)


class PlatformHealthAggregationTests(TestCase):
    def setUp(self):
        from companies.models import Company
        from harvest.models import CompanyFetchRun, CompanyPlatformLabel, JobBoardPlatform, RawJob

        self.platform = JobBoardPlatform.objects.create(
            slug="testboard",
            name="Test Board",
            is_enabled=True,
        )
        self.company = Company.objects.create(name="Agg Test Co")
        self.label = CompanyPlatformLabel.objects.create(
            company=self.company,
            platform=self.platform,
        )

    def _get_row(self, rows, slug):
        """Helper: find the aggregation row for a given slug."""
        for row in rows:
            if row.get("slug") == slug:
                return row
        return None

    def test_empty_runs_counted_in_zero_yield(self):
        """EMPTY-status runs must contribute to zero_yield_success_runs (not be silently dropped)."""
        from harvest.models import CompanyFetchRun
        from analytics.views import _platform_health_rows

        CompanyFetchRun.objects.create(
            label=self.label,
            status=CompanyFetchRun.Status.EMPTY,
        )
        rows = _platform_health_rows(since=None)
        row = self._get_row(rows, "testboard")
        self.assertIsNotNone(row, "Expected a row for 'testboard'")
        self.assertGreaterEqual(
            row.get("zero_yield_success_runs", 0),
            1,
            "EMPTY run must be counted in zero_yield_success_runs",
        )

    def test_blocked_q_uses_sync_skip_reason(self):
        """A PENDING job with a non-empty sync_skip_reason must be counted as blocked."""
        from harvest.models import RawJob
        from analytics.views import _platform_health_rows

        RawJob.objects.create(
            company=self.company,
            platform_slug="testboard",
            title="Blocked Job",
            sync_status=RawJob.SyncStatus.PENDING,
            sync_skip_reason="JD_TOO_WEAK",
            is_active=True,
            has_description=True,
        )
        rows = _platform_health_rows(since=None)
        row = self._get_row(rows, "testboard")
        self.assertIsNotNone(row, "Expected a row for 'testboard'")
        self.assertGreaterEqual(
            row.get("blocked_jobs", 0),
            1,
            "Job with sync_skip_reason='JD_TOO_WEAK' must appear in blocked_jobs",
        )

    def test_blocked_q_dead_failed_clause_removed(self):
        """A PENDING job with no blocker conditions must NOT be counted as blocked."""
        from harvest.models import RawJob
        from analytics.views import _platform_health_rows

        # NOTE: RawJob.save() overrides has_description from has_meaningful_description(),
        # so we must set a real description — otherwise has_description becomes False
        # and the job incorrectly triggers the Q(has_description=False) clause.
        RawJob.objects.create(
            company=self.company,
            platform_slug="testboard",
            title="Clean Job",
            description="This is a fully described and well-qualified job posting for testing.",
            sync_status=RawJob.SyncStatus.PENDING,
            sync_skip_reason="",
            is_active=True,
            resume_ready_score=0.9,
            category_confidence=0.9,
            classification_confidence=0.9,
        )
        rows = _platform_health_rows(since=None)
        row = self._get_row(rows, "testboard")
        self.assertIsNotNone(row, "Expected a row for 'testboard'")
        self.assertEqual(
            row.get("blocked_jobs", 0),
            0,
            "PENDING job with no blocker conditions must not appear in blocked_jobs",
        )

    def test_jarvis_not_counted_separately(self):
        """Aggregation must run without error (smoke test for Jarvis/special-slug handling)."""
        from analytics.views import _platform_health_rows

        rows = _platform_health_rows(since=None)
        self.assertIsInstance(rows, list)
