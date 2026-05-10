from django.test import TestCase

from companies.models import Company
from harvest.models import RawJob
from harvest.services.rawjob_query import apply_rawjob_filters, apply_stage_filter


class RawJobQueryFilterTests(TestCase):
    def setUp(self):
        self.company = Company.objects.create(name="Acme")

    def _raw_job(self, title, url_hash, **kwargs):
        return RawJob.objects.create(
            company=self.company,
            title=title,
            url_hash=url_hash,
            company_name=self.company.name,
            **kwargs,
        )

    def test_country_filter_matches_country_codes_array(self):
        primary_ca = self._raw_job(
            "Multi-country Engineer",
            "country-array",
            country_code="CA",
            country_codes=["CA", "US"],
        )
        self._raw_job("Canada Only", "country-primary", country_code="CA", country_codes=["CA"])

        qs = apply_rawjob_filters(RawJob.objects.all(), {"country_code": "US"})

        self.assertEqual(list(qs), [primary_ca])

    def test_marketing_role_filter_matches_candidate_domains(self):
        candidate = self._raw_job(
            "Platform Engineer",
            "role-candidate",
            job_domain="software-developer",
            job_domain_candidates=["software-developer", "devops-cloud"],
        )
        self._raw_job("Backend Engineer", "role-primary", job_domain="software-developer")

        qs = apply_rawjob_filters(RawJob.objects.all(), {"marketing_role": "devops-cloud"})

        self.assertEqual(list(qs), [candidate])

    def test_duplicate_stage_uses_duplicate_skip_reason_not_all_skipped(self):
        duplicate = self._raw_job(
            "Duplicate Engineer",
            "duplicate",
            sync_status=RawJob.SyncStatus.SKIPPED,
            sync_skip_reason="DUPLICATE_EXISTING",
        )
        self._raw_job(
            "Weak JD Engineer",
            "weak-jd",
            sync_status=RawJob.SyncStatus.SKIPPED,
            sync_skip_reason="JD_TOO_WEAK",
        )

        qs = apply_stage_filter(RawJob.objects.all(), "DUPLICATE")

        self.assertEqual(list(qs), [duplicate])
