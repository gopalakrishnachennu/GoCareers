"""Fast checks for career URLs, tenant extraction, harvester wiring, and smoke command."""

from io import StringIO

from django.core.management import call_command
from django.test import SimpleTestCase, TestCase

from apps.harvest.career_url import build_career_url
from apps.harvest.detectors import extract_tenant
from apps.harvest.harvesters import (
    TeamtailorHarvester,
    ZohoHarvester,
    get_harvester,
)
from apps.harvest.platform_engine import ImplementationKind, dedicated_slugs, kind_for_slug


class HarvestUrlAndRegistryTests(SimpleTestCase):
    def test_build_career_url_zoho(self):
        self.assertEqual(
            build_career_url("zoho", "acme"),
            "https://jobs.zoho.com/portal/acme/careers",
        )
        self.assertEqual(
            build_career_url("zoho", "acme.zohorecruit.com"),
            "https://acme.zohorecruit.com/jobs/Careers",
        )

    def test_extract_tenant_subdomain_hosts(self):
        self.assertEqual(
            extract_tenant("teamtailor", "https://widgets.teamtailor.com/jobs"),
            "widgets",
        )
        self.assertEqual(
            extract_tenant("breezy", "https://foo.breezy.hr/p/1"),
            "foo",
        )
        self.assertEqual(
            extract_tenant("zoho", "https://jobs.zoho.com/portal/acme/careers"),
            "acme",
        )
        self.assertEqual(
            extract_tenant("zoho", "https://acme.zohorecruit.com/jobs/Careers"),
            "acme",
        )

    def test_get_harvester_and_platform_kind(self):
        self.assertIsInstance(get_harvester("zoho"), ZohoHarvester)
        self.assertIsInstance(get_harvester("teamtailor"), TeamtailorHarvester)
        self.assertEqual(kind_for_slug("zoho"), ImplementationKind.DEDICATED)
        self.assertEqual(kind_for_slug("teamtailor"), ImplementationKind.DEDICATED)
        self.assertIn("zoho", dedicated_slugs())
        self.assertIn("teamtailor", dedicated_slugs())


class SmokeTestHarvestCommandTests(TestCase):
    """Dry-run must not require network or Celery."""

    def test_smoke_test_harvest_dry_run_exits_zero(self):
        out = StringIO()
        err = StringIO()
        try:
            call_command("smoke_test_harvest", "--dry-run", stdout=out, stderr=err)
        except SystemExit as e:
            self.fail(f"smoke_test_harvest --dry-run raised SystemExit({e.code})")
        self.assertIn("Dry run finished", out.getvalue())
