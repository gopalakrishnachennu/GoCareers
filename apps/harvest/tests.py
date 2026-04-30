"""Fast checks for career URLs, tenant extraction, harvester wiring, and smoke command."""

import json
from io import StringIO
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock, patch

import requests
from django.core.management import call_command
from django.test import SimpleTestCase, TestCase
from django.urls import reverse

from apps.harvest.career_url import build_career_url
from apps.harvest.detectors import extract_tenant
from apps.harvest.harvesters import (
    TeamtailorHarvester,
    ZohoHarvester,
    get_harvester,
)
from apps.harvest.jarvis import JobJarvis
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


class HarvestUrlHashDedupeTests(SimpleTestCase):
    def test_tracking_query_params_do_not_change_hash(self):
        from apps.harvest.normalizer import compute_url_hash

        a = "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE/jobs/6503?src=LinkedIn&utm_source=linkedin"
        b = "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE/jobs/6503"
        self.assertEqual(compute_url_hash(a), compute_url_hash(b))

    def test_identity_query_params_still_change_hash(self):
        from apps.harvest.normalizer import compute_url_hash

        a = "https://example.com/jobs/view?jobId=123"
        b = "https://example.com/jobs/view?jobId=456"
        self.assertNotEqual(compute_url_hash(a), compute_url_hash(b))


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


class JarvisPlatformApiExtractionTests(SimpleTestCase):
    """Verify Jarvis _platform_api paths populate description (backfill relies on this)."""

    def test_workday_detail_api_maps_job_description(self):
        jarvis = JobJarvis()
        wd_url = (
            "https://acme.wd1.myworkdayjobs.com/en-US/Search/job/"
            "Remote-Engineer_R_99999"
        )
        detail_resp = {
            "jobPostingInfo": {
                "title": "Remote Engineer",
                "location": "Remote",
                "externalJobId": "R_99999",
                "jobDescription": "<p>Workday JD body</p>",
            },
        }
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = detail_resp
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._workday(wd_url)
        self.assertIsNotNone(out)
        self.assertIn("Workday JD body", out.get("description", ""))
        self.assertEqual(out.get("title"), "Remote Engineer")

    def test_workday_search_fallback_when_detail_404s(self):
        jarvis = JobJarvis()
        wd_url = (
            "https://3m.wd1.myworkdayjobs.com/Search/job/"
            "US-MN/Engineer_R01049764"
        )
        # Detail returns 404
        detail_404 = MagicMock()
        detail_404.ok = False
        detail_404.status_code = 404

        # Search returns a result with description in search data
        search_resp = MagicMock()
        search_resp.ok = True
        search_resp.json.return_value = {
            "jobPostings": [{
                "title": "Manufacturing Engineer",
                "externalPath": "/job/US-MN/Engineer_R01049764",
                "locationsText": "Maplewood, MN",
                "bulletFields": ["R01049764"],
                "jobDescription": {"content": "<p>Workday search JD</p>"},
            }]
        }

        # Detail for correct path returns full JD
        detail_ok = MagicMock()
        detail_ok.ok = True
        detail_ok.json.return_value = {
            "jobPostingInfo": {
                "title": "Manufacturing Engineer",
                "location": "Maplewood, MN",
                "jobDescription": "<p>Full Workday JD from detail</p>",
            }
        }

        call_count = {"n": 0}
        def mock_get(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return detail_404  # first detail call fails
            return detail_ok  # second detail call succeeds

        with patch.object(jarvis._session, "get", side_effect=mock_get):
            with patch.object(jarvis._session, "post", return_value=search_resp):
                out = jarvis._workday(wd_url)
        self.assertIsNotNone(out)
        self.assertIn("Full Workday JD", out.get("description", ""))

    def test_smartrecruiters_normalize_posting_id_strips_seo_slug(self):
        from apps.harvest.jarvis import _smartrecruiters_normalize_posting_id

        self.assertEqual(
            _smartrecruiters_normalize_posting_id(
                "744000121421842-mgr-strategic-rebids-930951-",
            ),
            "744000121421842",
        )
        self.assertEqual(
            _smartrecruiters_normalize_posting_id("111222333"),
            "111222333",
        )

    def test_smartrecruiters_accepts_rest_api_url(self):
        """Apply links sometimes store api.smartrecruiters.com/v1/companies/.../postings/id."""
        jarvis = JobJarvis()
        url = "https://api.smartrecruiters.com/v1/companies/WesternDigital/postings/744000112340137"
        captured = {}

        def fake_get(u, **kwargs):
            captured["u"] = u
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {
                "id": "744000112340137",
                "name": "Engineer",
                "ref": "https://jobs.smartrecruiters.com/WesternDigital/744000112340137",
                "jobAd": {
                    "sections": {
                        "jobDescription": {"text": "<p>API body</p>"},
                    }
                },
            }
            return resp

        with patch.object(jarvis, "_http_get", side_effect=fake_get):
            out = jarvis._smartrecruiters(url)
        self.assertIsNotNone(out)
        self.assertIn("WesternDigital", captured.get("u", ""))
        self.assertIn("/postings/744000112340137", captured.get("u", ""))
        self.assertIn("API body", out.get("description", ""))

    def test_smartrecruiters_api_request_strips_seo_slug_from_url(self):
        """Detail API must receive numeric id only, not ``744...-title-slug``."""
        from apps.harvest.jarvis import _smartrecruiters_normalize_posting_id

        jarvis = JobJarvis()
        url = "https://jobs.smartrecruiters.com/DemoCo/744000121421842-mgr-title-"
        captured = {}

        def fake_get(u, **kwargs):
            captured["detail_url"] = u
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {
                "id": "744000121421842",
                "name": "Role",
                "ref": url,
                "jobAd": {
                    "sections": {
                        "jobDescription": {"text": "<p>Body</p>"},
                    }
                },
            }
            return resp

        with patch.object(jarvis, "_http_get", side_effect=fake_get):
            out = jarvis._smartrecruiters(url)
        self.assertIsNotNone(out)
        self.assertIn("/postings/744000121421842", captured.get("detail_url", ""))
        self.assertNotIn("mgr-title", captured.get("detail_url", ""))
        self.assertIn("Body", out.get("description", ""))
        self.assertEqual(
            _smartrecruiters_normalize_posting_id("744000121421842-mgr-title-"),
            "744000121421842",
        )

    def test_smartrecruiters_detail_maps_sections(self):
        jarvis = JobJarvis()
        url = "https://jobs.smartrecruiters.com/DemoCo/111222333"
        detail = {
            "name": "QA Role",
            "ref": url,
            "location": {"city": "Austin", "region": "TX", "country": "US"},
            "jobAd": {
                "sections": {
                    "jobDescription": {"text": "<p>SR JD</p>"},
                    "qualifications": {"text": "<p>Reqs</p>"},
                }
            },
        }
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = detail
        with patch.object(jarvis, "_http_get", return_value=mock_resp):
            out = jarvis._smartrecruiters(url)
        self.assertIsNotNone(out)
        self.assertIn("SR JD", out.get("description", ""))
        self.assertIn("Reqs", out.get("requirements", ""))

    def test_recruitee_offers_list_matches_slug(self):
        jarvis = JobJarvis()
        url = "https://widgets.recruitee.com/o/backend-engineer"
        offers = {
            "offers": [
                {
                    "id": 42,
                    "slug": "backend-engineer",
                    "title": "Backend Engineer",
                    "description": "<p>Recruitee JD</p>",
                    "requirements": "",
                    "city": "Berlin",
                    "country": "DE",
                    "careers_url": url,
                }
            ]
        }
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = offers
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._recruitee(url)
        self.assertIsNotNone(out)
        self.assertIn("Recruitee JD", out.get("description", ""))

    def test_bamboohr_detail_json_maps_description(self):
        jarvis = JobJarvis()
        url = "https://acme.bamboohr.com/careers/12345"
        payload = {
            "result": {
                "jobOpening": {
                    "description": "<p>Bamboo JD</p>",
                    "jobTitle": "Analyst",
                    "location": {"city": "NYC", "state": "NY", "addressCountry": "US"},
                }
            }
        }
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = payload
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._bamboohr(url)
        self.assertIsNotNone(out)
        self.assertIn("Bamboo JD", out.get("description", ""))
        self.assertEqual(out.get("title"), "Analyst")

    def test_icims_detail_page_scrape(self):
        jarvis = JobJarvis()
        url = "https://careers-acme.icims.com/jobs/12345/job"
        html = (
            '<html><body>'
            '<h1 class="iCIMS_JobTitle">Software Engineer</h1>'
            '<div class="iCIMS_JobContent"><p>iCIMS JD body that is long enough to pass minimum threshold of 72 chars easily here.</p></div>'
            '</body></html>'
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._icims(url)
        self.assertIsNotNone(out)
        self.assertIn("iCIMS JD body", out.get("description", ""))
        self.assertEqual(out.get("title"), "Software Engineer")

    def test_jobvite_detail_page_scrape(self):
        jarvis = JobJarvis()
        url = "https://jobs.jobvite.com/acmecorp/job/oABC123"
        html = (
            '<html><body>'
            '<h2 class="jv-header">QA Lead</h2>'
            '<div class="jv-job-detail-description"><p>Jobvite JD body here with plenty of text to pass the minimum character threshold easily.</p></div>'
            '</body></html>'
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._jobvite(url)
        self.assertIsNotNone(out)
        self.assertIn("Jobvite JD body", out.get("description", ""))
        self.assertEqual(out.get("title"), "QA Lead")

    def test_taleo_detail_page_scrape(self):
        jarvis = JobJarvis()
        url = "https://aa224.taleo.net/careersection/ex/jobdetail.ftl?job=12345&lang=en"
        html = (
            '<html><body>'
            '<h1 id="requisitionDescriptionInterface.reqTitleLinkAction.row1">PM Role</h1>'
            '<div id="requisitionDescriptionInterface.ID1702.row1"><p>Taleo JD body with enough content to pass the seventy two character minimum threshold.</p></div>'
            '</body></html>'
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._taleo(url)
        self.assertIsNotNone(out)
        self.assertIn("Taleo JD body", out.get("description", ""))

    def test_ultipro_embedded_json_in_html(self):
        jarvis = JobJarvis()
        url = (
            "https://recruiting.ultipro.com/INT1043EXCUR/JobBoard/"
            "ad5e5978-552f-4ef7-90c8-70ebb0a57994/OpportunityDetail"
            "?opportunityId=c19385b5-7296-4f1d-88d8-3cbf7507693f"
        )
        html = (
            "<html><script>\n"
            'var opportunity = new US.Opportunity.CandidateOpportunityDetail('
            '{"Title":"Finance Intern",'
            '"Description":"<p>UKG UltiPro full JD body with enough text for tests.</p>",'
            '"Locations":[{"LocalizedDescription":"Scottsdale HQ",'
            '"Address":{"City":"Scottsdale","State":{"Code":"AZ"},"Country":{"Code":"USA"}}}]}'
            ");\n</script></html>"
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        mock_resp.url = url
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._ultipro(url)
        self.assertIsNotNone(out)
        self.assertIn("UKG UltiPro full JD body", out.get("description", ""))
        self.assertEqual(out.get("title"), "Finance Intern")
        self.assertIn("Scottsdale", out.get("location_raw", ""))

    def test_oracle_ce_rest_api(self):
        jarvis = JobJarvis()
        url = "https://eeho.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX/job/300001"
        api_resp = {
            "items": [{
                "Title": "Oracle Dev",
                "ExternalDescriptionStr": "<p>Oracle JD body</p>",
                "PrimaryLocation": "Redwood City, CA",
                "Organization": "Engineering",
            }]
        }
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = api_resp
        with patch.object(jarvis._session, "get", return_value=mock_resp) as mock_get:
            out = jarvis._oracle(url)
        self.assertIsNotNone(out)
        self.assertIn("Oracle JD body", out.get("description", ""))
        self.assertEqual(out.get("title"), "Oracle Dev")
        finder = mock_get.call_args.kwargs.get("params", {}).get("finder", "")
        self.assertIn("ById;", finder)
        self.assertNotIn("findReqDetails", finder)

    def test_dayforce_job_detail_api(self):
        jarvis = JobJarvis()
        url = "https://jobs.dayforcehcm.com/en-US/corpay/CANDIDATEPORTAL/jobs/12345"
        api_resp = {
            "JobTitle": "Payroll Analyst",
            "Description": "<p>Dayforce JD body</p>",
            "JobLocation": "Tampa, FL",
        }
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = api_resp
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._dayforce(url)
        self.assertIsNotNone(out)
        self.assertIn("Dayforce JD body", out.get("description", ""))
        self.assertEqual(out.get("title"), "Payroll Analyst")

    def test_dayforce_next_data_fallback_for_modern_board_urls(self):
        jarvis = JobJarvis()
        url = "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE/jobs/6503?src=LinkedIn"
        next_data = {
            "props": {
                "pageProps": {
                    "jobData": {
                        "jobTitle": "Platform Engineer",
                        "jobReqId": "6503",
                        "postingLocations": [
                            {"formattedAddress": "Austin, Texas, United States of America"},
                            {"formattedAddress": "Tempe, Arizona, United States of America"},
                        ],
                        "jobPostingAttributes": [{"name": "JobFamily", "value": "Technology"}],
                        "postingStartTimestampUTC": "2026-04-02T03:00:00Z",
                        "jobPostingContent": {
                            "jobDescription": "<p>Lead and build secure cloud platforms.</p>",
                            "jobDescriptionFooter": "<p>Benefits package and growth opportunities.</p>",
                        },
                    }
                }
            },
            "query": {"clientNamespace": "kestra"},
        }
        html = (
            '<html><head></head><body>'
            '<script id="__NEXT_DATA__" type="application/json">'
            f"{json.dumps(next_data)}"
            "</script></body></html>"
        )

        detail_404 = requests.HTTPError("404")
        html_resp = MagicMock()
        html_resp.raise_for_status = MagicMock()
        html_resp.text = html
        html_resp.url = url

        def fake_get(target_url, *args, **kwargs):
            if "/api/geo/" in target_url:
                raise detail_404
            return html_resp

        with patch.object(jarvis, "_http_get", side_effect=fake_get):
            out = jarvis._dayforce(url)

        self.assertIsNotNone(out)
        self.assertEqual(out.get("title"), "Platform Engineer")
        self.assertEqual(out.get("company_name"), "Kestra")
        self.assertEqual(out.get("department"), "Technology")
        self.assertEqual(out.get("external_id"), "6503")
        self.assertIn("Austin, Texas", out.get("location_raw", ""))
        self.assertIn("Lead and build secure cloud platforms", out.get("description", ""))
        self.assertIn("Benefits package", out.get("description", ""))

    def test_breezy_detail_page_scrape(self):
        jarvis = JobJarvis()
        url = "https://acme.breezy.hr/p/abc123-software-dev"
        html = (
            '<html><body>'
            '<h1>Software Dev</h1>'
            '<div class="description"><p>Breezy JD body with enough words to clear the seventy two character minimum check easily.</p></div>'
            '</body></html>'
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._breezy(url)
        self.assertIsNotNone(out)
        self.assertIn("Breezy JD body", out.get("description", ""))

    def test_teamtailor_detail_page_scrape(self):
        jarvis = JobJarvis()
        url = "https://career.teamtailor.com/jobs/12345-qa-engineer"
        html = (
            '<html><body>'
            '<h1>QA Engineer</h1>'
            '<div class="job-description"><p>Teamtailor JD body with plenty of characters to easily clear the minimum threshold.</p></div>'
            '</body></html>'
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._teamtailor(url)
        self.assertIsNotNone(out)
        self.assertIn("Teamtailor JD body", out.get("description", ""))

    def test_zoho_detail_page_scrape(self):
        jarvis = JobJarvis()
        url = "https://jobs.zoho.com/portal/acme/apply/123"
        html = (
            '<html><body>'
            '<h1>Data Analyst</h1>'
            '<div class="job-description"><p>Zoho JD body with sufficient text content to pass the seventy-two character minimum check.</p></div>'
            '</body></html>'
        )
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = html
        with patch.object(jarvis._session, "get", return_value=mock_resp):
            out = jarvis._zoho(url)
        self.assertIsNotNone(out)
        self.assertIn("Zoho JD body", out.get("description", ""))


class JarvisFetchGateTests(SimpleTestCase):
    """JarvisFetchGate: retries and concurrency wrapper for outbound HTTP."""

    def test_retries_502_then_success(self):
        from unittest.mock import MagicMock, patch

        from apps.harvest.http_limits import JarvisFetchGate

        gate = JarvisFetchGate(50, 10, 3, 0.01)
        session = MagicMock()
        bad = MagicMock()
        bad.status_code = 502
        good = MagicMock()
        good.status_code = 200
        session.get.side_effect = [bad, good]
        with patch("apps.harvest.http_limits.time.sleep"):
            r = gate.request(session, "GET", "https://example.com/job/1")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(session.get.call_count, 2)

    def test_no_retry_on_404(self):
        from unittest.mock import MagicMock, patch

        from apps.harvest.http_limits import JarvisFetchGate

        gate = JarvisFetchGate(50, 10, 3, 0.01)
        session = MagicMock()
        nf = MagicMock()
        nf.status_code = 404
        session.get.return_value = nf
        with patch("apps.harvest.http_limits.time.sleep"):
            r = gate.request(session, "GET", "https://example.com/missing")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(session.get.call_count, 1)


class SmartRecruitersSupportTests(SimpleTestCase):
    """Canonical API URLs from list payload — avoids case-sensitive slug mismatches."""

    def test_backfill_fetch_url_uses_company_identifier_from_payload(self):
        from types import SimpleNamespace

        from harvest.smartrecruiters_support import backfill_fetch_url_for_raw_job

        job = SimpleNamespace(
            original_url="https://jobs.smartrecruiters.com/wrongslug/744000112340137",
            external_id="744000112340137",
            raw_payload={
                "company": {"identifier": "WesternDigital", "name": "WD"},
                "id": "744000112340137",
            },
        )
        u = backfill_fetch_url_for_raw_job(job)
        self.assertTrue(u.startswith("https://api.smartrecruiters.com/v1/companies/"))
        self.assertIn("WesternDigital", u)
        self.assertIn("/postings/744000112340137", u)


class BackfillJdEligibilityTests(TestCase):
    """Regression: skipped/failed backfill uses description=' ' — must stay eligible."""

    def test_space_placeholder_description_remains_in_backfill_queue(self):
        import hashlib

        from companies.models import Company

        from harvest.models import RawJob
        from harvest.tasks import _backfill_eligible_queryset

        c = Company.objects.create(name="BackfillEligTestCo")
        url = "https://example.com/job/backfill-elig-1"
        h = hashlib.sha256(url.encode()).hexdigest()
        j = RawJob.objects.create(
            company=c,
            title="Test",
            url_hash=h,
            original_url=url,
            description=" ",
        )
        self.assertTrue(_backfill_eligible_queryset(None).filter(pk=j.pk).exists())


class SyncRawJobsToPoolTests(TestCase):
    """Phase 5: sync_harvested_to_pool_task now reads RawJob directly."""

    def setUp(self):
        from companies.models import Company
        from harvest.models import JobBoardPlatform
        from users.models import User

        self.user = User.objects.create_user(
            username="sync_mirror_admin",
            email="sync_mirror@example.com",
            password="testpass123",
            is_superuser=True,
        )
        self.company = Company.objects.create(name="SyncMirrorCo")
        self.platform = JobBoardPlatform.objects.create(
            name="Sync Mirror Plat",
            slug="sync-mirror-plat",
        )

    def test_pool_sync_creates_job_from_raw_job(self):
        import hashlib
        from harvest.models import RawJob
        from harvest.tasks import sync_harvested_to_pool_task
        from jobs.models import Job

        url = "https://example.com/careers/sync-mirror-unique-99"
        h = hashlib.sha256(url.strip().encode()).hexdigest()
        raw = RawJob.objects.create(
            company=self.company,
            title="Engineer",
            url_hash=h,
            original_url=url,
            description=(
                "We are hiring a platform engineer to design, build, and maintain "
                "reliable cloud infrastructure, automation pipelines, observability "
                "dashboards, incident response processes, and secure deployment "
                "patterns across distributed systems. You will collaborate with "
                "engineering and operations teams, improve CI/CD workflows, enforce "
                "best practices, and support production services with clear runbooks "
                "and operational excellence. The role includes Linux operations, "
                "infrastructure as code, monitoring, alert tuning, service-level "
                "objectives, incident retrospectives, secure networking, and "
                "change management. Candidates should demonstrate scripting ability, "
                "cloud platform experience, strong communication, and ownership of "
                "production reliability initiatives across multiple environments."
            ),
            sync_status="PENDING",
        )
        sync_harvested_to_pool_task.apply(kwargs={"max_jobs": 10}).get()
        raw.refresh_from_db()
        self.assertEqual(raw.sync_status, "SYNCED")
        self.assertTrue(Job.objects.filter(url_hash=h).exists())

    def test_pool_sync_skipped_duplicate(self):
        import hashlib
        from harvest.models import RawJob
        from harvest.tasks import sync_harvested_to_pool_task
        from jobs.models import Job

        url = "https://example.com/careers/sync-mirror-dup-88"
        h = hashlib.sha256(url.strip().encode()).hexdigest()
        raw = RawJob.objects.create(
            company=self.company,
            title="Engineer",
            url_hash=h,
            original_url=url,
            sync_status="PENDING",
        )
        Job.objects.create(
            title="Already here",
            company=self.company.name,
            company_obj=self.company,
            description="Existing pool job",
            original_link=url,
            posted_by=self.user,
        )
        sync_harvested_to_pool_task.apply(kwargs={"max_jobs": 10}).get()
        raw.refresh_from_db()
        self.assertEqual(raw.sync_status, "SKIPPED")


class JarvisCompanyFallbackTests(SimpleTestCase):
    def test_extract_company_from_dayforce_url_uses_tenant(self):
        from harvest.tasks import _extract_company_from_url

        url = "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE/jobs/6503?src=LinkedIn"
        self.assertEqual(_extract_company_from_url(url), "Kestra")

    def test_jarvis_company_jobs_url_dayforce_board_root(self):
        from harvest.tasks import _jarvis_company_jobs_url

        self.assertEqual(
            _jarvis_company_jobs_url("dayforce", "kestra|KESTRACAREERSITE"),
            "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE",
        )


class JarvisPlatformLabelRepairTests(TestCase):
    def setUp(self):
        from companies.models import Company
        from harvest.models import CompanyPlatformLabel, JobBoardPlatform

        self.company = Company.objects.create(name="Appliedsystems")
        self.greenhouse, _ = JobBoardPlatform.objects.get_or_create(
            slug="greenhouse",
            defaults={"name": "Greenhouse", "is_enabled": True},
        )
        self.icims, _ = JobBoardPlatform.objects.get_or_create(
            slug="icims",
            defaults={"name": "iCIMS", "is_enabled": True},
        )
        self.label = CompanyPlatformLabel.objects.create(
            company=self.company,
            platform=self.greenhouse,
            tenant_id="appliedsystems",
            detection_method=CompanyPlatformLabel.DetectionMethod.URL_PATTERN,
            confidence=CompanyPlatformLabel.Confidence.MEDIUM,
        )

    def test_jarvis_can_repair_stale_platform_label_when_not_manual(self):
        from harvest.tasks import _jarvis_ensure_company_platform_label

        source_url = "https://careers-appliedsystems.icims.com/jobs/search"
        label, board_ctx = _jarvis_ensure_company_platform_label(
            company=self.company,
            detected_ats="icims",
            source_url=source_url,
            job_platform=self.icims,
        )
        label.refresh_from_db()
        self.assertIsNotNone(label)
        self.assertEqual(label.platform.slug, "icims")
        self.assertEqual(label.tenant_id, "careers-appliedsystems")
        self.assertEqual(board_ctx.get("platform_slug"), "icims")
        self.assertEqual(board_ctx.get("tenant_id"), "careers-appliedsystems")
        self.assertEqual(
            board_ctx.get("company_jobs_url"),
            "https://careers-appliedsystems.icims.com/jobs/search",
        )
        self.assertTrue(board_ctx.get("fetch_all_supported"))

    def test_manual_verified_label_is_not_overridden(self):
        from harvest.models import CompanyPlatformLabel
        from harvest.tasks import _jarvis_ensure_company_platform_label

        self.label.detection_method = CompanyPlatformLabel.DetectionMethod.MANUAL
        self.label.is_verified = True
        self.label.save(update_fields=["detection_method", "is_verified"])

        label, board_ctx = _jarvis_ensure_company_platform_label(
            company=self.company,
            detected_ats="icims",
            source_url="https://careers-appliedsystems.icims.com/jobs/search",
            job_platform=self.icims,
        )
        label.refresh_from_db()
        self.assertEqual(label.platform.slug, "greenhouse")
        self.assertEqual(board_ctx.get("platform_slug"), "greenhouse")
        self.assertEqual(board_ctx.get("tenant_id"), "appliedsystems")
        self.assertEqual(
            board_ctx.get("company_jobs_url"),
            "https://boards.greenhouse.io/appliedsystems",
        )

    def test_prefers_existing_label_for_detected_platform_when_multiple_labels_exist(self):
        from harvest.models import CompanyPlatformLabel
        from harvest.tasks import _jarvis_ensure_company_platform_label

        # CompanyPlatformLabel is OneToOne(company). Re-point the existing row to
        # iCIMS and verify Jarvis reuses it instead of creating anything new.
        self.label.platform = self.icims
        self.label.tenant_id = "careers-appliedsystems"
        self.label.detection_method = CompanyPlatformLabel.DetectionMethod.URL_PATTERN
        self.label.confidence = CompanyPlatformLabel.Confidence.HIGH
        self.label.save(
            update_fields=["platform", "tenant_id", "detection_method", "confidence"]
        )

        label, board_ctx = _jarvis_ensure_company_platform_label(
            company=self.company,
            detected_ats="icims",
            source_url="https://careers-appliedsystems.icims.com/jobs/search",
            job_platform=self.icims,
        )
        self.assertEqual(label.pk, self.label.pk)
        self.assertEqual(label.platform.slug, "icims")
        self.assertEqual(board_ctx.get("platform_slug"), "icims")


class JarvisCompanyAndRawJobDedupeTests(TestCase):
    def test_company_resolution_reuses_normalized_existing_company(self):
        from companies.models import Company
        from harvest.tasks import _jarvis_resolve_company

        existing = Company.objects.create(name="Applied Systems")
        resolved = _jarvis_resolve_company(
            "Appliedsystems",
            "https://careers-appliedsystems.icims.com/jobs/6419",
        )
        self.assertEqual(resolved.pk, existing.pk)

    def test_fetch_task_dedupes_same_external_id_with_different_urls(self):
        from companies.models import Company
        from harvest.models import CompanyPlatformLabel, JobBoardPlatform, RawJob
        from harvest.tasks import fetch_raw_jobs_for_company_task

        company = Company.objects.create(name="Acme")
        platform, _ = JobBoardPlatform.objects.get_or_create(
            slug="greenhouse",
            defaults={"name": "Greenhouse", "is_enabled": True},
        )
        label = CompanyPlatformLabel.objects.create(
            company=company,
            platform=platform,
            tenant_id="acme",
            confidence=CompanyPlatformLabel.Confidence.HIGH,
            detection_method=CompanyPlatformLabel.DetectionMethod.URL_PATTERN,
        )

        class _FakeHarvester:
            last_total_available = 2

            def fetch_jobs(self, *args, **kwargs):
                return [
                    {
                        "original_url": "https://example.com/jobs/view?jobId=123",
                        "apply_url": "https://example.com/jobs/view?jobId=123",
                        "external_id": "job-123",
                        "title": "Platform Engineer",
                        "company_name": "Acme",
                    },
                    {
                        "original_url": "https://example.com/jobs/123",
                        "apply_url": "https://example.com/jobs/123",
                        "external_id": "job-123",
                        "title": "Platform Engineer",
                        "company_name": "Acme",
                    },
                ]

        with patch("harvest.harvesters.get_harvester", return_value=_FakeHarvester()):
            out = fetch_raw_jobs_for_company_task.apply(
                kwargs={"label_pk": label.pk, "fetch_all": True}
            ).get()

        self.assertEqual(RawJob.objects.filter(platform_label=label).count(), 1)
        self.assertEqual(out["jobs_found"], 2)
        self.assertEqual(out["jobs_new"], 1)
        self.assertEqual(out["jobs_updated"], 1)

    def test_fetch_task_dedupes_query_variant_without_external_id(self):
        import hashlib

        from companies.models import Company
        from harvest.models import CompanyPlatformLabel, JobBoardPlatform, RawJob
        from harvest.tasks import fetch_raw_jobs_for_company_task

        company = Company.objects.create(name="Acme Query Variant")
        platform = JobBoardPlatform.objects.create(name="iCIMS Query", slug="icims-query-temp", is_enabled=True)
        label = CompanyPlatformLabel.objects.create(
            company=company,
            platform=platform,
            tenant_id="acme-query",
            confidence=CompanyPlatformLabel.Confidence.HIGH,
            detection_method=CompanyPlatformLabel.DetectionMethod.URL_PATTERN,
        )

        old_url = "https://example.com/jobs/6503?src=LinkedIn"
        old_hash = hashlib.sha256(old_url.encode("utf-8")).hexdigest()
        RawJob.objects.create(
            company=company,
            platform_label=label,
            job_platform=platform,
            title="Query Variant Role",
            original_url=old_url,
            apply_url=old_url,
            url_hash=old_hash,
            platform_slug="icims-query-temp",
            company_name=company.name,
        )

        class _FakeHarvester:
            last_total_available = 1

            def fetch_jobs(self, *args, **kwargs):
                return [
                    {
                        "original_url": "https://example.com/jobs/6503",
                        "apply_url": "https://example.com/jobs/6503",
                        "title": "Query Variant Role",
                        "company_name": company.name,
                    }
                ]

        with patch("harvest.harvesters.get_harvester", return_value=_FakeHarvester()):
            out = fetch_raw_jobs_for_company_task.apply(
                kwargs={"label_pk": label.pk, "fetch_all": True}
            ).get()

        self.assertEqual(RawJob.objects.filter(platform_label=label).count(), 1)
        self.assertEqual(out["jobs_found"], 1)
        self.assertEqual(out["jobs_new"], 0)
        self.assertEqual(out["jobs_updated"], 1)


class JarvisIngestDayforceIntegrationTests(TestCase):
    def test_ingest_auto_creates_dayforce_platform_and_company(self):
        from companies.models import Company
        from harvest.models import CompanyPlatformLabel, JobBoardPlatform, RawJob
        from harvest.tasks import jarvis_ingest_task

        JobBoardPlatform.objects.filter(slug="dayforce").delete()
        Company.objects.filter(name="Kestra").delete()

        source_url = "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE/jobs/6503?src=LinkedIn"
        mock_ingest = {
            "error": "",
            "platform_slug": "dayforce",
            "strategy": "api:dayforce",
            "title": "Platform Engineer",
            "company_name": "",
            "description": "Lead cloud platform engineering initiatives across secure Azure workloads.",
            "original_url": source_url,
            "apply_url": source_url,
            "raw_payload": {"source": "test"},
        }

        with patch("apps.harvest.jarvis.JobJarvis.ingest", return_value=mock_ingest):
            result = jarvis_ingest_task.apply(kwargs={"url": source_url, "user_id": None}).get()

        self.assertTrue(result.get("ok"))
        self.assertTrue(JobBoardPlatform.objects.filter(slug="dayforce").exists())
        company = Company.objects.get(name="Kestra")
        raw_job = RawJob.objects.get(pk=result["raw_job_id"])
        self.assertEqual(raw_job.company_id, company.pk)
        self.assertIsNotNone(raw_job.job_platform)
        self.assertEqual(raw_job.job_platform.slug, "dayforce")
        self.assertIsNotNone(raw_job.platform_label)
        self.assertEqual(raw_job.platform_label.tenant_id, "kestra|KESTRACAREERSITE")
        self.assertEqual(raw_job.platform_label.platform.slug, "dayforce")
        self.assertEqual((raw_job.raw_payload or {}).get("jarvis_tenant_id"), "kestra|KESTRACAREERSITE")
        self.assertEqual(
            (raw_job.raw_payload or {}).get("jarvis_company_jobs_url"),
            "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE",
        )
        self.assertTrue((raw_job.raw_payload or {}).get("jarvis_fetch_all_supported"))
        self.assertEqual((raw_job.raw_payload or {}).get("jarvis_detected_ats"), "dayforce")
        self.assertTrue(CompanyPlatformLabel.objects.filter(company=company, platform__slug="dayforce").exists())


class JarvisFetchAllCompanyViewTests(TestCase):
    def setUp(self):
        import hashlib

        from companies.models import Company
        from harvest.models import JobBoardPlatform, RawJob
        from users.models import User

        self.user = User.objects.create_user(
            username="jarvis_fetch_admin",
            email="jarvis_fetch_admin@example.com",
            password="testpass123",
            is_superuser=True,
        )
        self.client.force_login(self.user)
        self.company = Company.objects.create(name="Kestra")
        self.platform, _ = JobBoardPlatform.objects.get_or_create(
            slug="dayforce",
            defaults={"name": "Dayforce", "is_enabled": True},
        )
        self.source_url = "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE/jobs/6503?src=LinkedIn"
        h = hashlib.sha256(self.source_url.encode()).hexdigest()
        self.raw_job = RawJob.objects.create(
            company=self.company,
            job_platform=self.platform,
            title="Platform Engineer",
            url_hash=h,
            original_url=self.source_url,
            apply_url=self.source_url,
            description="JD text long enough for task wiring.",
            platform_slug="jarvis",
            raw_payload={"jarvis_detected_ats": "dayforce"},
        )

    def test_fetch_all_endpoint_queues_company_task_and_updates_payload(self):
        from types import SimpleNamespace

        from harvest.models import CompanyPlatformLabel

        with patch("apps.harvest.tasks.fetch_raw_jobs_for_company_task.apply_async", return_value=SimpleNamespace(id="task-123")) as mocked_apply:
            resp = self.client.post(
                reverse("harvest-jarvis-fetch-all"),
                {"raw_job_id": str(self.raw_job.pk)},
            )

        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("task_id"), "task-123")
        self.assertEqual(body.get("tenant_id"), "kestra|KESTRACAREERSITE")
        self.assertEqual(
            body.get("company_jobs_url"),
            "https://jobs.dayforcehcm.com/en-US/kestra/KESTRACAREERSITE",
        )
        self.assertTrue(body.get("progress_url"))
        parsed = urlparse(body["progress_url"])
        self.assertEqual(parsed.path, reverse("harvest-jarvis-fetch-all-progress"))
        qs = parse_qs(parsed.query)
        self.assertEqual(qs.get("task_id"), ["task-123"])
        self.assertEqual(qs.get("label_pk"), [str(body["label_pk"])])
        mocked_apply.assert_called_once()

        self.raw_job.refresh_from_db()
        self.assertIsNotNone(self.raw_job.platform_label)
        self.assertEqual(self.raw_job.platform_label.tenant_id, "kestra|KESTRACAREERSITE")
        self.assertTrue((self.raw_job.raw_payload or {}).get("jarvis_fetch_all_supported"))
        self.assertTrue(
            CompanyPlatformLabel.objects.filter(company=self.company, platform__slug="dayforce").exists()
        )

    def test_progress_api_returns_live_counts_and_recent_jobs(self):
        from django.utils import timezone

        from harvest.models import CompanyFetchRun, CompanyPlatformLabel

        label = CompanyPlatformLabel.objects.create(
            company=self.company,
            platform=self.platform,
            tenant_id="kestra|KESTRACAREERSITE",
            confidence=CompanyPlatformLabel.Confidence.HIGH,
        )
        self.raw_job.platform_label = label
        self.raw_job.save(update_fields=["platform_label", "updated_at"])

        run = CompanyFetchRun.objects.create(
            label=label,
            status=CompanyFetchRun.Status.RUNNING,
            task_id="task-live-1",
            started_at=timezone.now(),
            jobs_found=5,
            jobs_new=1,
            jobs_updated=1,
            jobs_duplicate=0,
            jobs_failed=0,
            triggered_by="JARVIS",
        )

        with patch("celery.result.AsyncResult") as mocked:
            mocked.return_value.state = "PROGRESS"
            mocked.return_value.info = {"percent": 44, "message": "Processing…"}
            resp = self.client.get(
                reverse("harvest-jarvis-fetch-all-progress-api"),
                {"task_id": run.task_id},
            )

        self.assertEqual(resp.status_code, 200, resp.content)
        body = resp.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("state"), "RUNNING")
        self.assertTrue(body.get("running"))
        self.assertFalse(body.get("done"))
        self.assertGreaterEqual(body.get("percent", 0), 1)
        self.assertEqual(body.get("counts", {}).get("found"), 5)
        self.assertEqual(body.get("counts", {}).get("new"), 1)
        self.assertTrue(body.get("rawjobs_url"))
        self.assertIn("recent_jobs", body)
        parsed = urlparse(body["rawjobs_url"])
        qs = parse_qs(parsed.query)
        self.assertEqual(qs.get("_subtab"), ["jobs"])
        self.assertEqual(qs.get("platform"), ["dayforce"])
        self.assertEqual(qs.get("company_id"), [str(self.company.pk)])
        self.assertEqual(qs.get("label_pk"), [str(label.pk)])


class RawJobPipelineUnificationTests(TestCase):
    def setUp(self):
        import hashlib

        from companies.models import Company
        from harvest.models import RawJob
        from users.models import User

        self.user = User.objects.create_user(
            username="raw_unify_admin",
            email="raw_unify_admin@example.com",
            password="testpass123",
            is_superuser=True,
        )
        self.client.force_login(self.user)
        self.company = Company.objects.create(name="UnifyCo")

        def _mk(
            suffix: str,
            *,
            desc: str = "",
            sync_status: str = RawJob.SyncStatus.PENDING,
            quality_score=None,
            jd_quality_score=None,
            category_confidence=None,
            classification_confidence=None,
            is_active: bool = True,
            word_count: int = 0,
        ) -> RawJob:
            url = f"https://example.com/jobs/{suffix}"
            return RawJob.objects.create(
                company=self.company,
                company_name="UnifyCo",
                title=f"Role {suffix}",
                url_hash=hashlib.sha256(url.encode()).hexdigest(),
                original_url=url,
                description=desc,
                sync_status=sync_status,
                quality_score=quality_score,
                jd_quality_score=jd_quality_score,
                category_confidence=category_confidence,
                classification_confidence=classification_confidence,
                is_active=is_active,
                word_count=word_count,
            )

        _mk("fetched", desc="")
        _mk("parsed", desc="Parsed description text")
        _mk("enriched", desc="Enriched text", quality_score=0.71)
        _mk("classified", desc="Classified text", quality_score=0.81, category_confidence=0.24)
        _mk("ready", desc="Ready text", quality_score=0.92, category_confidence=0.84, word_count=220)
        _mk(
            "synced",
            desc="Synced text",
            sync_status=RawJob.SyncStatus.SYNCED,
            quality_score=0.95,
            category_confidence=0.90,
            word_count=260,
        )

    def test_funnel_counts_match_stage_filters(self):
        from harvest.models import RawJob
        from harvest.services.pipeline_snapshot import raw_jobs_workflow_insights
        from harvest.services.rawjob_query import apply_rawjob_filters

        insights = raw_jobs_workflow_insights(stale_pending_hours=6)
        funnel = insights["funnel"]
        stage_to_key = {
            "FETCHED": "fetched",
            "PARSED": "parsed",
            "ENRICHED": "enriched",
            "CLASSIFIED": "classified",
            "READY": "ready",
            "SYNCED": "synced",
        }
        for stage, key in stage_to_key.items():
            expected = apply_rawjob_filters(RawJob.objects.all(), {"stage": stage}).count()
            self.assertEqual(
                funnel[key],
                expected,
                msg=f"Funnel mismatch for stage={stage}: {funnel[key]} != {expected}",
            )

    def test_rawjobs_stage_page_count_matches_shared_filter(self):
        from harvest.models import RawJob
        from harvest.services.rawjob_query import apply_rawjob_filters

        response = self.client.get(reverse("harvest-rawjobs"), {"stage": "CLASSIFIED"})
        self.assertEqual(response.status_code, 200)
        expected = apply_rawjob_filters(RawJob.objects.all(), {"stage": "CLASSIFIED"}).count()
        self.assertEqual(response.context["paginator"].count, expected)

    def test_jobs_pipeline_uses_shared_raw_total_snapshot(self):
        from harvest.services.pipeline_snapshot import load_rawjobs_dashboard_stats

        response = self.client.get(reverse("jobs-pipeline"), {"tab": "raw"})
        self.assertEqual(response.status_code, 200)
        stats = load_rawjobs_dashboard_stats(force_refresh=False)
        self.assertEqual(response.context["raw_total"], stats["total"])

    def test_classification_bucket_low_filter(self):
        from harvest.models import RawJob
        from harvest.services.rawjob_query import apply_rawjob_filters

        low_qs = apply_rawjob_filters(
            RawJob.objects.all(),
            {"classification_bucket": "low"},
        )
        self.assertEqual(low_qs.count(), 1)
        self.assertEqual(low_qs.first().title, "Role classified")

    def test_jobs_pipeline_raw_gate_summary_counts(self):
        response = self.client.get(reverse("jobs-pipeline"), {"tab": "raw"})
        self.assertEqual(response.status_code, 200)
        summary = response.context["raw_gate_summary"]
        self.assertEqual(summary["pending_total"], 5)
        self.assertEqual(summary["qualified_pending"], 1)
        self.assertEqual(summary["qualified_synced"], 1)
        self.assertEqual(summary["blocked_missing_jd"], 1)
        self.assertEqual(summary["blocked_inactive"], 0)
        self.assertEqual(summary["blocked_low_conf"], 1)

    def test_jobs_pipeline_raw_stage_filter_uses_shared_filter(self):
        from harvest.models import RawJob
        from harvest.services.rawjob_query import apply_rawjob_filters

        response = self.client.get(reverse("jobs-pipeline"), {"tab": "raw", "stage": "CLASSIFIED"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["raw_selected_stage"], "CLASSIFIED")

        expected_ids = list(
            apply_rawjob_filters(RawJob.objects.all(), {"stage": "CLASSIFIED"})
            .order_by("-fetched_at")
            .values_list("id", flat=True)[:200]
        )
        actual_ids = [row.id for row in response.context["tab_raw"]]
        self.assertEqual(actual_ids, expected_ids)

        html = response.content.decode("utf-8")
        self.assertIn("?tab=raw&stage=CLASSIFIED#raw-jobs-table", html)
        self.assertIn("?tab=raw&sync_status=PENDING&has_jd=0#raw-jobs-table", html)

    def test_jobs_pipeline_pool_gate_filter(self):
        from jobs.models import Job

        posted_by = self.user
        Job.objects.create(
            title="Pool eligible",
            company="UnifyCo",
            description="good",
            status=Job.Status.POOL,
            posted_by=posted_by,
            gate_status=Job.GateStatus.ELIGIBLE,
            vet_lane=Job.VetLane.AUTO,
        )
        Job.objects.create(
            title="Pool blocked",
            company="UnifyCo",
            description="bad",
            status=Job.Status.POOL,
            posted_by=posted_by,
            gate_status=Job.GateStatus.BLOCKED,
            vet_lane=Job.VetLane.BLOCKED,
        )
        response = self.client.get(reverse("jobs-pipeline"), {"tab": "pool", "gate": "BLOCKED"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["gate_tab"], "BLOCKED")
        rows = list(response.context["tab_jobs"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "Pool blocked")


class HarvestPhase3NavigationTests(TestCase):
    def setUp(self):
        from users.models import User

        self.user = User.objects.create_user(
            username="phase3_nav_admin",
            email="phase3_nav_admin@example.com",
            password="testpass123",
            is_superuser=True,
        )
        self.client.force_login(self.user)

    @patch("apps.harvest.tasks.sync_harvested_to_pool_task.delay")
    def test_run_sync_redirects_back_to_pipeline_raw_tab(self, mock_delay):
        mock_delay.return_value = MagicMock(id="task-1234-abcd")
        response = self.client.post(
            reverse("harvest-run-sync"),
            {
                "qualified_only": "1",
                "max_jobs": "0",
                "chunk_size": "500",
                "return_to": "jobs-pipeline",
                "return_tab": "raw",
            },
        )
        self.assertEqual(response.status_code, 302)
        parsed = urlparse(response["Location"])
        self.assertEqual(parsed.path, reverse("jobs-pipeline"))
        qs = parse_qs(parsed.query)
        self.assertEqual(qs.get("tab"), ["raw"])
        self.assertEqual(qs.get("tp"), ["task-1234-abcd"])
        self.assertEqual(qs.get("tpl"), ["Sync Qualified to Vet Queue"])

    def test_rawjobs_batches_html_redirects_to_rawjobs_subtab(self):
        response = self.client.get(reverse("harvest-rawjobs-batches"))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{reverse('harvest-rawjobs')}?_subtab=batches")

    def test_rawjobs_company_status_html_redirects_to_rawjobs_subtab(self):
        response = self.client.get(reverse("harvest-rawjobs-company-status"))
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response["Location"], f"{reverse('harvest-rawjobs')}?_subtab=companies")
