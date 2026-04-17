"""
OracleHCMHarvester — Oracle HCM Cloud Candidate Experience REST API

Oracle HCM exposes a public REST API for job requisitions (no auth for public jobs):
  GET https://{subdomain}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions
      ?onlyData=true&limit=100&offset=0
      &finder=findReqs;siteNumber={sites_id},facetsList=LOCATIONS%3BTITLES%3BORGANIZATIONS
      &expand=requisitionList.primaryLocation,requisitionList.otherLocations

tenant_id stored as "{subdomain}|{sites_id}"
  e.g. "eeho.fa.us2|CX"  or  "fusion.oracle.com|hcmUI"
"""
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

PAGE_SIZE = 100


class OracleHCMHarvester(BaseHarvester):
    platform_slug = "oracle"

    def fetch_jobs(
        self, company, tenant_id: str, since_hours: int = 24, fetch_all: bool = False
    ) -> list[dict[str, Any]]:
        self.last_total_available = 0
        if not tenant_id or "|" not in tenant_id:
            return []

        subdomain, sites_id = tenant_id.split("|", 1)
        subdomain = subdomain.strip()
        sites_id = sites_id.strip()
        if not subdomain or not sites_id:
            return []

        base_url = (
            f"https://{subdomain}.oraclecloud.com"
            f"/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
        )

        results: list[dict] = []
        offset = 0

        while True:
            params = {
                "onlyData": "true",
                "limit": PAGE_SIZE,
                "offset": offset,
                "finder": f"findReqs;siteNumber={sites_id}",
                "expand": "requisitionList",
            }
            data = self._get(base_url, params=params)

            if not isinstance(data, dict) or "error" in data:
                break

            items = data.get("items") or []
            if items and isinstance(items[0], dict):
                total_from_search = int(items[0].get("TotalJobsCount") or 0)
                if total_from_search:
                    self.last_total_available = total_from_search
            for item in items:
                for req in item.get("requisitionList") or []:
                    results.append(
                        self._normalize(req, subdomain, sites_id, company.name)
                    )

            # Oracle REST uses hasMore + totalResults
            has_more = data.get("hasMore", False)
            total = int(data.get("totalResults") or 0)
            if total:
                self.last_total_available = total
            # API returns search wrapper items; page via explicit offset step.
            offset += PAGE_SIZE

            if not fetch_all or not has_more or (total and offset >= total):
                break
            time.sleep(MIN_DELAY_API)

        return results

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize(self, req: dict, subdomain: str, sites_id: str, company_name: str) -> dict:
        req_id = str(req.get("Id") or req.get("requisitionId") or "")
        title = req.get("Title") or req.get("title") or ""

        primary_loc = (req.get("primaryLocation") or {})
        city = primary_loc.get("City") or primary_loc.get("city") or ""
        state = primary_loc.get("State") or primary_loc.get("state") or ""
        country = primary_loc.get("Country") or primary_loc.get("country") or ""
        location_raw = ", ".join(x for x in [city, state, country] if x)

        work_loc = (req.get("PrimaryWorkLocation") or "").lower()
        if "remote" in work_loc or req.get("WorkFromHome"):
            is_remote = True
            location_type = "REMOTE"
        elif "hybrid" in work_loc:
            is_remote = False
            location_type = "HYBRID"
        elif location_raw:
            is_remote = False
            location_type = "ONSITE"
        else:
            is_remote = False
            location_type = "UNKNOWN"

        # Build the job URL — Oracle uses the candidate experience portal path
        job_url = (
            f"https://{subdomain}.oraclecloud.com"
            f"/hcmUI/CandidateExperience/en/sites/{sites_id}/job/{req_id}"
        )

        posted_raw = req.get("PostedDate") or req.get("postedDate") or ""

        return {
            "external_id": req_id,
            "original_url": job_url,
            "apply_url": job_url,
            "title": title,
            "company_name": company_name,
            "department": req.get("Organization") or req.get("PrimaryOrganization") or "",
            "team": "",
            "location_raw": location_raw,
            "city": city,
            "state": state,
            "country": country,
            "is_remote": is_remote,
            "location_type": location_type,
            "employment_type": "UNKNOWN",
            "experience_level": "UNKNOWN",
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "",
            "salary_raw": "",
            "description": "",
            "requirements": "",
            "benefits": "",
            "posted_date_raw": posted_raw,
            "closing_date": "",
            "raw_payload": req,
        }
