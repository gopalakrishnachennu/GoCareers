"""
OracleHCMHarvester — Oracle HCM Cloud Candidate Experience REST API

Oracle HCM exposes a public REST API for job requisitions (no auth for public jobs):
  GET https://{subdomain}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions
      ?onlyData=true&limit=100&offset=0
      &finder=findReqs;siteNumber={sites_id},facetsList=LOCATIONS%3BTITLES%3BORGANIZATIONS
      &expand=requisitionList

tenant_id stored as "{subdomain}|{sites_id}"
  e.g. "eeho.fa.us2|CX"  or  "fusion.oracle.com|hcmUI"
"""
import re
import time
from typing import Any

from .base import BaseHarvester, MIN_DELAY_API

PAGE_SIZE = 100


def _oracle_str(value: Any) -> str:
    """Best-effort stringifier for Oracle API fields that may be nested/list-like."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in ("displayValue", "Meaning", "meaning", "value", "Value", "label", "Label", "name", "Name"):
            if key in value and value.get(key):
                return _oracle_str(value.get(key))
        return ""
    if isinstance(value, list):
        parts = [_oracle_str(v) for v in value]
        return " | ".join(p for p in parts if p)
    return str(value).strip()


def _oracle_first(payload: dict, *keys: str) -> str:
    for key in keys:
        if key in payload:
            val = _oracle_str(payload.get(key))
            if val:
                return val
    return ""


def _oracle_parse_postal(location_block: str) -> str:
    match = re.search(r"\b\d{5}(?:-\d{4})?\b", location_block or "")
    return match.group(0) if match else ""


def _oracle_parse_location_parts(location_block: str) -> tuple[str, str, str]:
    text = (location_block or "").strip()
    if not text:
        return "", "", ""
    compact = re.sub(r"\s+", " ", text)
    us_match = re.search(
        r"(?P<city>[^,]+),\s*(?P<state>[A-Z]{2}),\s*(?P<postal>\d{5}(?:-\d{4})?)?,?\s*(?P<country>US|USA|United States)\b",
        compact,
        re.I,
    )
    if us_match:
        return (
            us_match.group("city").strip(),
            us_match.group("state").strip(),
            _oracle_normalize_country(us_match.group("country")),
        )
    parts = [p.strip() for p in compact.split(",") if p.strip()]
    if len(parts) >= 3:
        city = parts[-3]
        state = parts[-2]
        country = _oracle_normalize_country(parts[-1])
        return city, state, country
    return "", "", ""


def _oracle_work_location(detail: dict[str, Any]) -> dict[str, Any]:
    work_locations = detail.get("workLocation") or detail.get("WorkLocation") or []
    if isinstance(work_locations, list) and work_locations:
        first = work_locations[0]
        if isinstance(first, dict):
            return first
    return {}


def _oracle_normalize_country(country: str) -> str:
    value = (country or "").strip()
    if value.upper() == "US":
        return "United States"
    return value


def _oracle_map_employment_type(*values: str) -> str:
    text = " ".join(v.lower() for v in values if v).strip()
    if not text:
        return "UNKNOWN"
    if "intern" in text:
        return "INTERNSHIP"
    if "contract" in text or "contingent" in text:
        return "CONTRACT"
    if "temp" in text:
        return "TEMPORARY"
    if "part" in text:
        return "PART_TIME"
    if "full" in text or "regular" in text:
        return "FULL_TIME"
    return "UNKNOWN"


def _oracle_map_education_level(value: str) -> str:
    text = (value or "").lower()
    if not text:
        return ""
    if "doctor" in text or "phd" in text:
        return "PHD"
    if "master" in text or text in {"ms", "m.s"}:
        return "MS"
    if "bachelor" in text or text in {"bs", "b.s", "ba", "b.a", "be", "b.e"}:
        return "BS"
    if "associate" in text:
        return "ASSOCIATE"
    if "high school" in text or text == "hs":
        return "HS"
    return ""


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
        total_jobs = 0  # populated from TotalJobsCount on first page

        while True:
            params = {
                "onlyData": "true",
                "limit": PAGE_SIZE,
                "offset": offset,
                "finder": f"findReqs;siteNumber={sites_id}",
                # Oracle rejects nested expand values on this list endpoint.
                # We use the requisition list for discovery, then fetch each job's
                # detail payload via recruitingCEJobRequisitionDetails.
                "expand": "requisitionList",
            }
            data = self._get(base_url, params=params)

            if not isinstance(data, dict) or "error" in data:
                break

            items = data.get("items") or []
            if items and isinstance(items[0], dict):
                count = int(items[0].get("TotalJobsCount") or 0)
                if count:
                    total_jobs = count
                    self.last_total_available = count

            page_results: list[dict] = []
            for item in items:
                for req in item.get("requisitionList") or []:
                    detail = self._fetch_detail(subdomain, sites_id, str(req.get("Id") or req.get("requisitionId") or ""))
                    page_results.append(
                        self._normalize(req, detail, subdomain, sites_id, company.name)
                    )
            results.extend(page_results)

            offset += PAGE_SIZE

            # Oracle wraps all jobs in ONE wrapper item, so top-level hasMore/totalResults
            # always equal False/1 — they are useless for job-level pagination.
            # Use TotalJobsCount (from the wrapper) as the real total.
            if not fetch_all or not page_results or (total_jobs and offset >= total_jobs):
                break
            time.sleep(MIN_DELAY_API)

        return results

    # ── Normalization ─────────────────────────────────────────────────────────

    def _fetch_detail(self, subdomain: str, sites_id: str, req_id: str) -> dict[str, Any]:
        if not req_id:
            return {}
        detail_url = (
            f"https://{subdomain}.oraclecloud.com"
            f"/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
        )
        params = {
            "onlyData": "true",
            "expand": "all",
            "finder": f"ById;Id={req_id},siteNumber={sites_id}",
        }
        data = self._get(detail_url, params=params)
        if not isinstance(data, dict) or "error" in data:
            return {}
        items = data.get("items") or []
        if items and isinstance(items[0], dict):
            return items[0]
        return {}

    def _normalize(self, req: dict, detail: dict[str, Any], subdomain: str, sites_id: str, company_name: str) -> dict:
        req_id = str(req.get("Id") or req.get("requisitionId") or "")
        title = _oracle_first(detail, "Title", "title") or req.get("Title") or req.get("title") or ""
        work_location = _oracle_work_location(detail)

        primary_loc = detail.get("primaryLocation") or req.get("primaryLocation") or {}
        if isinstance(primary_loc, dict):
            city = _oracle_first(primary_loc, "City", "city")
            state = _oracle_first(primary_loc, "State", "state")
            country = _oracle_normalize_country(_oracle_first(primary_loc, "Country", "country"))
        else:
            city = ""
            state = ""
            country = ""
        city = city or _oracle_first(work_location, "TownOrCity", "City")
        state = state or _oracle_first(work_location, "Region2", "State", "StateProvince")
        country = country or _oracle_normalize_country(_oracle_first(work_location, "Country", "CountryCode"))

        vendor_location_block = _oracle_first(
            work_location,
            "AddressLine1",
        )
        work_location_line = ", ".join(
            part
            for part in [
                _oracle_first(work_location, "AddressLine1"),
                _oracle_first(work_location, "TownOrCity", "City"),
                _oracle_first(work_location, "Region2", "State", "StateProvince"),
                _oracle_first(work_location, "PostalCode"),
                _oracle_normalize_country(_oracle_first(work_location, "Country", "CountryCode")),
            ]
            if part
        )
        vendor_location_block = work_location_line or _oracle_first(
            detail,
            "Locations",
            "Location",
            "PrimaryLocation",
            "PrimaryWorkLocation",
            "JobLocation",
            "Address",
        )
        if not vendor_location_block:
            vendor_location_block = _oracle_first(req, "PrimaryLocation", "Location")
        parsed_city, parsed_state, parsed_country = _oracle_parse_location_parts(vendor_location_block)
        city = city or parsed_city
        state = state or parsed_state
        country = country or parsed_country or _oracle_normalize_country(_oracle_first(req, "PrimaryLocationCountry"))
        location_raw = vendor_location_block or ", ".join(x for x in [city, state, country] if x)
        postal_code = _oracle_first(work_location, "PostalCode") or _oracle_parse_postal(vendor_location_block)

        work_loc = _oracle_first(detail, "PrimaryWorkLocation", "WorkLocation", "Workplace", "WorkplaceType") or _oracle_first(req, "PrimaryWorkLocation")
        work_loc = work_loc.lower()
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

        posted_raw = (
            _oracle_first(detail, "ExternalPostedStartDate", "PostedDate", "postedDate", "PostingDate")
            or req.get("PostedDate")
            or req.get("postedDate")
            or req.get("ExternalPostedStartDate")
            or ""
        )
        description = _oracle_first(detail, "ExternalDescriptionStr", "Description", "JobDescription", "ShortDescriptionStr")
        requirements = _oracle_first(detail, "ExternalQualificationsStr", "Qualifications", "RequiredQualifications")
        responsibilities = _oracle_first(detail, "ExternalResponsibilitiesStr", "Responsibilities", "JobResponsibilities")
        department = _oracle_first(
            detail,
            "Organization",
            "OrganizationDescriptionStr",
            "PrimaryOrganization",
            "BusinessUnit",
            "Department",
        ) or req.get("Organization") or req.get("PrimaryOrganization") or req.get("Department") or ""
        vendor_job_identification = _oracle_first(
            detail,
            "JobIdentification",
            "Job Identification",
            "JobIdentifier",
            "JobIdentifierNumber",
            "JobCode",
            "Job Number",
            "RequisitionNumber",
        ) or req_id
        vendor_job_category = _oracle_first(detail, "JobCategory", "Job Category", "Category", "Family", "JobFunction")
        vendor_degree_level = _oracle_first(detail, "DegreeLevel", "Degree Level", "EducationLevel", "RequiredEducation", "StudyLevel")
        vendor_job_schedule = _oracle_first(detail, "JobSchedule", "Job Schedule", "FullPartTime", "RegularTemporary")
        vendor_job_shift = _oracle_first(detail, "JobShift", "Job Shift", "Shift")
        employment_type = _oracle_map_employment_type(vendor_job_schedule, _oracle_first(detail, "RegularTemporary"), _oracle_first(detail, "FullPartTime"))
        education_required = _oracle_map_education_level(vendor_degree_level)
        raw_payload = {
            "source": "oracle_hcm",
            "summary": req,
            "detail": detail or {},
        }

        return {
            "external_id": req_id,
            "original_url": job_url,
            "apply_url": job_url,
            "title": title,
            "company_name": company_name,
            "department": department,
            "team": "",
            "location_raw": location_raw,
            "city": city,
            "state": state,
            "country": country,
            "postal_code": postal_code,
            "is_remote": is_remote,
            "location_type": location_type,
            "employment_type": employment_type,
            "experience_level": "UNKNOWN",
            "salary_min": None,
            "salary_max": None,
            "salary_currency": "USD",
            "salary_period": "",
            "salary_raw": "",
            "description": description,
            "requirements": requirements,
            "responsibilities": responsibilities,
            "benefits": "",
            "posted_date_raw": posted_raw,
            "closing_date": "",
            "job_category": vendor_job_category,
            "education_required": education_required,
            "schedule_type": vendor_job_schedule,
            "shift_schedule": vendor_job_shift,
            "shift_details": vendor_job_shift,
            "vendor_job_identification": vendor_job_identification,
            "vendor_job_category": vendor_job_category,
            "vendor_degree_level": vendor_degree_level,
            "vendor_job_schedule": vendor_job_schedule,
            "vendor_job_shift": vendor_job_shift,
            "vendor_location_block": vendor_location_block,
            "raw_payload": raw_payload,
        }
