"""Data layer for State Overview and District Detail dashboard.

Loads district-level CSV, builds district records, and provides
aggregation by state for map heatmap and state summary charts.
"""
from __future__ import annotations

import base64
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote

import pandas as pd

from config import OUTPUT_DIR, RESULTS_CSV

logger = logging.getLogger(__name__)

# US state FIPS/2-letter code to full name (for map tooltips)
STATE_NAMES: Dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut",
    "DE": "Delaware", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}


def _normalize_value(value: Any) -> Any:
    """Convert pandas NA/NaN to None for JSON-safe dicts."""
    try:
        if pd.isna(value):
            return None
        if value is pd.NA or value is pd.NaT:
            return None
    except (TypeError, ValueError):
        pass
    if value is None or value == "":
        return None
    return value


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _district_id_from_row(row: Dict[str, Any]) -> str:
    """Produce a URL-safe stable id for a district (for links)."""
    url = row.get("District URL") or row.get("District Website") or ""
    state = row.get("State") or ""
    name = row.get("District") or ""
    if url and str(url).strip():
        # URL-safe encoding: base64url of the URL string
        raw = str(url).strip()
        return quote(raw, safe="")
    return quote(f"{name}::{state}", safe="")


def load_all_states_data(csv_path: Optional[Path] = None) -> List[Dict[str, Any]]:
    """Load CSV for all states (no filter), normalize NaN for JSON."""
    if csv_path is None:
        updated_csv = OUTPUT_DIR / "florida_with_ai_summary_updated.csv"
        if updated_csv.exists():
            csv_path = updated_csv
            logger.info("Using updated summaries CSV: %s", updated_csv)
        else:
            csv_path = RESULTS_CSV

    if not csv_path.exists():
        logger.warning("Results CSV not found: %s", csv_path)
        return []

    try:
        df = pd.read_csv(csv_path)
        logger.info("Loaded %d rows from CSV", len(df))
        records = df.to_dict("records")
        for record in records:
            for key, value in list(record.items()):
                record[key] = _normalize_value(value)
        return records
    except Exception as e:
        logger.error("Error loading data: %s", e, exc_info=True)
        return []


def csv_row_to_district_record(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map one CSV row to internal district record for dashboard.

    id: URL-safe stable key (District URL or District::State).
    totalKeywordHits: District Total Occurrences.
    urls: list of { url, totalHits } from Page URLs Where Terms Found (no per-URL counts in CSV).
    keywordCounts: derived from District Terms Found (present=1; CSV has no per-keyword counts).
    """
    district_id = _district_id_from_row(row)
    state = (row.get("State") or "").strip() or None
    district_name = (row.get("District") or row.get("District Name") or "").strip() or "Unknown"
    total_hits = _safe_int(row.get("District Total Occurrences") or row.get("Total Occurrences"))
    pages_with_terms = _safe_int(row.get("District Pages With Terms") or row.get("Pages With Terms"))
    scrape_status = (row.get("Scrape Status") or "").strip() or "unknown"
    successful_scrapes = 1 if scrape_status == "success" else 0

    # URLs with terms: we have "Page URLs Where Terms Found" or "District Page URLs" as comma-sep
    page_urls_str = row.get("Page URLs Where Terms Found") or row.get("District Page URLs") or ""
    if isinstance(page_urls_str, str) and page_urls_str.strip():
        url_list = [u.strip() for u in page_urls_str.split(",") if u.strip()]
    else:
        url_list = []

    # Build urls array: each entry has url, totalHits (we don't have per-URL counts in CSV)
    urls = [{"url": u, "keywordCounts": {}, "totalHits": total_hits if len(url_list) == 1 else 1} for u in url_list]
    if not urls and total_hits > 0:
        # No URL list but has hits: single placeholder
        urls = [{"url": row.get("District URL") or row.get("District Website") or "", "keywordCounts": {}, "totalHits": total_hits}]

    # Keyword list from "District Terms Found" (comma-sep); no per-keyword counts in CSV
    terms_str = row.get("District Terms Found") or ""
    if terms_str and str(terms_str).strip() and str(terms_str) != "None":
        terms_list = [t.strip() for t in str(terms_str).split(",") if t.strip()]
        keyword_counts = {t: 1 for t in terms_list}
    else:
        keyword_counts = {}

    ai_summary = row.get("AI Summary")
    if ai_summary is not None and str(ai_summary).strip():
        ai_summary = str(ai_summary).strip()
    else:
        ai_summary = None

    return {
        "id": district_id,
        "state": state,
        "districtName": district_name,
        "totalDistrictUrlsScraped": pages_with_terms,
        "successfulScrapes": successful_scrapes,
        "totalKeywordHits": total_hits,
        "keywordCounts": keyword_counts,
        "urls": urls,
        "aiSummary": ai_summary,
        "sourceLinks": url_list[:50] if url_list else None,
        "scrapeStatus": scrape_status,
        "pagesWithTerms": pages_with_terms,
    }


def build_district_records(csv_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert CSV rows to district records. Dedupe by id (keep first)."""
    seen: set[str] = set()
    records: List[Dict[str, Any]] = []
    for row in csv_rows:
        rec = csv_row_to_district_record(row)
        if rec["id"] in seen:
            continue
        seen.add(rec["id"])
        records.append(rec)
    return records


def aggregate_by_state(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Aggregate by state for heatmap and tooltips.

    Returns dict keyed by state code (2-letter) with:
    - totalKeywordHits
    - totalDistricts
    - districtsWithKeywords (count where totalKeywordHits > 0)
    - districtsWithSuccess (Scrape Status == success)
    - stateShare = stateTotalHits / nationalTotalHits (0 if national 0)
    """
    national_total = sum(r.get("totalKeywordHits") or 0 for r in records)
    by_state: Dict[str, Dict[str, Any]] = {}

    for r in records:
        state = r.get("state") or ""
        if not state or not isinstance(state, str):
            continue
        state = state.strip().upper()[:2]
        if state not in by_state:
            by_state[state] = {
                "totalKeywordHits": 0,
                "totalDistricts": 0,
                "districtsWithKeywords": 0,
                "districtsWithSuccess": 0,
            }
        by_state[state]["totalDistricts"] += 1
        hits = r.get("totalKeywordHits") or 0
        by_state[state]["totalKeywordHits"] += hits
        if hits > 0:
            by_state[state]["districtsWithKeywords"] += 1
        if (r.get("scrapeStatus") or "").lower() == "success":
            by_state[state]["districtsWithSuccess"] += 1

    for state, data in by_state.items():
        data["stateShare"] = (data["totalKeywordHits"] / national_total) if national_total else 0.0

    return by_state


def get_state_districts(state_code: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter district records by state (2-letter code)."""
    state_code = (state_code or "").strip().upper()[:2]
    return [r for r in records if (r.get("state") or "").strip().upper()[:2] == state_code]


def keyword_breakdown(state_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Per-keyword counts and percentages for selected state; skip 0 counts.

    Returns list of { keyword, count, pct } sorted by count desc.
    """
    totals: Dict[str, int] = {}
    for r in state_records:
        for k, v in (r.get("keywordCounts") or {}).items():
            if not k or (isinstance(v, (int, float)) and v <= 0):
                continue
            totals[k] = totals.get(k, 0) + (int(v) if isinstance(v, (int, float)) else 1)
    total = sum(totals.values())
    out = []
    for k, c in sorted(totals.items(), key=lambda x: -x[1]):
        if c <= 0:
            continue
        pct = (100.0 * c / total) if total else 0.0
        out.append({"keyword": k, "count": c, "pct": round(pct, 1)})
    return out


def get_district_by_id(district_id: str, records: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Resolve district_id (URL-safe or decoded) to district record."""
    if not district_id or not records:
        return None
    # Normalize to decoded form so we match whether caller sent encoded (table) or decoded (URL) id
    district_id_normalized = unquote(str(district_id).strip())
    for r in records:
        if unquote(str(r.get("id") or "")) == district_id_normalized:
            return r
    return None
