"""
Re-run AI summaries only over existing results.csv (no re-scraping).

Reads each district row from results.csv, rebuilds search_results from CSV
columns (including Context Snippets), calls Perplexity to get a new summary
+ citations, then updates the CSV row with the new AI Summary and AI Engine Sources.
Keeps the same prompt content as the original run (terms, URLs, context snippets).
"""
import argparse
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import logging
import time

from config import RESULTS_CSV
from csv_generator import load_existing_results, update_csv_with_district
from ai_context import get_ai_contextualization

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_context_snippets_from_csv(snippets_str: str) -> list:
    """
    Parse "Context Snippets" column back into list of dicts for get_ai_contextualization.
    Stored format: "[term @ url (source)]: context" joined by " | ".
    """
    if not snippets_str or not str(snippets_str).strip():
        return []
    raw = str(snippets_str).strip()
    chunks = [c.strip() for c in raw.split(" | ") if c.strip()]
    result = []
    for chunk in chunks:
        # First "]: " separates "[term @ url (source)" from context
        idx = chunk.find("]: ")
        if idx == -1:
            continue
        prefix = chunk[:idx].strip()
        context = chunk[idx + 3 :].strip()
        if not prefix.startswith("["):
            continue
        prefix = prefix[1:]  # drop leading [
        # prefix is "term @ url (source)"
        try:
            last_paren = prefix.rfind(" (")
            if last_paren == -1:
                source = "unknown"
                rest = prefix
            else:
                rest = prefix[:last_paren].strip()
                source = prefix[last_paren + 2 :].rstrip(")").strip() or "unknown"
            # rest is "term @ url"
            at_idx = rest.find(" @ ")
            if at_idx == -1:
                continue
            term = rest[:at_idx].strip()
            url = rest[at_idx + 3 :].strip()
            result.append({"term": term, "url": url, "context": context, "source": source})
        except Exception:
            continue
    return result


def row_to_district_data(row) -> dict:
    """Build district_data dict expected by update_csv_with_district from a CSV row."""
    return {
        "DISTRICT_NAME": row.get("District") or row.get("District Name", ""),
        "ST": row.get("State", ""),
        "DISTRICT_URL": row.get("District Website") or row.get("District URL", ""),
        "District Website": row.get("District Website") or row.get("District URL", ""),
        "SCHOOLS_IN_DISTRICT": int(row.get("Count of Schools in the District", 0) or 0),
    }


def row_to_search_results(row) -> dict:
    """Build search_results from CSV row (including context snippets) for get_ai_contextualization."""
    terms_str = row.get("District Terms Found") or ""
    terms_found = [t.strip() for t in str(terms_str).split(",") if t.strip()] if terms_str else []

    page_urls_str = row.get("Page URLs Where Terms Found") or row.get("District Page URLs") or ""
    if isinstance(page_urls_str, str) and page_urls_str.strip():
        page_urls = [u.strip() for u in page_urls_str.replace(";", ",").split(",") if u.strip()]
    else:
        page_urls = []

    district_pages = int(row.get("District Pages With Terms", 0) or 0)
    district_total = int(row.get("District Total Occurrences", 0) or 0)

    context_snippets = parse_context_snippets_from_csv(row.get("Context Snippets") or "")

    return {
        "terms_found": terms_found,
        "page_urls": page_urls,
        "district_terms_found": terms_found,
        "district_page_urls": page_urls,
        "district_pages_with_terms": district_pages,
        "district_total_occurrences": district_total,
        "context_snippets": context_snippets,
    }


def main():
    parser = argparse.ArgumentParser(description="Re-run AI summaries only on existing results.csv")
    parser.add_argument("--csv", type=Path, default=RESULTS_CSV, help="Path to results CSV")
    parser.add_argument("--limit", type=int, default=None, help="Max number of districts to process")
    parser.add_argument("--delay", type=float, default=2.0, help="Seconds between API calls (rate limit)")
    args = parser.parse_args()

    csv_path = args.csv
    if not csv_path.is_absolute():
        csv_path = _project_root / csv_path
    if not csv_path.exists():
        logger.error("CSV not found: %s", csv_path)
        sys.exit(1)

    df = load_existing_results(csv_path)
    if df.empty:
        logger.warning("No rows in %s", csv_path)
        return

    rows = df.to_dict("records")
    if args.limit:
        rows = rows[: args.limit]
        logger.info("Limiting to first %d districts", args.limit)

    logger.info("Re-running AI summaries for %d districts (no re-scrape, with context snippets)", len(rows))
    updated = 0
    skipped_zero = 0
    for i, row in enumerate(rows):
        district_name = row.get("District") or row.get("District Name") or "Unknown"
        state = row.get("State", "")
        search_results = row_to_search_results(row)
        if (search_results.get("district_pages_with_terms") or 0) == 0:
            skipped_zero += 1
            logger.info("[%d/%d] %s (%s) — skipping (zero hits)", i + 1, len(rows), district_name, state)
            continue
        n_snippets = len(search_results.get("context_snippets") or [])
        logger.info("[%d/%d] %s (%s) — %d context snippets", i + 1, len(rows), district_name, state, n_snippets)

        district_data = row_to_district_data(row)
        scrape_status = row.get("Scrape Status", "success")

        ai_summaries = get_ai_contextualization(
            search_results,
            page_content_map={},
            school_name=None,
            district_name=district_name,
        )

        if ai_summaries:
            update_csv_with_district(
                district_data,
                search_results,
                ai_summaries,
                scrape_status,
                school_names=None,
                csv_path=csv_path,
            )
            updated += 1
            citations = ai_summaries.get("citations") or []
            logger.info("  -> Updated AI Summary + AI Engine Sources (%d citations)", len(citations))
        else:
            logger.warning("  -> No AI summary (skipping row update)")

        if i < len(rows) - 1 and args.delay > 0:
            time.sleep(args.delay)

    logger.info("Done. Updated %d districts, skipped %d with zero hits.", updated, skipped_zero)


if __name__ == "__main__":
    main()
