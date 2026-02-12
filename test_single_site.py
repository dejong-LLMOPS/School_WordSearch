#!/usr/bin/env python3
"""Test the full pipeline on a single website."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from utils.logging_config import setup_logging
from web_scraper import scrape_school_urls
from term_searcher import search_school_content
from csv_generator import create_results_dataframe, append_to_csv
from config import SEARCH_TERMS, RESULTS_CSV
import pandas as pd

# Try to import AI context (optional)
try:
    from ai_context import get_ai_contextualization
    from config import PERPLEXITY_CONFIG
    AI_AVAILABLE = True
    AI_KEY_AVAILABLE = bool(PERPLEXITY_CONFIG.get('api_key', ''))
except ImportError as e:
    print(f"⚠ AI contextualization not available (perplexity module not installed: {e})")
    AI_AVAILABLE = False
    AI_KEY_AVAILABLE = False
    def get_ai_contextualization(*args, **kwargs):
        return {}

# Setup logging
setup_logging(log_level='INFO')

# Test on Cumberland County Schools
# Use the same field names as the data extractor provides
test_school = {
    'SCH_NAME': 'Cumberland County Schools (TEST)',
    'DISTRICT_NAME': 'Cumberland County Schools',
    'LEA_NAME': 'Cumberland County Schools',
    'ST': 'NC',
    'SCHOOL_URL': '',  # No school-specific URL
    'DISTRICT_URL': 'http://www.ccs.k12.nc.us'
}

print("=" * 80)
print("TESTING FULL PIPELINE ON SINGLE SITE")
print("=" * 80)
print(f"School: {test_school['SCH_NAME']}")
print(f"URL: {test_school['DISTRICT_URL']}")
print(f"Search terms: {', '.join(SEARCH_TERMS[:5])}...")
print("=" * 80)

# Step 1: Scrape
print("\n[1/4] Scraping website...")
# Generate a test school ID
test_school_id = test_school.get('SCH_NAME', 'TEST_SCHOOL')
pages = scrape_school_urls(
    test_school.get('SCHOOL_URL'),
    test_school.get('DISTRICT_URL'),
    use_cache=False,  # Don't use cache for test
    school_id=test_school_id
)

if not pages:
    print("✗ No pages scraped!")
    sys.exit(1)

print(f"✓ Scraped {len(pages)} pages")
print(f"  Sample URLs:")
for page in pages[:5]:
    print(f"    - {page['url']} ({len(page.get('links', []))} links)")

# Step 2: Search for terms
print(f"\n[2/4] Searching for terms...")
search_results = search_school_content(pages, search_terms=SEARCH_TERMS)

terms_found = search_results.get('terms_found', [])
print(f"✓ Found {len(terms_found)} unique terms: {', '.join(terms_found)}")
print(f"  Total occurrences: {search_results.get('total_count', 0)}")
print(f"  Pages with terms: {len(search_results.get('pages', []))}")

if terms_found:
    print(f"\n  Pages where terms were found:")
    for page_url in search_results.get('pages', [])[:10]:
        print(f"    - {page_url}")
    
    print(f"\n  Sample context snippets:")
    for snippet in search_results.get('context_snippets', [])[:3]:
        print(f"    Term: '{snippet['term']}'")
        print(f"    URL: {snippet['url']}")
        print(f"    Context: \"{snippet['context'][:150]}...\"")
        print()

# Step 3: AI Contextualization
print(f"\n[3/4] Getting AI contextualization...")
if not AI_AVAILABLE:
    print("⚠ Skipping AI contextualization (perplexity module not installed)")
    ai_summaries = {}
elif not AI_KEY_AVAILABLE:
    print("⚠ Skipping AI contextualization (API key not configured in .env)")
    print("   Set PERPLEXITY_API_KEY in .env file to enable AI contextualization")
    ai_summaries = {}
elif not terms_found:
    print("⚠ Skipping AI contextualization (no terms found to contextualize)")
    ai_summaries = {}
else:
    print(f"  Requesting AI summary (unified mode)...")
    page_content_map = {page['url']: page.get('text', '') for page in pages}
    try:
        ai_summaries = get_ai_contextualization(
            search_results, 
            page_content_map,
            school_name=test_school.get('SCH_NAME'),
            district_name=test_school.get('DISTRICT_NAME') or test_school.get('LEA_NAME')
        )
        if ai_summaries:
            if 'summary' in ai_summaries:
                # Unified mode
                print(f"✓ Got unified AI summary")
                print(f"\n  Summary: {ai_summaries['summary'][:500]}...")
            else:
                # Per-term mode (legacy)
                print(f"✓ Got {len(ai_summaries)} AI summaries")
                for key, summary_data in list(ai_summaries.items())[:3]:
                    if isinstance(summary_data, dict):
                        print(f"\n  Term: '{summary_data.get('term', '')}'")
                        print(f"  URL: {summary_data.get('url', '')}")
                        print(f"  Summary: {summary_data.get('ai_summary', '')[:250]}...")
        else:
            print("⚠ No AI summaries returned (API may have rate limited or failed)")
    except Exception as e:
        print(f"⚠ Error getting AI contextualization: {e}")
        ai_summaries = {}

# Step 4: Generate CSV row
print(f"\n[4/4] Generating CSV output...")

# Create DataFrame row
df = create_results_dataframe(
    school_data=test_school,
    search_results=search_results,
    ai_summaries=ai_summaries,
    scrape_status='success'
)

print(f"✓ CSV row generated")
print(f"  Columns: {list(df.columns)}")

# Write to test CSV
test_csv = RESULTS_CSV.parent / 'test_results.csv'
append_to_csv(df, test_csv)

print(f"✓ Results written to {test_csv}")

print("\n" + "=" * 80)
print("TEST COMPLETE!")
print("=" * 80)
print(f"\nSummary:")
print(f"  Pages scraped: {len(pages)}")
print(f"  Terms found: {len(terms_found)}")
print(f"  Total occurrences: {search_results.get('total_count', 0)}")
print(f"  AI summaries: {len(ai_summaries)}")
print(f"\nResults saved to: {test_csv}")

