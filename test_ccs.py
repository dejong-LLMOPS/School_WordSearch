#!/usr/bin/env python3
"""Test scraper on Cumberland County Schools website."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from utils.logging_config import setup_logging
from web_scraper import WebScraper
from term_searcher import TermSearcher

# Setup logging
setup_logging(log_level='DEBUG')

# Test URL
test_url = "http://www.ccs.k12.nc.us"
target_page = "https://www.ccs.k12.nc.us/page/restorative-justice-centers-rjc-in-secondary-schools"

print("=" * 80)
print(f"Testing scraper on: {test_url}")
print(f"Looking for: {target_page}")
print("=" * 80)

scraper = WebScraper()
print(f"\n✓ Cloudscraper available: {scraper.cloudscraper_session is not None}")

# Scrape the site
print(f"\nStarting scrape...")
pages = scraper.scrape_site(test_url, max_depth=3, max_pages=30)

print(f"\n{'='*80}")
print(f"RESULTS:")
print(f"{'='*80}")
print(f"Total pages scraped: {len(pages)}")
print(f"\nURLs found:")
for i, page in enumerate(pages, 1):
    print(f"  {i}. {page['url']}")
    print(f"     - Links extracted: {len(page.get('links', []))}")
    print(f"     - Content length: {len(page.get('text', ''))} chars")
    if page.get('links'):
        print(f"     - Sample links: {page['links'][:3]}")

# Check if target page was found
found_target = any(target_page in page['url'] for page in pages)
print(f"\n{'='*80}")
print(f"Target page found: {'✓ YES' if found_target else '✗ NO'}")
if found_target:
    target_page_data = next((p for p in pages if target_page in p['url']), None)
    if target_page_data:
        print(f"  URL: {target_page_data['url']}")
        print(f"  Content length: {len(target_page_data.get('text', ''))} chars")

# Search for restorative justice terms
print(f"\n{'='*80}")
print("SEARCHING FOR TERMS:")
print(f"{'='*80}")

search_terms = ["restorative justice", "restorative practices", "race equity"]
searcher = TermSearcher()

for term in search_terms:
    results = searcher.find_term_occurrences(term, pages)
    count = results.get('count', 0)
    pages_with_term = results.get('pages', [])
    
    print(f"\n'{term}':")
    print(f"  Found: {count} occurrences")
    print(f"  Pages: {len(pages_with_term)}")
    if pages_with_term:
        for page_url in pages_with_term[:5]:
            print(f"    - {page_url}")
            # Show a snippet
            for snippet in results.get('context_snippets', []):
                if snippet['url'] == page_url:
                    print(f"      \"{snippet['context'][:100]}...\"")
                    break

print(f"\n{'='*80}")
print("TEST COMPLETE")
print(f"{'='*80}")

