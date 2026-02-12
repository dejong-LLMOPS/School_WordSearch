#!/usr/bin/env python3
"""Quick test to find restorative justice page on CCS website."""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from utils.logging_config import setup_logging
from web_scraper import WebScraper
from term_searcher import TermSearcher

# Setup logging (less verbose)
setup_logging(log_level='INFO')

test_url = "http://www.ccs.k12.nc.us"
target_page = "restorative-justice-centers-rjc-in-secondary-schools"

print(f"Testing: {test_url}")
print(f"Looking for: {target_page}")
print("-" * 80)

scraper = WebScraper()
pages = scraper.scrape_site(test_url, max_depth=2, max_pages=50)

print(f"\n✓ Scraped {len(pages)} pages")

# Check if target page URL is in any scraped page
found_url = False
for page in pages:
    if target_page in page['url']:
        found_url = True
        print(f"\n✓ FOUND TARGET PAGE: {page['url']}")
        print(f"  Content length: {len(page.get('text', ''))} chars")
        break

if not found_url:
    # Check if it's in any links
    all_links = []
    for page in pages:
        all_links.extend(page.get('links', []))
    
    found_in_links = [link for link in all_links if target_page in link]
    if found_in_links:
        print(f"\n✓ Found in links (but not scraped yet): {found_in_links[0]}")
    else:
        print(f"\n✗ Not found in first {len(pages)} pages or their links")

# Search for term in scraped pages
searcher = TermSearcher()
results = searcher.find_term_occurrences("restorative justice", pages)
count = results.get('count', 0)
pages_with_term = results.get('pages', [])

print(f"\n{'='*80}")
print(f"SEARCH RESULTS:")
print(f"  'restorative justice' found: {count} times")
print(f"  Pages with term: {len(pages_with_term)}")
if pages_with_term:
    for page_url in pages_with_term:
        print(f"    - {page_url}")

