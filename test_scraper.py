#!/usr/bin/env python3
"""Test script to verify scraper can find restorative justice content."""
import sys
from web_scraper import WebScraper
from term_searcher import TermSearcher

# Test URL
test_url = "https://www.ccs.k12.nc.us"
target_page = "https://www.ccs.k12.nc.us/page/restorative-justice-centers-rjc-in-secondary-schools"

print(f"Testing scraper on: {test_url}")
print(f"Looking for page: {target_page}")
print("-" * 80)

scraper = WebScraper()
pages = scraper.scrape_site(test_url, max_depth=3, max_pages=20)

print(f"\nScraped {len(pages)} pages")
print(f"URLs found:")
for i, page in enumerate(pages[:10], 1):
    print(f"  {i}. {page['url']}")
    print(f"     Links: {len(page.get('links', []))}")
    print(f"     Content length: {len(page.get('text', ''))}")

# Check if target page was found
found_target = any(page['url'] == target_page for page in pages)
print(f"\n✓ Target page found: {found_target}")

# Search for restorative justice
if pages:
    searcher = TermSearcher()
    results = searcher.find_term_occurrences("restorative justice", pages)
    print(f"\n✓ Found 'restorative justice' {results.get('count', 0)} times")
    if results.get('count', 0) > 0:
        print(f"  Pages with term: {len(results.get('pages', []))}")
        for page_url in results.get('pages', [])[:5]:
            print(f"    - {page_url}")

