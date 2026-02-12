"""Script to update AI summaries by rescraping URLs and regenerating summaries with improved prompt."""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from config import OUTPUT_DIR, PERPLEXITY_CONFIG
from web_scraper import scrape_school_urls
from term_searcher import TermSearcher, search_school_content
from ai_context import PerplexityClient, get_ai_contextualization

logger = logging.getLogger(__name__)

# Configuration for parallel processing
DEFAULT_NUM_WORKERS = 5  # Default number of parallel workers (adjust based on API rate limits)

# Configuration
INPUT_CSV = OUTPUT_DIR / "florida_with_ai_summary.csv"
OUTPUT_CSV = OUTPUT_DIR / "florida_with_ai_summary_updated.csv"
BACKUP_DIR = OUTPUT_DIR / "backups"


def create_improved_prompt(school_name: str, district_name: str, search_results: Dict,
                          page_content_map: Optional[Dict[str, str]] = None,
                          is_district_level: bool = False) -> str:
    """
    Create improved prompt for consistent narrative summaries.
    
    Args:
        school_name: Name of the school
        district_name: Name of the district
        search_results: Dictionary with search results
        page_content_map: Optional dictionary mapping URLs to page content
        is_district_level: True if district-level summary
    
    Returns:
        Prompt string for AI
    """
    # Get all page URLs
    page_urls = list(set(search_results.get('page_urls', [])))
    
    # Aggregate all page content
    all_content_parts = []
    terms_found = search_results.get('terms_found', [])
    context_snippets = search_results.get('context_snippets', [])
    
    # Build content with term highlights
    for url in page_urls:
        page_content = page_content_map.get(url, '') if page_content_map else ''
        if page_content:
            # Find terms in this page
            page_terms = [s['term'] for s in context_snippets if s.get('url') == url]
            if page_terms:
                all_content_parts.append(f"Page: {url}\nTerms found: {', '.join(set(page_terms))}\nContent: {page_content[:3000]}\n")
    
    # If we have context snippets but no full page content, use snippets
    if not all_content_parts and context_snippets:
        for snippet_data in context_snippets[:20]:  # Limit to first 20 snippets
            term = snippet_data.get('term', '')
            context = snippet_data.get('context', '')
            url = snippet_data.get('url', '')
            all_content_parts.append(f"[{term} @ {url}]: {context}\n")
    
    aggregated_content = '\n\n'.join(all_content_parts)
    
    # Limit total content to avoid token limits
    if len(aggregated_content) > 15000:
        aggregated_content = aggregated_content[:15000] + "\n\n[Content truncated...]"
    
    # Analyze source of hits (school vs district)
    school_hits = search_results.get('school_pages_with_terms', 0)
    district_hits = search_results.get('district_pages_with_terms', 0)
    has_school_hits = school_hits > 0
    has_district_hits = district_hits > 0
    
    # Build source context note
    source_context = []
    if has_school_hits and has_district_hits:
        source_context.append(f"Terms were found on both the school website ({school_hits} pages) and district website ({district_hits} pages).")
    elif has_school_hits:
        source_context.append(f"Terms were found ONLY on the school website ({school_hits} pages), not on the district website.")
    elif has_district_hits:
        source_context.append(f"Terms were found ONLY on the district website ({district_hits} pages), not on the school website.")
    
    source_context_str = ' '.join(source_context) if source_context else "No terms found on either website."
    
    # Build the prompt with district-level context note
    if is_district_level:
        context_note = f"Note: This is a district/county-level summary for {district_name}. The content reflects district-wide policies and programs, not individual school implementations."
        entity_name = district_name
    else:
        context_note = f"School/District: {school_name} / {district_name}"
        entity_name = f"{school_name} / {district_name}"
    
    # Get terms found for the prompt
    terms_found = search_results.get('terms_found', [])
    terms_str = ', '.join(terms_found) if terms_found else 'None found'
    
    # Get URLs where terms were found
    page_urls_list = search_results.get('page_urls', [])
    urls_str = '; '.join(page_urls_list) if page_urls_list else 'None'
    
    # Build the improved prompt
    user_content = f"""Analyze all content from {entity_name} and provide a comprehensive, consistent narrative summary of their approach to Restorative Justice and related terms.

{context_note}

Source of findings: {source_context_str}

Key terms found: {terms_str}
URLs where terms were found: {urls_str}

All scraped pages and term occurrences:
{aggregated_content}

Based on all the content from this {'district/county' if is_district_level else 'school/district'}'s website, provide a cohesive narrative summary that addresses the following:

**1. Restorative Justice Presence**: Begin with a clear statement about whether Restorative Justice (or related terms like restorative practices, restorative approaches) is part of this school/district's approach. If it is not clearly part of their approach, state that explicitly. If the content does not provide enough information to determine this, state that clearly.

**2. Contextual Understanding**: Explain where and how these terms appear on the web pages. What specific pages mention them? In what context are they used? Are they part of official policies, program descriptions, staff listings, mission statements, or passing mentions? Provide specific examples from the content.

**3. Philosophy Assessment**: Based on the content, does Restorative Justice appear to be part of the school/district's philosophy? Is it integrated into their educational approach, or is it mentioned peripherally? How do they conceptualize these terms? What do they mean to the school/district?

**4. Infrastructure**: Identify any infrastructure related to these terms:
- Dedicated staff positions (coordinators, facilitators, specialists, etc.) - include specific job titles if mentioned
- Programs or centers (Restorative Justice Centers, Character Academies, alternative programs, etc.) - include specific names
- Training programs or initiatives
- Organizational structures or departments
- If no infrastructure is evident, state that clearly

**Writing Style**: Write in a consistent, formal narrative style. Structure the summary as a cohesive analysis in paragraph form, not bullet points. Use specific examples, program names, and evidence from the content. Connect philosophy to implementation when possible.

**Important**: 
- If the content does not provide enough information to derive meaningful intelligence about their approach, state that clearly. It is acceptable to say "Based on the available content, it is not possible to determine whether Restorative Justice is part of this school/district's approach" if that is the case.
- At the end of your response, explicitly state whether the information comes from the school website only, the district website only, or both. Use this format: "Source: [School website only / District website only / Both school and district websites]"
"""
    
    return user_content


def load_florida_data(csv_path: Path) -> pd.DataFrame:
    """Load schools from Florida CSV with AI summaries."""
    if not csv_path.exists():
        logger.error(f"Input CSV not found: {csv_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} schools from {csv_path}")
        
        # Filter for schools with AI summaries
        df = df[df['AI Summary'].notna() & (df['AI Summary'] != '')].copy()
        logger.info(f"Found {len(df)} schools with AI summaries")
        
        return df
    except Exception as e:
        logger.error(f"Error loading CSV: {e}", exc_info=True)
        return pd.DataFrame()


def parse_url_list(url_str: str) -> List[str]:
    """Parse comma-separated URL string into list of URLs."""
    if not url_str or pd.isna(url_str):
        return []
    if isinstance(url_str, str):
        urls = [u.strip() for u in url_str.split(',') if u.strip()]
        return urls
    return []


def rescrape_urls_for_school(school_data: Dict) -> Dict[str, str]:
    """
    Rescrape URLs for a school where terms were found.
    
    Args:
        school_data: Dictionary with school data from CSV
    
    Returns:
        Dictionary mapping URLs to page content
    """
    page_content_map = {}
    
    # Get URLs where terms were found
    school_urls_str = school_data.get('School Page URLs', '')
    district_urls_str = school_data.get('District Page URLs', '')
    
    school_urls = parse_url_list(school_urls_str)
    district_urls = parse_url_list(district_urls_str)
    
    all_urls = list(set(school_urls + district_urls))
    
    if not all_urls:
        logger.warning(f"No URLs to scrape for {school_data.get('School Name', 'Unknown')}")
        return page_content_map
    
    logger.info(f"Rescraping {len(all_urls)} URLs for {school_data.get('School Name', 'Unknown')}")
    
    from web_scraper import WebScraper
    scraper = WebScraper()
    
    for url in all_urls:
        try:
            page = scraper.scrape_page(url, use_cloudscraper_first=True, add_to_scraped_pages=False)
            if page and page.get('text'):
                page_content_map[url] = page['text']
                logger.debug(f"Scraped {url}: {len(page['text'])} chars")
            else:
                logger.warning(f"Failed to scrape or empty content: {url}")
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            continue
        
        # Small delay to avoid overwhelming servers
        time.sleep(0.5)
    
    logger.info(f"Successfully scraped {len(page_content_map)}/{len(all_urls)} URLs")
    return page_content_map


def search_terms_on_pages(page_content_map: Dict[str, str], school_data: Dict) -> Dict:
    """
    Search for terms on rescraped pages.
    
    Args:
        page_content_map: Dictionary mapping URLs to page content
        school_data: School data dictionary
    
    Returns:
        Search results dictionary
    """
    if not page_content_map:
        logger.warning("No page content to search")
        return {}
    
    # Convert page content map to pages list format
    pages = []
    for url, content in page_content_map.items():
        # Determine source (school vs district)
        school_urls = parse_url_list(school_data.get('School Page URLs', ''))
        source = 'school' if url in school_urls else 'district'
        
        pages.append({
            'url': url,
            'text': content,
            'source': source
        })
    
    # Search for terms
    searcher = TermSearcher()
    search_results_list = searcher.search_pages(pages)
    
    # Aggregate results
    aggregated = searcher.aggregate_results(search_results_list)
    
    return aggregated


def generate_new_summary(school_data: Dict, search_results: Dict, 
                        page_content_map: Dict[str, str]) -> Optional[str]:
    """
    Generate new AI summary using improved prompt.
    
    Args:
        school_data: School data dictionary
        search_results: Search results dictionary
        page_content_map: Dictionary mapping URLs to page content
    
    Returns:
        New AI summary string or None
    """
    school_name = school_data.get('School Name', 'Unknown')
    district_name = school_data.get('District Name', 'Unknown')
    
    client = PerplexityClient()
    if not client.api_key:
        logger.warning("Perplexity API key not configured. Skipping AI summary.")
        return None
    
    # Create improved prompt
    prompt = create_improved_prompt(
        school_name,
        district_name,
        search_results,
        page_content_map,
        is_district_level=False
    )
    
    # Format as chat messages
    messages = [{'role': 'user', 'content': prompt}]
    
    logger.info(f"Generating new AI summary for {school_name}")
    
    # Use the client's internal method to make request
    # Note: This will use cache if same content, but since we're using a new prompt format,
    # the cache key will be different (based on school name, district, and URLs)
    # To force fresh summaries, we could add a timestamp or version to the cache key
    try:
        # Add a version marker to prompt to ensure fresh summaries
        # This makes the cache key different from old summaries
        versioned_prompt = prompt + "\n\n[Updated Summary Request - Version 2.0]"
        versioned_messages = [{'role': 'user', 'content': versioned_prompt}]
        
        result = client._make_request(versioned_messages, max_tokens=1000)
    except Exception as e:
        logger.error(f"Error making API request: {e}")
        return None
    
    if result:
        logger.info(f"Generated new summary for {school_name} ({len(result)} chars)")
    else:
        logger.warning(f"Failed to generate summary for {school_name}")
    
    return result


def create_backup(csv_path: Path) -> Path:
    """Create backup of CSV file."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = BACKUP_DIR / f"florida_with_ai_summary_backup_{timestamp}.csv"
    
    try:
        import shutil
        shutil.copy2(csv_path, backup_path)
        logger.info(f"Created backup: {backup_path}")
        return backup_path
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        return csv_path


def update_csv_with_summaries(input_csv: Path, updated_records: List[Dict]) -> bool:
    """
    Update CSV with new summaries.
    
    Args:
        input_csv: Path to input CSV
        updated_records: List of updated school records with new summaries
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create backup
        create_backup(input_csv)
        
        # Load original CSV
        df = pd.read_csv(input_csv)
        
        # Create mapping of school+district to new summary
        summary_map = {}
        for record in updated_records:
            key = (record.get('School Name'), record.get('District Name'))
            summary_map[key] = record.get('AI Summary')
        
        # Update AI Summary column
        updated_count = 0
        for idx, row in df.iterrows():
            key = (row.get('School Name'), row.get('District Name'))
            if key in summary_map:
                df.at[idx, 'AI Summary'] = summary_map[key]
                updated_count += 1
        
        # Write updated CSV
        df.to_csv(OUTPUT_CSV, index=False)
        logger.info(f"Updated {updated_count} summaries in {OUTPUT_CSV}")
        
        return True
    except Exception as e:
        logger.error(f"Error updating CSV: {e}", exc_info=True)
        return False


# Thread-safe counter for progress tracking
_progress_lock = Lock()
_processed_count = 0
_updated_count = 0
_failed_count = 0


def process_school(school_data: Dict, idx: int, total: int) -> Optional[Dict]:
    """
    Process a single school: rescrape, search, generate summary.
    
    Args:
        school_data: School data dictionary
        idx: Current index (for progress tracking)
        total: Total number of schools
    
    Returns:
        Updated school record with new summary, or None if failed
    """
    global _processed_count, _updated_count, _failed_count
    
    school_name = school_data.get('School Name', 'Unknown')
    
    try:
        logger.info(f"[{idx+1}/{total}] Processing {school_name}")
        
        # Step 1: Rescrape URLs
        page_content_map = rescrape_urls_for_school(school_data)
        
        if not page_content_map:
            logger.warning(f"No content scraped for {school_name}, skipping")
            with _progress_lock:
                _processed_count += 1
                _failed_count += 1
            return None
        
        # Step 2: Search for terms
        search_results = search_terms_on_pages(page_content_map, school_data)
        
        if not search_results or not search_results.get('terms_found'):
            logger.warning(f"No terms found for {school_name}, skipping")
            with _progress_lock:
                _processed_count += 1
                _failed_count += 1
            return None
        
        # Step 3: Generate new summary
        new_summary = generate_new_summary(school_data, search_results, page_content_map)
        
        if not new_summary:
            logger.warning(f"Failed to generate summary for {school_name}")
            with _progress_lock:
                _processed_count += 1
                _failed_count += 1
            return None
        
        # Create updated record
        updated_record = school_data.copy()
        updated_record['AI Summary'] = new_summary
        updated_record['Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        logger.info(f"✓ Successfully updated summary for {school_name}")
        
        with _progress_lock:
            _processed_count += 1
            _updated_count += 1
            if _processed_count % 10 == 0:
                logger.info(f"Progress: {_processed_count}/{total} processed ({_updated_count} updated, {_failed_count} failed)")
        
        # Rate limiting - small delay between API calls
        time.sleep(2)
        
        return updated_record
        
    except Exception as e:
        logger.error(f"Error processing {school_name}: {e}", exc_info=True)
        with _progress_lock:
            _processed_count += 1
            _failed_count += 1
        return None


def main(num_workers: Optional[int] = None):
    """
    Main orchestration function.
    
    Args:
        num_workers: Number of parallel workers (defaults to DEFAULT_NUM_WORKERS)
    """
    global _processed_count, _updated_count, _failed_count
    
    from utils.logging_config import setup_logging
    import sys
    
    setup_logging()
    
    # Get number of workers from command line or use default
    if num_workers is None:
        num_workers = DEFAULT_NUM_WORKERS
        # Check for command line argument
        if len(sys.argv) > 1:
            try:
                num_workers = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Invalid number of workers: {sys.argv[1]}. Using default: {DEFAULT_NUM_WORKERS}")
    
    logger.info("=" * 80)
    logger.info("Starting AI Summary Update Process")
    logger.info(f"Using {num_workers} parallel workers")
    logger.info("=" * 80)
    
    # Load data
    df = load_florida_data(INPUT_CSV)
    
    if df.empty:
        logger.error("No schools to process")
        return
    
    total_schools = len(df)
    logger.info(f"Processing {total_schools} schools")
    
    # Reset counters
    _processed_count = 0
    _updated_count = 0
    _failed_count = 0
    
    # Prepare school data list
    school_data_list = []
    for idx, row in df.iterrows():
        school_data_list.append((row.to_dict(), idx, total_schools))
    
    # Process schools in parallel
    updated_records = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        future_to_school = {
            executor.submit(process_school, school_data, idx, total_schools): (school_data, idx)
            for school_data, idx, _ in school_data_list
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_school):
            school_data, idx = future_to_school[future]
            try:
                result = future.result()
                if result:
                    updated_records.append(result)
            except Exception as e:
                school_name = school_data.get('School Name', 'Unknown')
                logger.error(f"Exception processing {school_name}: {e}", exc_info=True)
                with _progress_lock:
                    _failed_count += 1
    
    # Update CSV
    if updated_records:
        logger.info(f"\nUpdating CSV with {len(updated_records)} new summaries...")
        success = update_csv_with_summaries(INPUT_CSV, updated_records)
        
        if success:
            logger.info("=" * 80)
            logger.info("Update Complete!")
            logger.info(f"  Updated: {len(updated_records)} schools")
            logger.info(f"  Failed: {_failed_count} schools")
            logger.info(f"  Output: {OUTPUT_CSV}")
            logger.info("=" * 80)
        else:
            logger.error("Failed to update CSV")
    else:
        logger.warning("No summaries were updated")


if __name__ == "__main__":
    main()

