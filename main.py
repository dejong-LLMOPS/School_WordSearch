"""Main orchestrator for the school policy web scraper."""
import argparse
import json
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from data_extractor import get_schools_for_state
from web_scraper import scrape_school_urls, _global_url_cache
from term_searcher import search_school_content
from ai_context import get_ai_contextualization
from csv_generator import update_csv_with_school, get_processed_schools, load_existing_results
from config import (
    DEFAULT_STATE, SEARCH_TERMS, RESULTS_CSV, PROGRESS_FILE,
    SCRAPING_CONFIG
)
from utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    """Orchestrates the entire scraping and analysis pipeline."""
    
    def __init__(self, state: str = DEFAULT_STATE, search_terms: Optional[List[str]] = None,
                 resume: bool = True, max_schools: Optional[int] = None, 
                 workers: int = None, delay: Optional[float] = None,
                 min_delay: Optional[float] = None, max_delay: Optional[float] = None):
        # Cache for search results by URL (shared across schools)
        self._search_results_cache: Dict[str, Dict] = {}
        # Set of already processed schools (loaded from CSV)
        self._processed_schools: set = set()
        # Set of already processed schools (loaded from CSV)
        self._processed_schools: set = set()
        """
        Initialize the orchestrator.
        
        Args:
            state: State code to process
            search_terms: List of terms to search for
            resume: Whether to resume from previous progress
            max_schools: Maximum number of schools to process (None for all)
            workers: Number of worker threads for parallel processing
            delay: Override delay between requests (in seconds)
            min_delay: Minimum delay for adaptive rate limiting (in seconds)
            max_delay: Maximum delay for adaptive rate limiting (in seconds)
        """
        self.state = state
        self.search_terms = search_terms or SEARCH_TERMS
        self.resume = resume
        self.max_schools = max_schools
        self.workers = workers or SCRAPING_CONFIG.get('workers', 5)
        self.delay = delay
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.processed_count = 0
        self.error_count = 0
        self.progress_file = PROGRESS_FILE
        self.csv_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.start_time = None
        
    def load_progress(self) -> Dict:
        """Load progress from checkpoint file."""
        if not self.resume or not self.progress_file.exists():
            return {}
        
        try:
            with open(self.progress_file, 'r') as f:
                progress = json.load(f)
            logger.info(f"Loaded progress: {progress.get('processed', 0)} schools processed")
            return progress
        except Exception as e:
            logger.warning(f"Error loading progress: {e}")
            return {}
    
    def save_progress(self, progress: Dict) -> None:
        """Save progress to checkpoint file (thread-safe - caller must hold lock)."""
        try:
            # Ensure output directory exists
            self.progress_file.parent.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Attempting to save progress to {self.progress_file}")
            with open(self.progress_file, 'w') as f:
                json.dump(progress, f, indent=2)
            logger.info(f"✓ Saved progress to {self.progress_file}: {progress.get('processed', 0)} schools")
        except Exception as e:
            logger.error(f"✗ Error saving progress to {self.progress_file}: {e}", exc_info=True)
            raise  # Re-raise so caller knows it failed
    
    def _get_school_id(self, school_data: Dict) -> str:
        """Generate a unique identifier for a school."""
        # Use NCESSCH (National Center for Education Statistics School ID) if available
        ncessch = school_data.get('NCESSCH')
        if ncessch and str(ncessch) != 'nan':
            return str(ncessch)
        # Fallback to name + state
        return f"{school_data.get('SCH_NAME', 'Unknown')}_{school_data.get('ST', '')}"
    
    def process_school(self, school_data: Dict) -> Dict:
        """
        Process a single school through the entire pipeline.
        Uses URL-level caching to share results across schools.
        
        Args:
            school_data: Dictionary with school information
        
        Returns:
            Dictionary with processing results
        """
        school_name = school_data.get('SCH_NAME', 'Unknown')
        school_url = school_data.get('SCHOOL_URL')
        district_url = school_data.get('DISTRICT_URL')
        school_id = self._get_school_id(school_data)
        
        # Check if school is already in results CSV
        school_identifier = (school_name, self.state)
        if school_identifier in self._processed_schools:
            logger.info(f"⏭ Skipping {school_name} - already in results CSV")
            return None  # Signal to skip this school
        
        thread_id = threading.current_thread().ident
        thread_name = threading.current_thread().name
        logger.info(f"Processing: {school_name} (ID: {school_id}) [Thread: {thread_name}]")
        
        result = {
            'school_data': school_data,
            'search_results': {},
            'ai_summaries': {},
            'scrape_status': 'pending'
        }
        
        # Check if we have URLs to scrape
        if not school_url and not district_url:
            logger.warning(f"No URLs available for {school_name}")
            result['scrape_status'] = 'no_url'
            return result
        
        try:
            # Step 1: Scrape websites (with URL-level caching)
            logger.debug(f"Scraping URLs for {school_name}")
            pages = scrape_school_urls(school_url, district_url, use_cache=True, school_id=school_id)
            
            if not pages:
                logger.warning(f"✗ No pages scraped for {school_name}")
                result['scrape_status'] = 'scrape_failed'
                # Still create empty search_results so school gets saved to CSV
                result['search_results'] = {
                    'terms_found': [],
                    'page_urls': [],
                    'context_snippets': [],
                    'total_occurrences': 0,
                    'pages_with_terms': 0,
                    'school_terms_found': [],
                    'school_page_urls': [],
                    'school_total_occurrences': 0,
                    'school_pages_with_terms': 0,
                    'district_terms_found': [],
                    'district_page_urls': [],
                    'district_total_occurrences': 0,
                    'district_pages_with_terms': 0
                }
                result['ai_summaries'] = {}
                return result
            
            result['scrape_status'] = 'success'
            # Count pages with actual content
            pages_with_content = [p for p in pages if p.get('content_length', 0) > 0]
            logger.info(f"✓ Successfully scraped {len(pages)} pages for {school_name} ({len(pages_with_content)} with content)")
            
            # Step 2: Search for terms (with result sharing across schools using same URLs)
            logger.debug(f"Searching for terms in {school_name}")
            
            # Check if we can reuse search results from another school using the same URLs
            search_results = None
            
            # Try full URL combination first (school + district)
            urls_key = f"{school_url or ''}|{district_url or ''}"
            
            # Also try district-only key (for schools sharing same district but different school URLs)
            district_key = f"|{district_url or ''}" if district_url else None
            
            if urls_key in self._search_results_cache:
                logger.debug(f"Reusing search results from cache for URLs: {urls_key}")
                search_results = self._search_results_cache[urls_key]
            elif district_key and district_key in self._search_results_cache:
                # Reuse district-only results if available (for schools sharing same district)
                logger.debug(f"Reusing district search results from cache: {district_key}")
                district_results = self._search_results_cache[district_key]
                # Filter to only district pages (since school pages might be different)
                search_results = {
                    'terms_found': district_results.get('district_terms_found', []),
                    'page_urls': district_results.get('district_page_urls', []),
                    'context_snippets': [s for s in district_results.get('context_snippets', []) 
                                        if s.get('source') == 'district'],
                    'total_occurrences': district_results.get('district_total_occurrences', 0),
                    'pages_with_terms': district_results.get('district_pages_with_terms', 0),
                    'school_terms_found': [],
                    'school_page_urls': [],
                    'school_total_occurrences': 0,
                    'school_pages_with_terms': 0,
                    'district_terms_found': district_results.get('district_terms_found', []),
                    'district_page_urls': district_results.get('district_page_urls', []),
                    'district_total_occurrences': district_results.get('district_total_occurrences', 0),
                    'district_pages_with_terms': district_results.get('district_pages_with_terms', 0)
                }
                # Still need to search school pages if they exist
                school_pages = [p for p in pages if p.get('source') == 'school']
                if school_pages:
                    school_results = search_school_content(school_pages, search_terms=self.search_terms)
                    # Merge school results with district results
                    search_results['school_terms_found'] = school_results.get('terms_found', [])
                    search_results['school_page_urls'] = school_results.get('page_urls', [])
                    search_results['school_total_occurrences'] = school_results.get('total_occurrences', 0)
                    search_results['school_pages_with_terms'] = school_results.get('pages_with_terms', 0)
                    # Update combined totals
                    search_results['terms_found'] = list(set(search_results['terms_found'] + school_results.get('terms_found', [])))
                    search_results['page_urls'] = search_results['page_urls'] + school_results.get('page_urls', [])
                    search_results['total_occurrences'] = search_results['total_occurrences'] + school_results.get('total_occurrences', 0)
                    search_results['pages_with_terms'] = search_results['pages_with_terms'] + school_results.get('pages_with_terms', 0)
                    search_results['context_snippets'].extend(school_results.get('context_snippets', []))
            else:
                # Perform search on all pages
                search_results = search_school_content(pages, search_terms=self.search_terms)
                # Cache the results with full key
                self._search_results_cache[urls_key] = search_results
                # Also cache district-only results if district URL exists
                if district_key and district_url:
                    district_pages = [p for p in pages if p.get('source') == 'district']
                    if district_pages:
                        district_only_results = search_school_content(district_pages, search_terms=self.search_terms)
                        # Store district results separately for reuse
                        self._search_results_cache[district_key] = {
                            'district_terms_found': district_only_results.get('terms_found', []),
                            'district_page_urls': district_only_results.get('page_urls', []),
                            'district_total_occurrences': district_only_results.get('total_occurrences', 0),
                            'district_pages_with_terms': district_only_results.get('pages_with_terms', 0),
                            'context_snippets': district_only_results.get('context_snippets', [])
                        }
            
            result['search_results'] = search_results
            
            if not search_results.get('terms_found'):
                logger.info(f"No terms found for {school_name}")
                # Still continue to save to CSV even with no hits
                result['ai_summaries'] = {}
            else:
                logger.info(f"Found terms for {school_name}: {search_results.get('terms_found')}")
                
                # Step 3: Get AI contextualization
                logger.debug(f"Getting AI contextualization for {school_name}")
                
                # Create page content map for AI
                page_content_map = {page['url']: page.get('text', '') for page in pages}
                
                # Get district name for context
                district_name = school_data.get('DISTRICT_NAME') or school_data.get('LEA_NAME', '')
                
                ai_summaries = get_ai_contextualization(
                    search_results, 
                    page_content_map,
                    school_name=school_name,
                    district_name=district_name
                )
                result['ai_summaries'] = ai_summaries
                
                if ai_summaries:
                    logger.info(f"Got AI summary for {school_name}")
                else:
                    logger.warning(f"No AI summary for {school_name} (API may be unavailable)")
            
        except Exception as e:
            logger.error(f"Error processing {school_name}: {e}", exc_info=True)
            result['scrape_status'] = 'error'
            result['error_message'] = str(e)
        
        return result
    
    def run(self) -> None:
        """Run the complete scraping and analysis pipeline."""
        logger.info(f"Starting pipeline for state: {self.state}")
        logger.info(f"Search terms: {', '.join(self.search_terms)}")
        
        # Load progress
        progress = self.load_progress()
        # Convert lists back to tuples (JSON stores tuples as lists)
        processed_schools_list = progress.get('processed_schools', [])
        processed_schools = set(tuple(item) if isinstance(item, list) else item 
                                for item in processed_schools_list)
        
        # Load already processed schools from CSV to avoid duplicates
        logger.info("Loading already processed schools from CSV...")
        csv_processed = get_processed_schools()
        if csv_processed:
            logger.info(f"Found {len(csv_processed)} schools already in CSV")
            # Merge with progress.json schools
            processed_schools.update(csv_processed)
        
        # Store in instance variable for process_school to use
        self._processed_schools = processed_schools
        
        # Get schools data
        logger.info("Loading schools data...")
        schools_df = get_schools_for_state(self.state)
        
        if schools_df.empty:
            logger.error(f"No schools found for state: {self.state}")
            return
        
        logger.info(f"Found {len(schools_df)} schools in {self.state}")
        
        # Filter to unprocessed schools (always check CSV, not just when resuming)
        if processed_schools:
            # Create identifier for schools
            schools_df['identifier'] = schools_df.apply(
                lambda row: (row.get('SCH_NAME', ''), row.get('ST', '')),
                axis=1
            )
            unprocessed = schools_df[~schools_df['identifier'].isin(processed_schools)]
            skipped_count = len(schools_df) - len(unprocessed)
            if skipped_count > 0:
                logger.info(f"⏭ Skipping {skipped_count} schools already in CSV (found {len(processed_schools)} total processed)")
            schools_df = unprocessed.drop(columns=['identifier'])
        
        # Limit to max_schools if specified
        if self.max_schools:
            schools_df = schools_df.head(self.max_schools)
            logger.info(f"Limiting to {self.max_schools} schools")
        
        total_schools = len(schools_df)
        logger.info(f"Processing {total_schools} schools with {self.workers} worker threads...")
        if self.workers > 1:
            logger.info(f"✓ Parallel processing ENABLED - {self.workers} schools will be processed simultaneously")
        else:
            logger.warning(f"⚠ Parallel processing DISABLED - running sequentially (workers=1)")
        
        # Update delay settings in config if provided
        if self.delay is not None:
            SCRAPING_CONFIG['delay'] = self.delay
            logger.info(f"Using custom delay: {self.delay}s")
        if hasattr(self, 'min_delay') and self.min_delay is not None:
            SCRAPING_CONFIG['min_delay'] = self.min_delay
            logger.info(f"Using custom min_delay: {self.min_delay}s")
        if hasattr(self, 'max_delay') and self.max_delay is not None:
            SCRAPING_CONFIG['max_delay'] = self.max_delay
            logger.info(f"Using custom max_delay: {self.max_delay}s")
        
        self.start_time = time.time()
        
        # Convert DataFrame to list of dicts for parallel processing
        schools_list = [row.to_dict() for _, row in schools_df.iterrows()]
        
        # Process schools in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Submit all tasks
            future_to_school = {
                executor.submit(self.process_school, school_data): school_data
                for school_data in schools_list
            }
            
            # Process completed tasks as they finish
            completed_count = 0
            for future in as_completed(future_to_school):
                school_data = future_to_school[future]
                school_name = school_data.get('SCH_NAME', 'Unknown')
                completed_count += 1
                
                try:
                    result = future.result()
                    
                    # Skip if school was already processed (result is None)
                    if result is None:
                        logger.debug(f"Skipped {school_name} - already in CSV")
                        continue
                    
                    # Validate result structure
                    if not isinstance(result, dict):
                        logger.error(f"Invalid result type for {school_name}: {type(result)}")
                        continue
                    
                    if 'school_data' not in result:
                        logger.error(f"Missing 'school_data' in result for {school_name}")
                        continue
                    
                    if 'search_results' not in result:
                        logger.error(f"Missing 'search_results' in result for {school_name}")
                        continue
                    
                    if 'ai_summaries' not in result:
                        logger.warning(f"Missing 'ai_summaries' in result for {school_name}, using empty dict")
                        result['ai_summaries'] = {}
                    
                    if 'scrape_status' not in result:
                        logger.warning(f"Missing 'scrape_status' in result for {school_name}, defaulting to 'unknown'")
                        result['scrape_status'] = 'unknown'
                    
                    # Save to CSV (thread-safe)
                    csv_saved = False
                    with self.csv_lock:
                        try:
                            logger.debug(f"Attempting to save {school_name} to CSV...")
                            update_csv_with_school(
                                result['school_data'],
                                result['search_results'],
                                result['ai_summaries'],
                                result.get('scrape_status', 'unknown')
                            )
                            csv_saved = True
                            logger.info(f"✓ Saved {school_name} to CSV (status: {result.get('scrape_status', 'unknown')})")
                        except Exception as e:
                            logger.error(f"✗ CRITICAL: Failed to save {school_name} to CSV: {e}", exc_info=True)
                            # Continue anyway - we'll try to save progress
                    
                    if not csv_saved:
                        logger.warning(f"⚠ {school_name} was NOT saved to CSV - check errors above")
                    
                    # Update progress (thread-safe)
                    with self.progress_lock:
                        processed_schools.add((school_name, self.state))
                        progress = {
                            'state': self.state,
                            'processed': len(processed_schools),
                            'processed_schools': list(processed_schools),
                            'last_updated': datetime.now().isoformat()
                        }
                        try:
                            self.save_progress(progress)
                            logger.info(f"Progress saved: {len(processed_schools)} schools processed")
                        except Exception as e:
                            logger.error(f"Error saving progress: {e}", exc_info=True)
                        
                        # Update counters
                        if result['scrape_status'] == 'success':
                            self.processed_count += 1
                        else:
                            self.error_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing school {school_name}: {e}", exc_info=True)
                    # Still try to save error result to CSV
                    try:
                        error_result = {
                            'school_data': school_data,
                            'search_results': {
                                'terms_found': [],
                                'page_urls': [],
                                'context_snippets': [],
                                'total_occurrences': 0,
                                'pages_with_terms': 0,
                                'school_terms_found': [],
                                'school_page_urls': [],
                                'school_total_occurrences': 0,
                                'school_pages_with_terms': 0,
                                'district_terms_found': [],
                                'district_page_urls': [],
                                'district_total_occurrences': 0,
                                'district_pages_with_terms': 0
                            },
                            'ai_summaries': {},
                            'scrape_status': 'error',
                            'error_message': str(e)
                        }
                        with self.csv_lock:
                            update_csv_with_school(
                                error_result['school_data'],
                                error_result['search_results'],
                                error_result['ai_summaries'],
                                error_result['scrape_status']
                            )
                            logger.info(f"✓ Saved {school_name} (error) to CSV")
                    except Exception as save_error:
                        logger.error(f"Error saving error result for {school_name}: {save_error}")
                    with self.progress_lock:
                        self.error_count += 1
                        processed_schools.add((school_name, self.state))
                        progress = {
                            'state': self.state,
                            'processed': len(processed_schools),
                            'processed_schools': list(processed_schools),
                            'last_updated': datetime.now().isoformat()
                        }
                        self.save_progress(progress)
                
                # Log progress periodically
                if completed_count % 10 == 0 or completed_count == total_schools:
                    elapsed = time.time() - self.start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    with self.progress_lock:
                        logger.info(f"Progress: {completed_count}/{total_schools} schools processed "
                                  f"({self.processed_count} success, {self.error_count} errors) "
                                  f"[{rate:.2f} schools/sec]")
        
        # Final summary with performance metrics
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        
        # Final progress save
        with self.progress_lock:
            final_progress = {
                'state': self.state,
                'processed': len(processed_schools),
                'processed_schools': list(processed_schools),
                'last_updated': datetime.now().isoformat(),
                'completed': True
            }
            try:
                self.save_progress(final_progress)
                logger.info(f"✓ Final progress saved: {len(processed_schools)} schools")
            except Exception as e:
                logger.error(f"✗ Failed to save final progress: {e}", exc_info=True)
        
        # Check CSV file
        csv_count = 0
        if RESULTS_CSV.exists():
            try:
                import pandas as pd
                df = pd.read_csv(RESULTS_CSV)
                csv_count = len(df)
            except:
                pass
        
        logger.info("=" * 60)
        logger.info("Pipeline completed!")
        logger.info(f"Total schools processed: {total_schools}")
        logger.info(f"Successful scrapes: {self.processed_count}")
        logger.info(f"Errors: {self.error_count}")
        logger.info(f"Schools in CSV: {csv_count}")
        logger.info(f"Schools in progress.json: {len(processed_schools)}")
        logger.info(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        if elapsed_time > 0:
            logger.info(f"Average time per school: {elapsed_time/total_schools:.2f} seconds")
            logger.info(f"Throughput: {total_schools/elapsed_time:.2f} schools/second ({total_schools*60/elapsed_time:.2f} schools/minute)")
        logger.info(f"Results CSV: {RESULTS_CSV}")
        logger.info(f"Progress file: {self.progress_file}")
        if csv_count != len(processed_schools):
            logger.warning(f"⚠ WARNING: CSV has {csv_count} rows but {len(processed_schools)} schools were processed!")
        logger.info("=" * 60)


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='Scrape school websites and search for policy-related terms'
    )
    parser.add_argument(
        '--state',
        type=str,
        default=DEFAULT_STATE,
        help=f'State code to process (default: {DEFAULT_STATE})'
    )
    parser.add_argument(
        '--terms',
        type=str,
        nargs='+',
        default=SEARCH_TERMS,
        help='Search terms (default: restorative justice race equity)'
    )
    parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Do not resume from previous progress'
    )
    parser.add_argument(
        '--max-schools',
        type=int,
        default=None,
        help='Maximum number of schools to process (for testing)'
    )
    parser.add_argument(
        '--max',
        type=int,
        default=None,
        dest='max_schools',
        help='Alias for --max-schools (maximum number of schools to process)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    parser.add_argument(
        '--log-file',
        type=str,
        default=None,
        help='Optional log file path'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help=f'Number of worker threads for parallel processing (default: {SCRAPING_CONFIG.get("workers", 5)})'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=None,
        help=f'Delay between page requests in seconds (default: {SCRAPING_CONFIG.get("delay", 0.5)})'
    )
    parser.add_argument(
        '--min-delay',
        type=float,
        default=None,
        help=f'Minimum delay for adaptive rate limiting in seconds (default: {SCRAPING_CONFIG.get("min_delay", 0.3)})'
    )
    parser.add_argument(
        '--max-delay',
        type=float,
        default=None,
        help=f'Maximum delay for adaptive rate limiting in seconds (default: {SCRAPING_CONFIG.get("max_delay", 2.0)})'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level=log_level, log_file=args.log_file)
    
    # Create and run orchestrator
    orchestrator = ScraperOrchestrator(
        state=args.state,
        search_terms=args.terms,
        resume=not args.no_resume,
        max_schools=args.max_schools,
        workers=args.workers,
        delay=args.delay,
        min_delay=getattr(args, 'min_delay', None),
        max_delay=getattr(args, 'max_delay', None)
    )
    
    try:
        orchestrator.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
