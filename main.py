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

from data_extractor import get_districts_for_state, count_schools_per_district
from web_scraper import scrape_school_urls, _global_url_cache
from term_searcher import search_school_content
from ai_context import get_ai_contextualization
from csv_generator import update_csv_with_district, get_processed_districts, get_failed_districts, load_existing_results, deduplicate_results_csv
from config import (
    DEFAULT_STATE, SEARCH_TERMS, RESULTS_CSV, PROGRESS_FILE,
    SCRAPING_CONFIG
)
from utils.logging_config import setup_logging

logger = logging.getLogger(__name__)


class ScraperOrchestrator:
    """Orchestrates the entire scraping and analysis pipeline."""
    
    def __init__(self, state: str = DEFAULT_STATE, search_terms: Optional[List[str]] = None,
                 resume: bool = True, max_districts: Optional[int] = None, 
                 workers: int = None, delay: Optional[float] = None,
                 min_delay: Optional[float] = None, max_delay: Optional[float] = None,
                 rerun_failed: bool = False):
        """
        Initialize the orchestrator.
        
        Args:
            state: State code to process
            search_terms: List of terms to search for
            resume: Whether to resume from previous progress
            max_districts: Maximum number of districts to process (None for all)
            workers: Number of worker threads for parallel processing
            delay: Override delay between requests (in seconds)
            min_delay: Minimum delay for adaptive rate limiting (in seconds)
            max_delay: Maximum delay for adaptive rate limiting (in seconds)
            rerun_failed: If True, rerun districts with scrape_failed status
        """
        # Cache for search results by URL (shared across districts)
        self._search_results_cache: Dict[str, Dict] = {}
        # Set of already processed districts (loaded from CSV)
        self._processed_districts: set = set()
        self.state = state
        self.search_terms = search_terms or SEARCH_TERMS
        self.resume = resume
        self.max_districts = max_districts
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
        self.rerun_failed = rerun_failed
        
    def load_progress(self) -> Dict:
        """Load progress from checkpoint file."""
        if not self.resume or not self.progress_file.exists():
            return {}
        
        try:
            with open(self.progress_file, 'r') as f:
                progress = json.load(f)
            logger.info(f"Loaded progress: {progress.get('processed', 0)} districts processed")
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
            logger.info(f"✓ Saved progress to {self.progress_file}: {progress.get('processed', 0)} districts")
        except Exception as e:
            logger.error(f"✗ Error saving progress to {self.progress_file}: {e}", exc_info=True)
            raise  # Re-raise so caller knows it failed
    
    def _get_district_id(self, district_data: Dict) -> str:
        """Generate a unique identifier for a district."""
        # Use LEAID (Local Education Agency ID) if available
        leaid = district_data.get('LEAID')
        if leaid and str(leaid) != 'nan':
            return str(leaid)
        # Fallback to district URL or name + state
        district_url = district_data.get('DISTRICT_URL', '')
        if district_url:
            return district_url
        district_name = district_data.get('DISTRICT_NAME') or district_data.get('LEA_NAME', 'Unknown')
        return f"{district_name}_{district_data.get('ST', '')}"
    
    def process_district(self, district_data: Dict) -> Dict:
        """
        Process a single district through the entire pipeline.
        Uses URL-level caching to share results across districts.
        
        Args:
            district_data: Dictionary with district information
        
        Returns:
            Dictionary with processing results
        """
        district_name = district_data.get('DISTRICT_NAME') or district_data.get('LEA_NAME', 'Unknown')
        district_url = district_data.get('DISTRICT_URL')
        district_id = self._get_district_id(district_data)
        
        # Check if district is already in results CSV
        if district_url and district_url in self._processed_districts:
            logger.info(f"⏭ Skipping {district_name} - already in results CSV")
            return None  # Signal to skip this district
        
        thread_name = threading.current_thread().name
        logger.info(f"Processing: {district_name} (ID: {district_id}) [Thread: {thread_name}]")
        
        result = {
            'district_data': district_data,
            'search_results': {},
            'ai_summaries': {},
            'scrape_status': 'pending'
        }
        
        # Check if we have district URL to scrape
        if not district_url:
            logger.warning(f"No district URL available for {district_name}")
            result['scrape_status'] = 'no_url'
            # Create empty search_results so district gets saved to CSV
            result['search_results'] = {
                'terms_found': [],
                'page_urls': [],
                'context_snippets': [],
                'total_occurrences': 0,
                'pages_with_terms': 0,
                'district_terms_found': [],
                'district_page_urls': [],
                'district_total_occurrences': 0,
                'district_pages_with_terms': 0
            }
            result['ai_summaries'] = {}
            return result
        
        try:
            # Step 1: Scrape district website (with URL-level caching)
            logger.debug(f"Scraping district URL for {district_name}")
            pages = scrape_school_urls(None, district_url, use_cache=True, school_id=district_id)
            
            if not pages:
                logger.warning(f"✗ No pages scraped for {district_name}")
                result['scrape_status'] = 'scrape_failed'
                # Still create empty search_results so district gets saved to CSV
                result['search_results'] = {
                    'terms_found': [],
                    'page_urls': [],
                    'context_snippets': [],
                    'total_occurrences': 0,
                    'pages_with_terms': 0,
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
            logger.info(f"✓ Successfully scraped {len(pages)} pages for {district_name} ({len(pages_with_content)} with content)")
            
            # Step 2: Search for terms (with result sharing across districts using same URLs)
            logger.debug(f"Searching for terms in {district_name}")
            
            # Check if we can reuse search results from another district using the same URL
            search_results = None
            
            # Use district URL as key
            district_key = district_url if district_url else None
            
            if district_key and district_key in self._search_results_cache:
                # Reuse district results if available
                logger.debug(f"Reusing district search results from cache: {district_key}")
                district_results = self._search_results_cache[district_key]
                search_results = {
                    'terms_found': district_results.get('terms_found', []),
                    'page_urls': district_results.get('page_urls', []),
                    'context_snippets': district_results.get('context_snippets', []),
                    'total_occurrences': district_results.get('total_occurrences', 0),
                    'pages_with_terms': district_results.get('pages_with_terms', 0),
                    'district_terms_found': district_results.get('terms_found', []),
                    'district_page_urls': district_results.get('page_urls', []),
                    'district_total_occurrences': district_results.get('total_occurrences', 0),
                    'district_pages_with_terms': district_results.get('pages_with_terms', 0)
                }
            else:
                # Perform search on all pages
                search_results = search_school_content(pages, search_terms=self.search_terms)
                # Cache the results by district URL
                if district_key:
                    self._search_results_cache[district_key] = {
                        'terms_found': search_results.get('terms_found', []),
                        'page_urls': search_results.get('page_urls', []),
                        'total_occurrences': search_results.get('total_occurrences', 0),
                        'pages_with_terms': search_results.get('pages_with_terms', 0),
                        'context_snippets': search_results.get('context_snippets', [])
                    }
                    # Set district-specific fields
                    search_results['district_terms_found'] = search_results.get('terms_found', [])
                    search_results['district_page_urls'] = search_results.get('page_urls', [])
                    search_results['district_total_occurrences'] = search_results.get('total_occurrences', 0)
                    search_results['district_pages_with_terms'] = search_results.get('pages_with_terms', 0)
            
            result['search_results'] = search_results
            
            if not search_results.get('terms_found'):
                logger.info(f"No terms found for {district_name}")
                # Still continue to save to CSV even with no hits
                result['ai_summaries'] = {}
            else:
                logger.info(f"Found terms for {district_name}: {search_results.get('terms_found')}")
                
                # Step 3: Get AI contextualization
                logger.debug(f"Getting AI contextualization for {district_name}")
                
                # Create page content map for AI
                page_content_map = {page['url']: page.get('text', '') for page in pages}
                
                ai_summaries = get_ai_contextualization(
                    search_results, 
                    page_content_map,
                    school_name=None,
                    district_name=district_name
                )
                result['ai_summaries'] = ai_summaries
                
                if ai_summaries:
                    logger.info(f"Got AI summary for {district_name}")
                else:
                    logger.warning(f"No AI summary for {district_name} (API may be unavailable)")
            
        except Exception as e:
            logger.error(f"Error processing {district_name}: {e}", exc_info=True)
            result['scrape_status'] = 'error'
            result['error_message'] = str(e)
        
        return result
    
    def run(self) -> None:
        """Run the complete scraping and analysis pipeline."""
        logger.info(f"Starting pipeline for state: {self.state}")
        logger.info(f"Search terms: {', '.join(self.search_terms)}")
        
        # Load progress
        progress = self.load_progress()
        # Convert lists back to sets (JSON stores sets as lists)
        processed_districts_list = progress.get('processed_districts', [])
        processed_districts = set(processed_districts_list)
        
        # Load already processed districts from CSV to avoid duplicates
        logger.info("Loading already processed districts from CSV...")
        csv_processed_districts = get_processed_districts()
        if csv_processed_districts:
            logger.info(f"Found {len(csv_processed_districts)} districts already in CSV")
            processed_districts.update(csv_processed_districts)
        
        # If rerun_failed is True, exclude failed districts from processed list
        if self.rerun_failed:
            failed_districts = get_failed_districts()
            if failed_districts:
                logger.info(f"Rerunning {len(failed_districts)} districts with scrape_failed status")
                # Remove failed districts from processed list so they get rerun
                processed_districts = processed_districts - failed_districts
                logger.info(f"Excluding {len(failed_districts)} failed districts from skip list (now {len(processed_districts)} districts will be skipped)")
        
        self._processed_districts = processed_districts
        
        # Get districts data
        logger.info("Loading districts data...")
        districts_df = get_districts_for_state(self.state)
        
        if districts_df.empty:
            logger.error(f"No districts found for state: {self.state}")
            return
        
        logger.info(f"Found {len(districts_df)} districts in {self.state}")
        
        # Count schools per district
        logger.info("Counting schools per district...")
        school_counts = count_schools_per_district(self.state)
        
        # Add school count to each district's data
        if 'LEAID' in districts_df.columns:
            districts_df['SCHOOLS_IN_DISTRICT'] = districts_df['LEAID'].map(school_counts).fillna(0).astype(int)
        else:
            districts_df['SCHOOLS_IN_DISTRICT'] = 0
            logger.warning("LEAID column not found, cannot count schools per district")
        
        # Filter to unprocessed districts (always check CSV, not just when resuming)
        if processed_districts:
            # Filter by district URL (districts without URLs will still be processed)
            if 'DISTRICT_URL' in districts_df.columns:
                # Only filter districts that have URLs and are in processed_districts
                # Districts without URLs (NaN/None) will not be filtered out
                unprocessed = districts_df[
                    ~(districts_df['DISTRICT_URL'].notna() & districts_df['DISTRICT_URL'].isin(processed_districts))
                ]
                skipped_count = len(districts_df) - len(unprocessed)
                if skipped_count > 0:
                    logger.info(f"⏭ Skipping {skipped_count} districts already in CSV (found {len(processed_districts)} total processed)")
                districts_df = unprocessed
            else:
                logger.warning("DISTRICT_URL column not found, cannot filter processed districts")
        
        # Limit to max_districts if specified
        if self.max_districts:
            districts_df = districts_df.head(self.max_districts)
            logger.info(f"Limiting to {self.max_districts} districts")
        
        total_districts = len(districts_df)
        logger.info(f"Processing {total_districts} districts with {self.workers} worker threads...")
        if self.workers > 1:
            logger.info(f"✓ Parallel processing ENABLED - {self.workers} districts will be processed simultaneously")
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
        districts_list = [row.to_dict() for _, row in districts_df.iterrows()]
        
        # Process districts in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Submit all tasks
            future_to_district = {
                executor.submit(self.process_district, district_data): district_data
                for district_data in districts_list
            }
            
            # Process completed tasks as they finish
            completed_count = 0
            for future in as_completed(future_to_district):
                district_data = future_to_district[future]
                district_name = district_data.get('DISTRICT_NAME') or district_data.get('LEA_NAME', 'Unknown')
                completed_count += 1
                
                try:
                    result = future.result()
                    
                    # Skip if district was already processed (result is None)
                    if result is None:
                        logger.debug(f"Skipped {district_name} - already in CSV")
                        continue
                    
                    # Validate result structure
                    if not isinstance(result, dict):
                        logger.error(f"Invalid result type for {district_name}: {type(result)}")
                        continue
                    
                    if 'district_data' not in result:
                        logger.error(f"Missing 'district_data' in result for {district_name}")
                        continue
                    
                    if 'search_results' not in result:
                        logger.error(f"Missing 'search_results' in result for {district_name}")
                        continue
                    
                    if 'ai_summaries' not in result:
                        logger.warning(f"Missing 'ai_summaries' in result for {district_name}, using empty dict")
                        result['ai_summaries'] = {}
                    
                    if 'scrape_status' not in result:
                        logger.warning(f"Missing 'scrape_status' in result for {district_name}, defaulting to 'unknown'")
                        result['scrape_status'] = 'unknown'
                    
                    # Save district to CSV (thread-safe) - save all districts including those with no_url or scrape_failed
                    district_url = district_data.get('DISTRICT_URL')
                    csv_saved = False
                    
                    # Use district URL as identifier if available, otherwise use district name + state
                    district_identifier = district_url if district_url else f"{district_name}_{self.state}"
                    
                    with self.csv_lock:
                        # Check if district already saved
                        if district_identifier in self._processed_districts:
                            logger.debug(f"District {district_identifier} already in CSV, skipping")
                            csv_saved = True
                        else:
                            try:
                                # Save district with results (including scrape_failed and no_url districts)
                                logger.debug(f"Attempting to save district {district_name} to CSV...")
                                update_csv_with_district(
                                    result['district_data'],
                                    result['search_results'],
                                    result['ai_summaries'],
                                    result.get('scrape_status', 'unknown'),
                                    school_names=None  # No longer tracking school names
                                )
                                self._processed_districts.add(district_identifier)
                                csv_saved = True
                                status = result.get('scrape_status', 'unknown')
                                logger.info(f"✓ Saved district {district_name} to CSV (status: {status})")
                            except Exception as e:
                                logger.error(f"✗ CRITICAL: Failed to save district {district_name} to CSV: {e}", exc_info=True)
                                # Continue anyway - we'll try to save progress
                    
                    if not csv_saved:
                        logger.warning(f"⚠ District {district_name} was NOT saved to CSV - check errors above")
                    
                    # Update progress (thread-safe)
                    with self.progress_lock:
                        # Track by identifier (URL or name+state)
                        processed_districts.add(district_identifier)
                        progress = {
                            'state': self.state,
                            'processed': len(processed_districts),
                            'processed_districts': list(processed_districts),
                            'last_updated': datetime.now().isoformat()
                        }
                        try:
                            self.save_progress(progress)
                            logger.info(f"Progress saved: {len(processed_districts)} districts processed")
                        except Exception as e:
                            logger.error(f"Error saving progress: {e}", exc_info=True)
                        
                        # Update counters
                        if result['scrape_status'] == 'success':
                            self.processed_count += 1
                        elif result['scrape_status'] == 'error':
                            # Only count actual errors, not scrape_failed or no_url
                            self.error_count += 1
                    
                except Exception as e:
                    logger.error(f"Error processing district {district_name}: {e}", exc_info=True)
                    # Still try to save error result to CSV
                    try:
                        error_result = {
                            'district_data': district_data,
                            'search_results': {
                                'terms_found': [],
                                'page_urls': [],
                                'context_snippets': [],
                                'total_occurrences': 0,
                                'pages_with_terms': 0,
                                'district_terms_found': [],
                                'district_page_urls': [],
                                'district_total_occurrences': 0,
                                'district_pages_with_terms': 0
                            },
                            'ai_summaries': {},
                            'scrape_status': 'error',
                            'error_message': str(e)
                        }
                        district_url = district_data.get('DISTRICT_URL')
                        # Use district URL as identifier if available, otherwise use district name + state
                        district_identifier = district_url if district_url else f"{district_name}_{self.state}"
                        
                        with self.csv_lock:
                            # Only save district if not already saved
                            if district_identifier not in self._processed_districts:
                                update_csv_with_district(
                                    error_result['district_data'],
                                    error_result['search_results'],
                                    error_result['ai_summaries'],
                                    error_result['scrape_status'],
                                    school_names=None
                                )
                                self._processed_districts.add(district_identifier)
                                logger.info(f"✓ Saved district {district_name} (error) to CSV")
                            else:
                                logger.debug(f"District {district_identifier} already saved, skipping error save")
                    except Exception as save_error:
                        logger.error(f"Error saving error result for {district_name}: {save_error}")
                    with self.progress_lock:
                        self.error_count += 1
                        # Track by identifier (URL or name+state)
                        processed_districts.add(district_identifier)
                        progress = {
                            'state': self.state,
                            'processed': len(processed_districts),
                            'processed_districts': list(processed_districts),
                            'last_updated': datetime.now().isoformat()
                        }
                        self.save_progress(progress)
                
                # Log progress periodically
                if completed_count % 10 == 0 or completed_count == total_districts:
                    elapsed = time.time() - self.start_time
                    rate = completed_count / elapsed if elapsed > 0 else 0
                    with self.progress_lock:
                        logger.info(f"Progress: {completed_count}/{total_districts} districts processed "
                                  f"({self.processed_count} success, {self.error_count} errors) "
                                  f"[{rate:.2f} districts/sec]")
        
        # Final summary with performance metrics
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        
        # Final progress save
        with self.progress_lock:
            final_progress = {
                'state': self.state,
                'processed': len(processed_districts),
                'processed_districts': list(processed_districts),
                'last_updated': datetime.now().isoformat(),
                'completed': True
            }
            try:
                self.save_progress(final_progress)
                logger.info(f"✓ Final progress saved: {len(processed_districts)} districts")
            except Exception as e:
                logger.error(f"✗ Failed to save final progress: {e}", exc_info=True)

        # Deduplicate results CSV: one row per district, favour success over scrape_failed
        try:
            removed = deduplicate_results_csv(RESULTS_CSV)
            if removed > 0:
                logger.info(f"✓ Deduplicated {RESULTS_CSV}: removed {removed} duplicate row(s)")
        except Exception as e:
            logger.warning(f"Deduplication of results CSV failed: {e}", exc_info=True)

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
        logger.info(f"Total districts processed: {total_districts}")
        logger.info(f"Successful scrapes: {self.processed_count}")
        logger.info(f"Errors: {self.error_count}")
        logger.info(f"Districts in CSV: {csv_count}")
        logger.info(f"Districts in progress.json: {len(processed_districts)}")
        logger.info(f"Total time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
        if elapsed_time > 0 and total_districts > 0:
            logger.info(f"Average time per district: {elapsed_time/total_districts:.2f} seconds")
            logger.info(f"Throughput: {total_districts/elapsed_time:.2f} districts/second ({total_districts*60/elapsed_time:.2f} districts/minute)")
        logger.info(f"Results CSV: {RESULTS_CSV}")
        logger.info(f"Progress file: {self.progress_file}")
        if csv_count != len(processed_districts):
            logger.warning(f"⚠ WARNING: CSV has {csv_count} rows but {len(processed_districts)} districts were processed!")
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
        '--max-districts',
        type=int,
        default=None,
        help='Maximum number of districts to process (for testing)'
    )
    parser.add_argument(
        '--max',
        type=int,
        default=None,
        dest='max_districts',
        help='Alias for --max-districts (maximum number of districts to process)'
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
    parser.add_argument(
        '--html',
        action='store_true',
        help='Generate HTML report after processing'
    )
    parser.add_argument(
        '--rerun-failed',
        action='store_true',
        help='Rerun districts that have scrape_failed status'
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
        max_districts=getattr(args, 'max_districts', None),
        workers=args.workers,
        delay=args.delay,
        min_delay=getattr(args, 'min_delay', None),
        max_delay=getattr(args, 'max_delay', None),
        rerun_failed=args.rerun_failed
    )
    
    try:
        orchestrator.run()
        
        # Generate HTML report if requested
        if args.html:
            from csv_generator import generate_html_report
            logger.info("Generating HTML report...")
            try:
                html_path = generate_html_report()
                logger.info(f"✓ HTML report generated: {html_path}")
            except Exception as e:
                logger.error(f"Error generating HTML report: {e}", exc_info=True)
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
