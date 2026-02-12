"""CSV generator module for creating detailed output reports."""
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
from config import RESULTS_CSV, OUTPUT_DIR

logger = logging.getLogger(__name__)


def create_results_dataframe(school_data: Dict, search_results: Dict, 
                             ai_summaries: Dict, scrape_status: str = "success") -> pd.DataFrame:
    """
    Create a DataFrame row for a school's results.
    
    Args:
        school_data: Dictionary with school information
        search_results: Dictionary with search results from term_searcher
        ai_summaries: Dictionary with AI contextualization summaries
        scrape_status: Status of the scraping operation
    
    Returns:
        DataFrame with a single row of results
    """
    # Extract basic school info
    school_name = school_data.get('SCH_NAME', '')
    district_name = school_data.get('DISTRICT_NAME') or school_data.get('LEA_NAME', '')
    state = school_data.get('ST', '')
    school_url = school_data.get('SCHOOL_URL', '')
    district_url = school_data.get('DISTRICT_URL', '')
    
    # Extract search results (with safe defaults for missing fields)
    terms_found = ', '.join(search_results.get('terms_found', [])) if search_results.get('terms_found') else ''
    page_urls = ', '.join(search_results.get('page_urls', [])) if search_results.get('page_urls') else ''
    
    # Extract school-specific results (handle missing fields gracefully)
    school_terms_list = search_results.get('school_terms_found', [])
    if isinstance(school_terms_list, set):
        school_terms_list = list(school_terms_list)
    school_terms_found = ', '.join(school_terms_list) if school_terms_list else ''
    school_page_urls = ', '.join(search_results.get('school_page_urls', [])) if search_results.get('school_page_urls') else ''
    school_total_occurrences = search_results.get('school_total_occurrences', 0) or 0
    school_pages_with_terms = search_results.get('school_pages_with_terms', 0) or 0
    
    # Extract district-specific results (handle missing fields gracefully)
    district_terms_list = search_results.get('district_terms_found', [])
    if isinstance(district_terms_list, set):
        district_terms_list = list(district_terms_list)
    district_terms_found = ', '.join(district_terms_list) if district_terms_list else ''
    district_page_urls = ', '.join(search_results.get('district_page_urls', [])) if search_results.get('district_page_urls') else ''
    district_total_occurrences = search_results.get('district_total_occurrences', 0) or 0
    district_pages_with_terms = search_results.get('district_pages_with_terms', 0) or 0
    
    # Extract context snippets with source info
    context_snippets = []
    for snippet_data in search_results.get('context_snippets', []):
        context = snippet_data.get('context', '')
        term = snippet_data.get('term', '')
        url = snippet_data.get('url', '')
        source = snippet_data.get('source', 'unknown')
        context_snippets.append(f"[{term} @ {url} ({source})]: {context}")
    
    context_snippets_str = ' | '.join(context_snippets) if context_snippets else ''
    
    # Extract AI summaries
    # Support both unified mode (single 'summary' key) and per_term mode (dictionary of summaries)
    if 'summary' in ai_summaries:
        # Unified mode: single summary string
        ai_summary_str = ai_summaries.get('summary', '')
    else:
        # Per-term mode (legacy): join multiple summaries
        ai_summary_parts = []
        for key, summary_data in ai_summaries.items():
            if isinstance(summary_data, dict):
                term = summary_data.get('term', '')
                url = summary_data.get('url', '')
                summary = summary_data.get('ai_summary', '')
                ai_summary_parts.append(f"[{term} @ {url}]: {summary}")
        ai_summary_str = ' | '.join(ai_summary_parts) if ai_summary_parts else ''
    
    # Create row data
    row_data = {
        'School Name': school_name,
        'District Name': district_name,
        'State': state,
        'School URL': school_url,
        'District URL': district_url,
        'Terms Found': terms_found,
        'Page URLs Where Terms Found': page_urls,
        # School-specific columns
        'School Terms Found': school_terms_found,
        'School Page URLs': school_page_urls,
        'School Total Occurrences': school_total_occurrences,
        'School Pages With Terms': school_pages_with_terms,
        # District-specific columns
        'District Terms Found': district_terms_found,
        'District Page URLs': district_page_urls,
        'District Total Occurrences': district_total_occurrences,
        'District Pages With Terms': district_pages_with_terms,
        # Other columns
        'Context Snippets': context_snippets_str,
        'AI Summary': ai_summary_str,
        'Total Occurrences': search_results.get('total_occurrences', 0),
        'Pages With Terms': search_results.get('pages_with_terms', 0),
        'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Scrape Status': scrape_status
    }
    
    return pd.DataFrame([row_data])


def append_to_csv(df: pd.DataFrame, csv_path: Path) -> None:
    """
    Append DataFrame to CSV file (create if doesn't exist).
    Handles column mismatches by aligning columns.
    
    Args:
        df: DataFrame to append
        csv_path: Path to CSV file
    """
    try:
        if csv_path.exists():
            # Append to existing file
            existing_df = pd.read_csv(csv_path)
            
            # Align columns - add missing columns to both DataFrames with empty/default values
            all_columns = set(existing_df.columns) | set(df.columns)
            
            # Add missing columns to existing_df
            for col in all_columns:
                if col not in existing_df.columns:
                    existing_df[col] = ''
            
            # Add missing columns to new df
            for col in all_columns:
                if col not in df.columns:
                    df[col] = ''
            
            # Reorder columns to match
            column_order = sorted(all_columns)
            existing_df = existing_df[column_order]
            df = df[column_order]
            
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_csv(csv_path, index=False)
            logger.info(f"✓ Appended to CSV: {csv_path} (now {len(combined_df)} rows, added {len(df)} row(s))")
        else:
            # Create new file
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_path, index=False)
            logger.info(f"✓ Created new CSV: {csv_path} ({len(df)} row(s))")
    except Exception as e:
        logger.error(f"✗ Error writing to CSV {csv_path}: {e}", exc_info=True)
        raise  # Re-raise so caller knows it failed
        raise


def create_results_csv(results: List[Dict], output_path: Optional[Path] = None) -> Path:
    """
    Create a CSV file from a list of result dictionaries.
    
    Args:
        results: List of result dictionaries, each containing:
            - school_data: School information
            - search_results: Search results
            - ai_summaries: AI summaries
            - scrape_status: Scraping status
        output_path: Optional path for output file (defaults to RESULTS_CSV)
    
    Returns:
        Path to created CSV file
    """
    output_path = output_path or RESULTS_CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create list of DataFrames
    dfs = []
    
    for result in results:
        df = create_results_dataframe(
            result.get('school_data', {}),
            result.get('search_results', {}),
            result.get('ai_summaries', {}),
            result.get('scrape_status', 'unknown')
        )
        dfs.append(df)
    
    if not dfs:
        logger.warning("No results to write to CSV")
        # Create empty CSV with headers
        empty_df = create_results_dataframe({}, {}, {}, 'no_data')
        empty_df = empty_df.iloc[0:0]  # Empty but with correct columns
        empty_df.to_csv(output_path, index=False)
        return output_path
    
    # Combine all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Write to CSV
    combined_df.to_csv(output_path, index=False)
    logger.info(f"Created results CSV with {len(combined_df)} rows: {output_path}")
    
    return output_path


def update_csv_with_school(school_data: Dict, search_results: Dict, 
                           ai_summaries: Dict, scrape_status: str = "success",
                           csv_path: Optional[Path] = None) -> None:
    """
    Update CSV file with a single school's results.
    
    Args:
        school_data: School information dictionary
        search_results: Search results dictionary
        ai_summaries: AI summaries dictionary
        scrape_status: Scraping status
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)
    """
    csv_path = csv_path or RESULTS_CSV
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    df = create_results_dataframe(school_data, search_results, ai_summaries, scrape_status)
    append_to_csv(df, csv_path)


def load_existing_results(csv_path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load existing results from CSV.
    
    Args:
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)
    
    Returns:
        DataFrame with existing results
    """
    csv_path = csv_path or RESULTS_CSV
    
    if csv_path.exists():
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} existing results from {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading existing results: {e}")
            return pd.DataFrame()
    else:
        return pd.DataFrame()


def get_processed_schools(csv_path: Optional[Path] = None) -> set:
    """
    Get set of school names that have already been processed.
    
    Args:
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)
    
    Returns:
        Set of school names (or identifiers) that have been processed
    """
    df = load_existing_results(csv_path)
    
    if df.empty or 'School Name' not in df.columns:
        return set()
    
    # Use school name + state as identifier
    if 'State' in df.columns:
        processed = set(zip(df['School Name'], df['State']))
    else:
        processed = set(df['School Name'].unique())
    
    return processed


if __name__ == "__main__":
    # Test the CSV generator
    from utils.logging_config import setup_logging
    
    setup_logging()
    
    # Create sample data
    school_data = {
        'SCH_NAME': 'Test School',
        'DISTRICT_NAME': 'Test District',
        'ST': 'NC',
        'SCHOOL_URL': 'https://example.com/school',
        'DISTRICT_URL': 'https://example.com/district'
    }
    
    search_results = {
        'terms_found': ['restorative justice', 'race equity'],
        'page_urls': ['https://example.com/policy'],
        'context_snippets': [
            {'term': 'restorative justice', 'context': 'We implement restorative justice practices...', 'url': 'https://example.com/policy'}
        ],
        'total_occurrences': 2,
        'pages_with_terms': 1
    }
    
    ai_summaries = {
        'key1': {
            'term': 'restorative justice',
            'url': 'https://example.com/policy',
            'context': 'We implement restorative justice practices...',
            'ai_summary': 'The school uses restorative justice as an alternative disciplinary approach.'
        }
    }
    
    # Test creating CSV
    test_csv = OUTPUT_DIR / "test_results.csv"
    update_csv_with_school(school_data, search_results, ai_summaries, 'success', test_csv)
    
    print(f"\nTest CSV created: {test_csv}")
    df = pd.read_csv(test_csv)
    print(f"\nCSV contents:")
    print(df.to_string())

