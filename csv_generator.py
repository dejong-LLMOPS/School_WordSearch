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
    schools_in_district = school_data.get('SCHOOLS_IN_DISTRICT', 0)
    
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
        'Schools in District': schools_in_district,
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


def create_district_dataframe(district_data: Dict, search_results: Dict, 
                              ai_summaries: Dict, scrape_status: str = "success",
                              school_names: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Create a DataFrame row for a district's results.
    
    Args:
        district_data: Dictionary with district information
        search_results: Dictionary with search results from term_searcher
        ai_summaries: Dictionary with AI contextualization summaries
        scrape_status: Status of the scraping operation
        school_names: Optional list of school names in this district (not used, kept for compatibility)
    
    Returns:
        DataFrame with a single row of results
    """
    # Extract basic district info
    district_name = district_data.get('DISTRICT_NAME') or district_data.get('LEA_NAME', '')
    state = district_data.get('ST', '')
    district_url = district_data.get('DISTRICT_URL', '')
    schools_in_district = district_data.get('SCHOOLS_IN_DISTRICT', 0)
    
    # Extract search results (with safe defaults for missing fields)
    terms_found = ', '.join(search_results.get('terms_found', [])) if search_results.get('terms_found') else ''
    page_urls = ', '.join(search_results.get('page_urls', [])) if search_results.get('page_urls') else ''
    
    # Extract district-specific results
    district_terms_list = search_results.get('district_terms_found', [])
    if isinstance(district_terms_list, set):
        district_terms_list = list(district_terms_list)
    district_terms_found = ', '.join(district_terms_list) if district_terms_list else ''
    district_page_urls = ', '.join(search_results.get('district_page_urls', [])) if search_results.get('district_page_urls') else ''
    district_total_occurrences = search_results.get('district_total_occurrences', 0) or 0
    district_pages_with_terms = search_results.get('district_pages_with_terms', 0) or 0
    
    # Extract context snippets
    context_snippets = []
    for snippet_data in search_results.get('context_snippets', []):
        context = snippet_data.get('context', '')
        term = snippet_data.get('term', '')
        url = snippet_data.get('url', '')
        source = snippet_data.get('source', 'unknown')
        context_snippets.append(f"[{term} @ {url} ({source})]: {context}")
    
    context_snippets_str = ' | '.join(context_snippets) if context_snippets else ''
    
    # Extract AI summaries
    if 'summary' in ai_summaries:
        ai_summary_str = ai_summaries.get('summary', '')
    else:
        ai_summary_parts = []
        for key, summary_data in ai_summaries.items():
            if isinstance(summary_data, dict):
                term = summary_data.get('term', '')
                url = summary_data.get('url', '')
                summary = summary_data.get('ai_summary', '')
                ai_summary_parts.append(f"[{term} @ {url}]: {summary}")
        ai_summary_str = ' | '.join(ai_summary_parts) if ai_summary_parts else ''
    
    # Create row data (district-level) - exact column order as specified
    row_data = {
        'State': state,
        'District': district_name,
        'District Website': district_url,
        'Count of Schools in the District': schools_in_district,
        'AI Summary': ai_summary_str,
        'Context Snippets': context_snippets_str,
        'District Page URLs': district_page_urls,
        'District Pages With Terms': district_pages_with_terms,
        'District Terms Found': district_terms_found,
        'District Total Occurrences': district_total_occurrences,
        'District URL': district_url,  # Keep for reference
        'Page URLs Where Terms Found': page_urls,
        'Pages With Terms': search_results.get('pages_with_terms', 0),
        'Scrape Status': scrape_status
    }
    
    return pd.DataFrame([row_data])


def update_csv_with_district(district_data: Dict, search_results: Dict, 
                            ai_summaries: Dict, scrape_status: str = "success",
                            school_names: Optional[List[str]] = None,
                            csv_path: Optional[Path] = None) -> None:
    """
    Update CSV file with a single district's results.
    Removes any existing entries for the same district before adding the new one.
    
    Args:
        district_data: District information dictionary
        search_results: Search results dictionary
        ai_summaries: AI summaries dictionary
        scrape_status: Scraping status
        school_names: Optional list of school names in this district (not used, kept for compatibility)
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)
    """
    csv_path = csv_path or RESULTS_CSV
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Remove existing entries for this district to avoid duplicates
    district_url = district_data.get('DISTRICT_URL') or district_data.get('District Website', '')
    district_name = district_data.get('DISTRICT_NAME') or district_data.get('LEA_NAME', '')
    state = district_data.get('ST', '')
    
    if csv_path.exists():
        try:
            existing_df = pd.read_csv(csv_path)
            
            # Remove rows matching this district (by URL if available, otherwise by name+state)
            if 'District Website' in existing_df.columns and district_url:
                existing_df = existing_df[existing_df['District Website'] != district_url]
            elif 'District URL' in existing_df.columns and district_url:
                existing_df = existing_df[existing_df['District URL'] != district_url]
            elif 'District' in existing_df.columns and 'State' in existing_df.columns:
                # Fallback to name+state matching
                existing_df = existing_df[~((existing_df['District'] == district_name) & (existing_df['State'] == state))]
            
            # Write back the filtered dataframe
            existing_df.to_csv(csv_path, index=False)
        except Exception as e:
            logger.warning(f"Could not remove existing entries for district {district_name}: {e}")
            # Continue anyway - we'll just append (may create duplicates)
    
    df = create_district_dataframe(district_data, search_results, ai_summaries, scrape_status, school_names)
    append_to_csv(df, csv_path)


def get_processed_districts(csv_path: Optional[Path] = None) -> set:
    """
    Get set of districts that have already been processed.
    
    Args:
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)
    
    Returns:
        Set of district URLs (or identifiers) that have been processed
    """
    df = load_existing_results(csv_path)
    
    if df.empty:
        return set()
    
    # Try new column name first, then fall back to old column name for backward compatibility
    if 'District Website' in df.columns:
        processed = set(df['District Website'].dropna().unique())
    elif 'District URL' in df.columns:
        processed = set(df['District URL'].dropna().unique())
    else:
        return set()
    
    return processed


def get_failed_districts(csv_path: Optional[Path] = None) -> set:
    """
    Get set of districts that have scrape_failed status.
    
    Args:
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)
    
    Returns:
        Set of district URLs (or identifiers) that have scrape_failed status
    """
    df = load_existing_results(csv_path)
    
    if df.empty or 'Scrape Status' not in df.columns:
        return set()
    
    # Filter to only scrape_failed districts
    failed_df = df[df['Scrape Status'] == 'scrape_failed']
    
    if failed_df.empty:
        return set()
    
    # Get district URLs/websites for failed districts
    if 'District Website' in failed_df.columns:
        failed = set(failed_df['District Website'].dropna().unique())
    elif 'District URL' in failed_df.columns:
        failed = set(failed_df['District URL'].dropna().unique())
    else:
        return set()
    
    return failed


def deduplicate_results_csv(csv_path: Optional[Path] = None) -> int:
    """
    Deduplicate results CSV by district. When both scrape_failed and success exist
    for the same district, keep the success row.

    Args:
        csv_path: Optional path to CSV file (defaults to RESULTS_CSV)

    Returns:
        Number of rows removed.
    """
    csv_path = csv_path or RESULTS_CSV
    if not csv_path.exists():
        return 0
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        logger.warning(f"Could not read CSV for deduplication: {e}")
        return 0
    if df.empty or len(df) == 0:
        return 0
    if 'Scrape Status' not in df.columns:
        return 0

    # Key: District URL or District Website; if missing, District + State
    url_col = 'District URL' if 'District URL' in df.columns else ('District Website' if 'District Website' in df.columns else None)
    if url_col:
        key = df[url_col].fillna('').astype(str)
    else:
        key = pd.Series('', index=df.index)
    if 'District' in df.columns and 'State' in df.columns:
        fallback = df['District'].fillna('').astype(str) + '::' + df['State'].fillna('').astype(str)
        key = key.where(key != '', fallback)
    df = df.copy()
    df['_key'] = key

    # Prefer success over scrape_failed over other statuses (lower number = keep)
    status_priority = {'success': 0, 'scrape_failed': 1, 'no_url': 2, 'error': 3}
    df['_priority'] = df['Scrape Status'].map(lambda s: status_priority.get(str(s).strip(), 4))

    df = df.sort_values('_priority')
    deduped = df.drop_duplicates(subset=['_key'], keep='first')
    deduped = deduped.drop(columns=['_key', '_priority'], errors='ignore')
    removed = len(df) - len(deduped)
    if removed > 0:
        deduped.to_csv(csv_path, index=False)
        logger.info(f"Deduplicated results CSV: removed {removed} duplicate(s), favouring success over scrape_failed")
    return removed


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


def generate_html_report(csv_path: Optional[Path] = None, output_path: Optional[Path] = None) -> Path:
    """
    Generate an HTML report from the CSV results.
    
    Args:
        csv_path: Path to CSV file (defaults to RESULTS_CSV)
        output_path: Path for HTML output (defaults to OUTPUT_DIR / "results.html")
    
    Returns:
        Path to generated HTML file
    """
    csv_path = csv_path or RESULTS_CSV
    output_path = output_path or OUTPUT_DIR / "results.html"
    
    if not csv_path.exists():
        logger.warning(f"CSV file not found: {csv_path}")
        return output_path
    
    try:
        df = pd.read_csv(csv_path)
        
        # Generate HTML
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>District Policy Analysis Report</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        .district-card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .district-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #ecf0f1;
        }}
        .district-name {{
            font-size: 1.5em;
            font-weight: bold;
            color: #2c3e50;
        }}
        .district-meta {{
            color: #7f8c8d;
            font-size: 0.9em;
        }}
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: bold;
        }}
        .status-success {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .status-no-url {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .ai-summary {{
            background-color: #f8f9fa;
            padding: 15px;
            border-left: 4px solid #3498db;
            margin: 15px 0;
            white-space: pre-wrap;
        }}
        .terms-found {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin: 10px 0;
        }}
        .term-badge {{
            background-color: #e8f4f8;
            color: #2c3e50;
            padding: 5px 10px;
            border-radius: 15px;
            font-size: 0.85em;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin: 15px 0;
        }}
        .stat-item {{
            background-color: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #3498db;
        }}
        .stat-label {{
            font-size: 0.85em;
            color: #7f8c8d;
        }}
        .urls-list {{
            background-color: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            margin: 10px 0;
            max-height: 200px;
            overflow-y: auto;
        }}
        .urls-list a {{
            color: #3498db;
            text-decoration: none;
            display: block;
            padding: 3px 0;
        }}
        .urls-list a:hover {{
            text-decoration: underline;
        }}
        .context-snippets {{
            background-color: #fff;
            padding: 10px;
            border-radius: 4px;
            margin: 10px 0;
            max-height: 300px;
            overflow-y: auto;
            font-size: 0.9em;
            border: 1px solid #dee2e6;
        }}
        .summary-header {{
            font-weight: bold;
            color: #2c3e50;
            margin-top: 15px;
            margin-bottom: 10px;
        }}
    </style>
</head>
<body>
    <h1>District Policy Analysis Report</h1>
    <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p>Total Districts: {len(df)}</p>
"""
        
        # Process each district
        for idx, row in df.iterrows():
            district_name = row.get('District', 'Unknown')
            state = row.get('State', '')
            district_url = row.get('District Website', '')
            schools_count = row.get('Count of Schools in the District', 0)
            ai_summary = row.get('AI Summary', '')
            terms_found = row.get('District Terms Found', '')
            context_snippets = row.get('Context Snippets', '')
            page_urls = row.get('District Page URLs', '')
            total_occurrences = row.get('District Total Occurrences', 0)
            pages_with_terms = row.get('District Pages With Terms', 0)
            scrape_status = row.get('Scrape Status', 'unknown')
            
            # Status badge
            status_class = {
                'success': 'status-success',
                'scrape_failed': 'status-failed',
                'no_url': 'status-no-url',
                'error': 'status-failed'
            }.get(scrape_status, 'status-failed')
            
            # Terms list
            terms_list = [t.strip() for t in terms_found.split(',') if t.strip()] if terms_found else []
            
            # URLs list
            urls_list = [u.strip() for u in page_urls.split(',') if u.strip()] if page_urls else []
            
            html_content += f"""
    <div class="district-card">
        <div class="district-header">
            <div>
                <div class="district-name">{district_name}</div>
                <div class="district-meta">{state} • {schools_count} schools</div>
            </div>
            <span class="status-badge {status_class}">{scrape_status}</span>
        </div>
"""
            
            if ai_summary:
                html_content += f"""
        <div class="summary-header">AI Summary:</div>
        <div class="ai-summary">{ai_summary}</div>
"""
            
            if terms_list:
                html_content += f"""
        <div class="summary-header">Terms Found:</div>
        <div class="terms-found">
"""
                for term in terms_list:
                    html_content += f'            <span class="term-badge">{term}</span>\n'
                html_content += "        </div>\n"
            
            html_content += f"""
        <div class="stats">
            <div class="stat-item">
                <div class="stat-value">{total_occurrences}</div>
                <div class="stat-label">Total Occurrences</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{pages_with_terms}</div>
                <div class="stat-label">Pages With Terms</div>
            </div>
            <div class="stat-item">
                <div class="stat-value">{len(urls_list)}</div>
                <div class="stat-label">Pages Scraped</div>
            </div>
        </div>
"""
            
            if district_url:
                html_content += f"""
        <div class="summary-header">District Website:</div>
        <p><a href="{district_url}" target="_blank">{district_url}</a></p>
"""
            
            if urls_list:
                html_content += f"""
        <div class="summary-header">Pages Where Terms Were Found:</div>
        <div class="urls-list">
"""
                for url in urls_list[:20]:  # Limit to first 20 URLs
                    html_content += f'            <a href="{url}" target="_blank">{url}</a>\n'
                if len(urls_list) > 20:
                    html_content += f'            <p>... and {len(urls_list) - 20} more</p>\n'
                html_content += "        </div>\n"
            
            if context_snippets:
                # Split context snippets
                snippets = context_snippets.split(' | ') if ' | ' in context_snippets else [context_snippets]
                html_content += f"""
        <div class="summary-header">Context Snippets:</div>
        <div class="context-snippets">
"""
                for snippet in snippets[:10]:  # Limit to first 10 snippets
                    # Escape HTML special characters
                    snippet_escaped = snippet.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                    html_content += f'            <p>{snippet_escaped[:500]}{"..." if len(snippet) > 500 else ""}</p>\n'
                if len(snippets) > 10:
                    html_content += f'            <p>... and {len(snippets) - 10} more snippets</p>\n'
                html_content += "        </div>\n"
            
            html_content += "    </div>\n"
        
        html_content += """
</body>
</html>
"""
        
        # Write HTML file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Generated HTML report: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Error generating HTML report: {e}", exc_info=True)
        raise


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

