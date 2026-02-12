"""Data extraction module for reading Excel files and filtering schools."""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional, List, Dict
from config import SCHOOLS_FILE, DISTRICTS_FILE, DEFAULT_STATE

logger = logging.getLogger(__name__)


def read_schools_file(file_path: Path, state_filter: Optional[str] = None) -> pd.DataFrame:
    """
    Read the schools Excel file and optionally filter by state.
    
    Args:
        file_path: Path to the schools Excel file
        state_filter: State code to filter by (e.g., 'NC'). If None, returns all schools.
    
    Returns:
        DataFrame with school data
    """
    try:
        logger.info(f"Reading schools file: {file_path}")
        df = pd.read_excel(file_path)
        logger.info(f"Loaded {len(df)} total schools")
        
        if state_filter:
            df = df[df['ST'] == state_filter.upper()]
            logger.info(f"Filtered to {len(df)} schools in {state_filter}")
        
        return df
    except Exception as e:
        logger.error(f"Error reading schools file: {e}")
        raise


def read_districts_file(file_path: Path, state_filter: Optional[str] = None) -> pd.DataFrame:
    """
    Read the districts Excel file and optionally filter by state.
    
    Args:
        file_path: Path to the districts Excel file
        state_filter: State code to filter by (e.g., 'NC'). If None, returns all districts.
    
    Returns:
        DataFrame with district data
    """
    try:
        logger.info(f"Reading districts file: {file_path}")
        # Districts file has header in row 1 (0-indexed)
        df = pd.read_excel(file_path, header=1)
        logger.info(f"Loaded {len(df)} total districts")
        
        if state_filter:
            df = df[df['ST'] == state_filter.upper()]
            logger.info(f"Filtered to {len(df)} districts in {state_filter}")
        
        return df
    except Exception as e:
        logger.error(f"Error reading districts file: {e}")
        raise


def merge_school_district_data(schools_df: pd.DataFrame, districts_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge school data with district data to get district URLs.
    
    Args:
        schools_df: DataFrame with school data
        districts_df: DataFrame with district data
    
    Returns:
        Merged DataFrame with both school and district information
    """
    try:
        # Merge on LEAID (Local Education Agency ID)
        merged = schools_df.merge(
            districts_df[['LEAID', 'LEA_NAME', 'WEBSITE']],
            on='LEAID',
            how='left',
            suffixes=('', '_district')
        )
        
        # Rename columns for clarity
        merged = merged.rename(columns={
            'WEBSITE': 'SCHOOL_URL',
            'WEBSITE_district': 'DISTRICT_URL',
            'LEA_NAME_district': 'DISTRICT_NAME'
        })
        
        # Use LEA_NAME from schools if DISTRICT_NAME is missing
        if 'DISTRICT_NAME' not in merged.columns or merged['DISTRICT_NAME'].isna().any():
            merged['DISTRICT_NAME'] = merged.get('LEA_NAME', merged.get('DISTRICT_NAME', ''))
        
        logger.info(f"Merged {len(merged)} schools with district data")
        return merged
    except Exception as e:
        logger.error(f"Error merging data: {e}")
        raise


def extract_school_urls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract and clean school/district URLs from the DataFrame.
    
    Args:
        df: DataFrame with school data
    
    Returns:
        DataFrame with cleaned URLs
    """
    df = df.copy()
    
    # Clean URLs - remove NaN and convert to string
    for col in ['SCHOOL_URL', 'DISTRICT_URL']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('nan', '')
            df[col] = df[col].str.strip()
            df[col] = df[col].replace('', None)
    
    # Count schools with URLs
    schools_with_urls = df['SCHOOL_URL'].notna().sum()
    districts_with_urls = df['DISTRICT_URL'].notna().sum()
    
    logger.info(f"Schools with URLs: {schools_with_urls}/{len(df)}")
    logger.info(f"Districts with URLs: {districts_with_urls}/{len(df)}")
    
    return df


def get_schools_for_state(state: str = DEFAULT_STATE) -> pd.DataFrame:
    """
    Main function to get all schools for a given state with URLs.
    
    Args:
        state: State code (default: NC)
    
    Returns:
        DataFrame with school data including URLs
    """
    logger.info(f"Extracting schools for state: {state}")
    
    # Read both files
    schools_df = read_schools_file(SCHOOLS_FILE, state_filter=state)
    districts_df = read_districts_file(DISTRICTS_FILE, state_filter=state)
    
    # Merge data
    merged_df = merge_school_district_data(schools_df, districts_df)
    
    # Extract and clean URLs
    final_df = extract_school_urls(merged_df)
    
    # Select relevant columns for output
    output_columns = [
        'SCH_NAME', 'LEA_NAME', 'DISTRICT_NAME', 'ST', 'STATENAME',
        'SCHOOL_URL', 'DISTRICT_URL', 'LEAID', 'NCESSCH', 'MCITY', 'MZIP'
    ]
    
    # Only include columns that exist
    available_columns = [col for col in output_columns if col in final_df.columns]
    final_df = final_df[available_columns]
    
    logger.info(f"Final dataset: {len(final_df)} schools")
    return final_df


if __name__ == "__main__":
    # Test the data extraction
    from utils.logging_config import setup_logging
    
    setup_logging()
    df = get_schools_for_state("NC")
    print(f"\nExtracted {len(df)} North Carolina schools")
    print(f"\nSample data:")
    print(df.head())
    print(f"\nColumns: {df.columns.tolist()}")

