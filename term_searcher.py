"""Term search module for finding policy-related terms in scraped content."""
import re
import logging
from typing import List, Dict, Set, Optional
from config import SEARCH_TERMS, SCRAPING_CONFIG

logger = logging.getLogger(__name__)


class TermSearcher:
    """Search for terms in text content with context extraction."""
    
    def __init__(self, search_terms: Optional[List[str]] = None, 
                 context_length: Optional[int] = None):
        """
        Initialize the term searcher.
        
        Args:
            search_terms: List of terms to search for (defaults to SEARCH_TERMS)
            context_length: Length of context snippet in characters (defaults to config)
        """
        self.search_terms = search_terms or SEARCH_TERMS
        self.context_length = context_length or SCRAPING_CONFIG.get('context_snippet_length', 200)
        
        # Create case-insensitive regex patterns
        self.patterns = {}
        for term in self.search_terms:
            # Escape special regex characters and create pattern
            escaped_term = re.escape(term)
            self.patterns[term] = re.compile(escaped_term, re.IGNORECASE)
    
    def find_term_occurrences(self, text: str, term: str) -> List[Dict]:
        """
        Find all occurrences of a term in text with context.
        
        Args:
            text: Text to search in
            term: Term to search for
        
        Returns:
            List of dictionaries with match information
        """
        if term not in self.patterns:
            return []
        
        pattern = self.patterns[term]
        matches = []
        
        for match in pattern.finditer(text):
            start = match.start()
            end = match.end()
            
            # Extract context
            context_start = max(0, start - self.context_length // 2)
            context_end = min(len(text), end + self.context_length // 2)
            context = text[context_start:context_end].strip()
            
            # Clean up context (remove extra whitespace)
            context = ' '.join(context.split())
            
            matches.append({
                'term': term,
                'start': start,
                'end': end,
                'context': context,
                'matched_text': match.group()
            })
        
        return matches
    
    def search_text(self, text: str, page_url: str) -> Dict:
        """
        Search for all terms in text.
        
        Args:
            text: Text content to search
            page_url: URL of the page being searched
        
        Returns:
            Dictionary with search results
        """
        results = {
            'url': page_url,
            'terms_found': [],
            'occurrences': [],
            'total_matches': 0
        }
        
        found_terms = set()
        
        for term in self.search_terms:
            matches = self.find_term_occurrences(text, term)
            
            if matches:
                found_terms.add(term)
                results['occurrences'].extend(matches)
                results['total_matches'] += len(matches)
        
        results['terms_found'] = list(found_terms)
        
        return results
    
    def search_pages(self, pages: List[Dict]) -> List[Dict]:
        """
        Search for terms across multiple pages.
        Preserves source information (school vs district) from pages.
        
        Args:
            pages: List of page dictionaries with 'url', 'text', and optionally 'source' keys
        
        Returns:
            List of search result dictionaries with 'source' field preserved
        """
        all_results = []
        
        for page in pages:
            url = page.get('url', '')
            text = page.get('text', '')
            source = page.get('source', 'unknown')  # Preserve source info
            
            if not text:
                continue
            
            result = self.search_text(text, url)
            # Add source information to result
            result['source'] = source
            
            if result['terms_found']:
                all_results.append(result)
                logger.info(f"Found {len(result['terms_found'])} term(s) in {url} (source: {source})")
        
        return all_results
    
    def aggregate_results(self, search_results: List[Dict]) -> Dict:
        """
        Aggregate search results across all pages.
        Separates results by source (school vs district).
        
        Args:
            search_results: List of search result dictionaries with 'source' field
        
        Returns:
            Aggregated results dictionary with school/district separation
        """
        aggregated = {
            'terms_found': set(),
            'page_urls': [],
            'context_snippets': [],
            'total_occurrences': 0,
            'pages_with_terms': len(search_results),
            # Separate tracking for school vs district
            'school_terms_found': set(),
            'school_page_urls': [],
            'school_total_occurrences': 0,
            'school_pages_with_terms': 0,
            'district_terms_found': set(),
            'district_page_urls': [],
            'district_total_occurrences': 0,
            'district_pages_with_terms': 0
        }
        
        for result in search_results:
            source = result.get('source', 'unknown')  # 'school', 'district', or 'unknown'
            aggregated['terms_found'].update(result['terms_found'])
            aggregated['page_urls'].append(result['url'])
            aggregated['total_occurrences'] += result['total_matches']
            
            # Separate by source
            if source == 'school':
                aggregated['school_terms_found'].update(result['terms_found'])
                aggregated['school_page_urls'].append(result['url'])
                aggregated['school_total_occurrences'] += result['total_matches']
                aggregated['school_pages_with_terms'] += 1
            elif source == 'district':
                aggregated['district_terms_found'].update(result['terms_found'])
                aggregated['district_page_urls'].append(result['url'])
                aggregated['district_total_occurrences'] += result['total_matches']
                aggregated['district_pages_with_terms'] += 1
            
            # Collect context snippets with source info
            for occurrence in result['occurrences']:
                aggregated['context_snippets'].append({
                    'term': occurrence['term'],
                    'context': occurrence['context'],
                    'url': result['url'],
                    'source': source
                })
        
        # Convert sets to sorted lists
        aggregated['terms_found'] = sorted(list(aggregated['terms_found']))
        aggregated['school_terms_found'] = sorted(list(aggregated['school_terms_found']))
        aggregated['district_terms_found'] = sorted(list(aggregated['district_terms_found']))
        
        return aggregated


def search_school_content(pages: List[Dict], search_terms: Optional[List[str]] = None) -> Dict:
    """
    Search for terms in scraped school/district pages.
    
    Args:
        pages: List of scraped page dictionaries
        search_terms: Optional list of terms to search for
    
    Return        Dictionary with aggregated search results (always includes all fields, even if empty)
    """
    searcher = TermSearcher(search_terms=search_terms)
    search_results = searcher.search_pages(pages)
    aggregated = searcher.aggregate_results(search_results)
    
    # Ensure all required fields exist even if empty
    if 'school_terms_found' not in aggregated:
        aggregated['school_terms_found'] = []
    if 'school_page_urls' not in aggregated:
        aggregated['school_page_urls'] = []
    if 'school_total_occurrences' not in aggregated:
        aggregated['school_total_occurrences'] = 0
    if 'school_pages_with_terms' not in aggregated:
        aggregated['school_pages_with_terms'] = 0
    if 'district_terms_found' not in aggregated:
        aggregated['district_terms_found'] = []
    if 'district_page_urls' not in aggregated:
        aggregated['district_page_urls'] = []
    if 'district_total_occurrences' not in aggregated:
        aggregated['district_total_occurrences'] = 0
    if 'district_pages_with_terms' not in aggregated:
        aggregated['district_pages_with_terms'] = 0
    
    return aggregated


if __name__ == "__main__":
    # Test the term searcher
    from utils.logging_config import setup_logging
    
    setup_logging()
    
    # Test with sample text
    test_text = """
    Our school district is committed to implementing restorative justice practices
    in all our schools. We believe that restorative justice helps build stronger
    communities. Additionally, we focus on race equity in our educational programs.
    """
    
    searcher = TermSearcher()
    result = searcher.search_text(test_text, "https://example.com/test")
    
    print(f"\nSearch Results:")
    print(f"Terms found: {result['terms_found']}")
    print(f"Total matches: {result['total_matches']}")
    print(f"\nOccurrences:")
    for occ in result['occurrences']:
        print(f"  - {occ['term']}: {occ['context'][:100]}...")

