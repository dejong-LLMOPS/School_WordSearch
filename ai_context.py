"""AI contextualization module using Perplexity API."""
import logging
import time
import hashlib
import threading
from typing import Dict, List, Optional
from config import PERPLEXITY_CONFIG

logger = logging.getLogger(__name__)

# Try to import Perplexity SDK (optional)
try:
    from perplexity import Perplexity
    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False
    logger.warning("perplexity module not available. Install with: pip install perplexityai")
    Perplexity = None


class PerplexityClient:
    """Client for Perplexity AI Chat Completions API using the official SDK."""
    
    # Class-level cache shared across all instances (thread-safe)
    _summary_cache: Dict[str, Dict] = {}
    _cache_lock = threading.Lock()
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize Perplexity client.
        
        Args:
            config: Optional configuration dictionary (defaults to PERPLEXITY_CONFIG)
        """
        self.config = config or PERPLEXITY_CONFIG
        self.api_key = self.config.get('api_key', '')
        self.model = self.config.get('model', 'sonar')
        self.max_retries = self.config.get('max_retries', 3)
        
        if not PERPLEXITY_AVAILABLE:
            logger.warning("Perplexity SDK not available. AI contextualization will be disabled.")
            self.client = None
        elif not self.api_key:
            logger.warning("Perplexity API key not found. AI contextualization will be disabled.")
            self.client = None
        else:
            # Initialize Perplexity SDK client
            # SDK automatically reads PERPLEXITY_API_KEY from environment
            # But we can also pass it explicitly if needed
            try:
                self.client = Perplexity(api_key=self.api_key)
            except Exception as e:
                logger.error(f"Error initializing Perplexity client: {e}")
                self.client = None
    
    def _get_context_hash(self, term: str, context_snippet: str, page_url: str, 
                         page_content: Optional[str] = None) -> str:
        """
        Generate a hash for caching based on context.
        
        Args:
            term: The search term
            context_snippet: Context snippet around the term
            page_url: URL where term was found
            page_content: Optional full page content
        
        Returns:
            Hash string for caching
        """
        # Use a normalized version of the context for hashing
        # Include term, normalized context (first 500 chars), and URL
        normalized_context = context_snippet[:500].strip().lower()
        content_hash = hashlib.md5(
            f"{term}|{normalized_context}|{page_url}".encode('utf-8')
        ).hexdigest()
        return content_hash
    
    def _get_cached_summary(self, context_hash: str) -> Optional[Dict]:
        """Get cached summary if available (thread-safe)."""
        with self._cache_lock:
            return self._summary_cache.get(context_hash)
    
    def _set_cached_summary(self, context_hash: str, summary_data: Dict) -> None:
        """Cache a summary (thread-safe)."""
        with self._cache_lock:
            self._summary_cache[context_hash] = summary_data
    
    def _make_request(self, messages: List[Dict], retry_count: int = 0, max_tokens: int = 500) -> Optional[str]:
        """
        Make a chat completions request to Perplexity API using the SDK.
        
        Args:
            messages: List of message dictionaries with 'role' and 'content' keys
            retry_count: Current retry attempt
            max_tokens: Maximum tokens for response (default 500)
        
        Returns:
            Response text or None if error
        """
        if not self.client:
            return None
        
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.2,
                max_tokens=max_tokens
            )
            
            # Extract content from response
            if completion.choices and len(completion.choices) > 0:
                return completion.choices[0].message.content
            else:
                logger.warning("No content in API response")
                return None
                
        except Exception as e:
            error_str = str(e).lower()
            
            # Handle rate limiting
            if '429' in error_str or 'rate limit' in error_str:
                if retry_count < self.max_retries:
                    wait_time = (retry_count + 1) * 5
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    return self._make_request(messages, retry_count + 1)
                else:
                    logger.error("Rate limit exceeded. Max retries reached.")
                    return None
            
            # Handle authentication errors
            elif '401' in error_str or 'unauthorized' in error_str:
                logger.error("Invalid API key. Please check your Perplexity API credentials.")
                return None
            
            else:
                logger.error(f"API request error: {e}")
                return None
    
    def contextualize_term(self, term: str, context_snippet: str, page_url: str, 
                          page_content: Optional[str] = None) -> Optional[str]:
        """
        Get AI contextualization for a found term using chat completions.
        Uses caching to avoid duplicate requests for the same context.
        
        Args:
            term: The term that was found
            context_snippet: The context snippet around the term
            page_url: URL where the term was found
            page_content: Optional full page content for more context
        
        Returns:
            AI-generated contextual summary or None if error
        """
        if not self.api_key:
            return None
        
        # Check cache first
        context_hash = self._get_context_hash(term, context_snippet, page_url, page_content)
        cached = self._get_cached_summary(context_hash)
        if cached:
            logger.debug(f"Using cached AI summary for term '{term}' (hash: {context_hash[:8]}...)")
            return cached.get('ai_summary')
        
        # Build the prompt
        if page_content and len(page_content) < 5000:
            user_content = f"""Analyze the following content from a school or district website and provide a brief summary of how the term "{term}" is being used and what it means in this context.

Page URL: {page_url}

Page content:
{page_content[:5000]}

Context snippet where term appears:
{context_snippet}

Please provide a concise summary (2-3 sentences) explaining:
1. How the term is being used in this context
2. What it means for the school/district's policies or practices
3. Any important implications"""
        else:
            user_content = f"""Analyze the following context from a school or district website and provide a brief summary of how the term "{term}" is being used and what it means in this context.

Context snippet:
{context_snippet}

Page URL: {page_url}

Please provide a concise summary (2-3 sentences) explaining:
1. How the term is being used in this context
2. What it means for the school/district's policies or practices
3. Any important implications"""
        
        # Format as chat messages
        messages = [
            {
                'role': 'user',
                'content': user_content
            }
        ]
        
        logger.debug(f"Requesting AI contextualization for term '{term}' from {page_url}")
        result = self._make_request(messages)
        
        if result:
            logger.debug(f"Received AI contextualization: {result[:100]}...")
            # Cache the result
            self._set_cached_summary(context_hash, {
                'term': term,
                'url': page_url,
                'context': context_snippet,
                'ai_summary': result
            })
        
        return result
    
    def contextualize_multiple_terms(self, search_results: Dict, 
                                     page_content_map: Optional[Dict[str, str]] = None) -> Dict:
        """
        Contextualize all found terms from search results.
        Batches similar requests and uses caching to minimize API calls.
        
        Args:
            search_results: Dictionary with search results from term_searcher
            page_content_map: Optional dictionary mapping URLs to full page content
        
        Returns:
            Dictionary with AI summaries for each term/context
        """
        if not self.api_key:
            logger.warning("Skipping AI contextualization - API key not available")
            return {}
        
        summaries = {}
        cache_hits = 0
        api_calls = 0
        
        # Get unique term-context combinations
        for snippet_data in search_results.get('context_snippets', []):
            term = snippet_data['term']
            context = snippet_data['context']
            url = snippet_data['url']
            
            # Get page content if available
            page_content = page_content_map.get(url) if page_content_map else None
            
            # Check cache first
            context_hash = self._get_context_hash(term, context, url, page_content)
            cached = self._get_cached_summary(context_hash)
            
            if cached:
                cache_hits += 1
                summaries[context_hash] = cached
            else:
                # Make API call
                api_calls += 1
                summary = self.contextualize_term(term, context, url, page_content)
                if summary:
                    summaries[context_hash] = {
                        'term': term,
                        'url': url,
                        'context': context,
                        'ai_summary': summary
                    }
                
                # Add delay between API calls to respect rate limits
                if api_calls > 0:
                    time.sleep(1)
        
        if cache_hits > 0:
            logger.info(f"AI contextualization: {cache_hits} cache hits, {api_calls} API calls")
        
        return summaries
    
    def _get_school_cache_hash(self, school_name: str, district_name: str, page_urls: List[str]) -> str:
        """
        Generate a hash for caching school-level summaries.
        
        Args:
            school_name: Name of the school
            district_name: Name of the district
            page_urls: List of all page URLs scraped
        
        Returns:
            Hash string for caching
        """
        # Sort URLs for consistent hashing
        sorted_urls = sorted(page_urls)
        url_string = '|'.join(sorted_urls)
        content_hash = hashlib.md5(
            f"{school_name}|{district_name}|{url_string}".encode('utf-8')
        ).hexdigest()
        return content_hash
    
    def contextualize_school_approach(self, school_name: str, district_name: str, 
                                     search_results: Dict, 
                                     page_content_map: Optional[Dict[str, str]] = None,
                                     is_district_level: bool = False) -> Optional[str]:
        """
        Get a unified AI summary of a school/district's restorative justice approach.
        Aggregates all pages and term occurrences into one cohesive summary.
        
        Args:
            school_name: Name of the school (or district if district-level)
            district_name: Name of the district
            search_results: Dictionary with search results from term_searcher
            page_content_map: Optional dictionary mapping URLs to full page content
            is_district_level: True if this is a district/county-level summary (no school name)
        
        Returns:
            Single cohesive summary string or None if error
        """
        if not self.api_key:
            return None
        
        # Get all page URLs
        page_urls = list(set(search_results.get('page_urls', [])))
        
        # Check cache first
        cache_hash = self._get_school_cache_hash(school_name, district_name, page_urls)
        cached = self._get_cached_summary(cache_hash)
        if cached:
            logger.debug(f"Using cached school summary for {school_name} (hash: {cache_hash[:8]}...)")
            return cached.get('ai_summary')
        
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
        
        # Limit total content to avoid token limits (keep first 15000 chars)
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
        
        # Build the prompt
        user_content = f"""Analyze all content from {entity_name} and provide a comprehensive, evidence-based summary of their approach to the key terms found.

{context_note}

Source of findings: {source_context_str}

Key terms found: {terms_str}
URLs where terms were found: {urls_str}

All scraped pages and term occurrences:
{aggregated_content}

Based on all the content from this {'district/county' if is_district_level else 'school/district'}'s website, provide exactly 2 detailed paragraphs in a formal, analytical style:

**Paragraph 1 - Philosophy and Conceptualization:** Analyze the overall philosophy and approach to the key terms found (restorative justice, race equity, discipline practices, etc.). Explain how the {'district/county' if is_district_level else 'school/district'} conceptualizes these terms, what they mean in their context, and how these concepts relate to each other in their policies and practices. Include references to specific documents, policies, or frameworks mentioned (e.g., Student Code of Conduct, district philosophies, mission statements). Discuss their commitment level, integration approach, and any timelines or goals mentioned. Focus on their conceptual understanding and how they frame these ideas as part of their educational approach.

**Paragraph 2 - Infrastructure and Implementation:** Analyze the concrete infrastructure, programs, staffing, and operational systems related to these terms. Provide specific details: What dedicated centers, programs, departments, or academies exist? What are they called? What dedicated positions, coordinators, facilitators, or staff roles are mentioned? Include specific program names, school names (if applicable), timelines for implementation, training schedules, and any other concrete organizational elements. Demonstrate that this is not merely a policy concept but an operationalized system with dedicated personnel and specialized structures. Focus on tangible, implementational elements that show how philosophy translates into practice.

**Writing Style:** Write in a formal, analytical tone similar to academic or policy analysis. Use specific examples, program names, and evidence from the content. Connect philosophy to implementation. Be comprehensive and detailed, showing depth of understanding of their approach.

**Important:** At the end of your response, include:
1. Explicitly state whether the information comes from the school website only, the district website only, or both. Use this format: "Source: [School website only / District website only / Both school and district websites]"
2. If specific sources or documents are referenced, you may note them (e.g., "sources: schools site, sources, etc").
"""
        
        # Format as chat messages
        messages = [
            {
                'role': 'user',
                'content': user_content
            }
            
        ]
        
        logger.debug(f"Requesting unified AI summary for {school_name} / {district_name}")
        result = self._make_request(messages, max_tokens=800)  # More tokens for longer summary
        
        if result:
            logger.debug(f"Received unified AI summary: {result[:100]}...")
            # Cache the result
            self._set_cached_summary(cache_hash, {
                'school_name': school_name,
                'district_name': district_name,
                'page_urls': page_urls,
                'ai_summary': result
            })
        
        return result


def get_ai_contextualization(search_results: Dict, page_content_map: Optional[Dict[str, str]] = None,
                            school_name: Optional[str] = None, district_name: Optional[str] = None,
                            mode: Optional[str] = None) -> Dict:
    """
    Get AI contextualization for search results.
    Supports two modes:
    - "unified": One cohesive summary per school (default)
    - "per_term": Individual summaries for each term/context (legacy)
    
    Args:
        search_results: Dictionary with search results from term_searcher
        page_content_map: Optional dictionary mapping URLs to full page content
        school_name: Name of the school (required for unified mode)
        district_name: Name of the district (required for unified mode)
        mode: "unified" or "per_term" (defaults to config or "unified")
    
    Returns:
        For unified mode: Dictionary with single 'summary' key containing the summary string
        For per_term mode: Dictionary with AI summaries for each term/context
    """
    from config import PERPLEXITY_CONFIG
    
    client = PerplexityClient()
    
    if not client.api_key:
        logger.warning("Perplexity API key not configured. Skipping AI contextualization.")
        return {}
    
    # Determine mode
    if mode is None:
        mode = PERPLEXITY_CONFIG.get('ai_summary_mode', 'unified')
    
    if mode == 'unified':
        # Unified mode: one summary per school/district
        if not school_name and not district_name:
            logger.warning("School or district name required for unified mode. Falling back to per_term mode.")
            mode = 'per_term'
        else:
            # Use district as school if school missing
            effective_school = school_name or district_name or 'Unknown'
            is_district_level = not school_name and district_name
            summary = client.contextualize_school_approach(
                effective_school,
                district_name or school_name or 'Unknown',
                search_results,
                page_content_map,
                is_district_level=is_district_level
            )
            if summary:
                return {'summary': summary}
            else:
                return {}
    
    # Per-term mode (legacy)
    if mode == 'per_term':
        return client.contextualize_multiple_terms(search_results, page_content_map)
    
    # Unknown mode, default to per_term
    logger.warning(f"Unknown AI summary mode: {mode}. Using per_term mode.")
    return client.contextualize_multiple_terms(search_results, page_content_map)


if __name__ == "__main__":
    # Test the AI client
    from utils.logging_config import setup_logging
    
    setup_logging()
    
    # Test with sample data
    test_context = "Our school district is committed to implementing restorative justice practices in all our schools."
    
    client = PerplexityClient()
    summary = client.contextualize_term(
        "restorative justice",
        test_context,
        "https://example.com/policy"
    )
    
    if summary:
        print(f"\nAI Summary:\n{summary}")
    else:
        print("\nAI contextualization not available (API key may be missing)")

