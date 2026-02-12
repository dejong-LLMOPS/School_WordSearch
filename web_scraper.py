"""Web scraping module for school and district websites."""
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Set, Optional
from pathlib import Path
import json
import threading
from datetime import datetime
from config import SCRAPING_CONFIG, CACHE_DIR, SEARCH_TERMS
import re
import ssl
import urllib3

logger = logging.getLogger(__name__)

# Try to import cloudscraper for bot protection bypass
try:
    import cloudscraper
    CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    CLOUDSCRAPER_AVAILABLE = False
    logger.warning("cloudscraper not available - bot protection bypass disabled. Install with: pip install cloudscraper")

# #region agent log
import json
def _debug_log(location, message, data=None, hypothesis_id=None, run_id="run1"):
    try:
        import time
        with open('/home/ndejong/CPF_Projects/School_WordSearch/.cursor/debug.log', 'a') as f:
            log_entry = {
                "runId": run_id,
                "hypothesisId": hypothesis_id,
                "location": location,
                "message": message,
                "data": data or {},
                "timestamp": int(time.time() * 1000)
            }
            f.write(json.dumps(log_entry) + '\n')
            f.flush()  # Ensure it's written immediately
    except Exception as e:
        # Log to stderr so we can see if there's an issue
        import sys
        print(f"DEBUG LOG ERROR: {e}", file=sys.stderr)
# #endregion


class URLCache:
    """
    Thread-safe URL-level cache for storing scraped pages.
    Allows sharing scraped content across schools that use the same URLs.
    """
    
    def __init__(self):
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._normalizer = None  # Will be set to WebScraper instance's normalizer
    
    def set_normalizer(self, normalizer_func):
        """Set the URL normalization function to use."""
        self._normalizer = normalizer_func
    
    def normalize_key(self, url: str) -> str:
        """Normalize URL for use as cache key."""
        if self._normalizer:
            return self._normalizer(url)
        # Fallback normalization
        try:
            parsed = urlparse(url.lower().rstrip('/'))
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}"
        except:
            return url.lower().rstrip('/')
    
    def get(self, url: str) -> Optional[List[Dict]]:
        """Get cached pages for a URL (thread-safe)."""
        key = self.normalize_key(url)
        with self._lock:
            if key in self._cache:
                entry = self._cache[key]
                logger.debug(f"URL cache hit: {url} (normalized: {key})")
                return entry.get('pages', [])
        return None
    
    def set(self, url: str, pages: List[Dict], school_id: Optional[str] = None) -> None:
        """Cache pages for a URL (thread-safe)."""
        key = self.normalize_key(url)
        with self._lock:
            if key not in self._cache:
                self._cache[key] = {
                    'pages': pages,
                    'scraped_at': datetime.now().isoformat(),
                    'schools': set()
                }
            if school_id:
                self._cache[key]['schools'].add(school_id)
            logger.debug(f"Cached {len(pages)} pages for URL: {key}")
    
    def get_schools_for_url(self, url: str) -> Set[str]:
        """Get set of school IDs that use this URL."""
        key = self.normalize_key(url)
        with self._lock:
            if key in self._cache:
                return self._cache[key].get('schools', set())
        return set()
    
    def clear(self) -> None:
        """Clear the cache (thread-safe)."""
        with self._lock:
            self._cache.clear()
    
    def size(self) -> int:
        """Get number of cached URLs."""
        with self._lock:
            return len(self._cache)


# Global URL cache instance (shared across all scrapers)
_global_url_cache = URLCache()


class WebScraper:
    """Web scraper with rate limiting, error handling, and link following."""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the web scraper.
        
        Args:
            config: Optional configuration dictionary (defaults to SCRAPING_CONFIG)
        """
        self.config = config or SCRAPING_CONFIG
        self.session = requests.Session()
        
        # Configure SSL verification
        self.verify_ssl = self.config.get('verify_ssl', True)
        self.ssl_fallback = self.config.get('ssl_fallback_unverified', True)
        self.ssl_retry_unverified = self.config.get('ssl_retry_with_unverified', True)
        
        # Disable SSL warnings if we're using unverified connections (user choice)
        if not self.verify_ssl or self.ssl_fallback:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Set headers to mimic a real browser
        self.session.headers.update({
            'User-Agent': self.config.get('user_agent', 'Mozilla/5.0'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        # Create cloudscraper session as fallback for bot protection
        self.cloudscraper_session = None
        self._cloudscraper_init_failed = False  # Track if initialization failed
        # #region agent log
        _debug_log("web_scraper.py:162", "Checking cloudscraper availability", {"CLOUDSCRAPER_AVAILABLE": CLOUDSCRAPER_AVAILABLE, "verify_ssl": self.verify_ssl}, "A")
        # #endregion
        if CLOUDSCRAPER_AVAILABLE:
            try:
                # #region agent log
                _debug_log("web_scraper.py:166", "Before cloudscraper.create_scraper call", {"verify_ssl": self.verify_ssl, "cloudscraper_version": getattr(cloudscraper, '__version__', 'unknown'), "cloudscraper_module": str(type(cloudscraper))}, "B")
                # #endregion
                # Configure cloudscraper - verify parameter is NOT accepted by create_scraper()
                # Instead, we'll set verify on the session after creation or pass it to individual requests
                # #region agent log
                _debug_log("web_scraper.py:170", "Attempting to create cloudscraper session without verify parameter", {"browser_config": {"browser": "chrome", "platform": "windows", "desktop": True}}, "C")
                # #endregion
                self.cloudscraper_session = cloudscraper.create_scraper(
                    browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'desktop': True
                    }
                )
                # #region agent log
                _debug_log("web_scraper.py:179", "cloudscraper session created, setting verify attribute", {"session_exists": self.cloudscraper_session is not None, "verify_ssl": self.verify_ssl, "session_has_verify_attr": hasattr(self.cloudscraper_session, 'verify') if self.cloudscraper_session else False}, "D")
                # #endregion
                # Set verify on the session object after creation
                if self.cloudscraper_session:
                    self.cloudscraper_session.verify = self.verify_ssl
                    # #region agent log
                    _debug_log("web_scraper.py:184", "Set verify on cloudscraper session", {"verify_set": hasattr(self.cloudscraper_session, 'verify'), "verify_value": getattr(self.cloudscraper_session, 'verify', None)}, "D")
                    # #endregion
                # #region agent log
                _debug_log("web_scraper.py:187", "cloudscraper session created successfully", {"session_exists": self.cloudscraper_session is not None, "session_type": str(type(self.cloudscraper_session))}, "A")
                # #endregion
            except Exception as e:
                self._cloudscraper_init_failed = True
                logger.warning(f"Failed to initialize cloudscraper: {e}")
                # #region agent log
                _debug_log("web_scraper.py:192", "cloudscraper initialization failed", {"error": str(e), "error_type": type(e).__name__}, "A")
                # #endregion
                import traceback
                # #region agent log
                _debug_log("web_scraper.py:196", "cloudscraper initialization traceback", {"traceback": traceback.format_exc()}, "A")
                # #endregion
        else:
            # #region agent log
            _debug_log("web_scraper.py:200", "cloudscraper not available, skipping initialization", {}, "A")
            # #endregion
        self.visited_urls: Set[str] = set()
        self.scraped_pages: List[Dict] = []
        # Adaptive delay tracking
        self.current_delay = self.config.get('delay', 0.5)
        self.min_delay = self.config.get('min_delay', 0.3)
        self.max_delay = self.config.get('max_delay', 2.0)
        self.consecutive_errors = 0
        self.consecutive_successes = 0
        # Set up URL cache normalizer
        _global_url_cache.set_normalizer(self.normalize_url_for_cache)
        
    def is_valid_url(self, url: str) -> bool:
        """Check if URL is valid and should be scraped."""
        if not url or url.startswith('mailto:') or url.startswith('tel:'):
            return False
        
        # Check if it looks like an email address (contains @ but no scheme)
        if '@' in url and not url.startswith(('http://', 'https://', 'mailto:')):
            return False
        
        try:
            parsed = urlparse(url)
            # Must have a scheme and netloc
            if not parsed.scheme or not parsed.netloc:
                return False
            # Scheme must be http or https
            if parsed.scheme not in ['http', 'https']:
                return False
            # Netloc should not be just an email address
            if '@' in parsed.netloc and not parsed.netloc.startswith('['):
                return False
            return True
        except Exception:
            return False
    
    def normalize_url(self, url: str, base_url: str) -> str:
        """Normalize and resolve relative URLs."""
        try:
            return urljoin(base_url, url).split('#')[0].rstrip('/')
        except Exception:
            return url
    
    def normalize_url_for_cache(self, url: str) -> str:
        """
        Normalize URL for caching (canonical form).
        Handles http/https, trailing slashes, www, etc.
        
        Args:
            url: URL to normalize
        
        Returns:
            Normalized canonical URL
        """
        if not url:
            return ''
        
        try:
            parsed = urlparse(url)
            # Normalize scheme (prefer https)
            scheme = parsed.scheme.lower() if parsed.scheme else 'https'
            if scheme not in ['http', 'https']:
                scheme = 'https'
            
            # Normalize netloc (lowercase, handle www)
            netloc = parsed.netloc.lower() if parsed.netloc else ''
            # Remove www. prefix for consistency (optional - you may want to keep it)
            # netloc = netloc.replace('www.', '') if netloc.startswith('www.') else netloc
            
            # Normalize path (remove trailing slash, lowercase)
            path = parsed.path.rstrip('/') if parsed.path != '/' else '/'
            
            # Reconstruct URL
            normalized = f"{scheme}://{netloc}{path}"
            if parsed.query:
                normalized += f"?{parsed.query}"
            
            return normalized
        except Exception as e:
            logger.warning(f"Error normalizing URL {url}: {e}")
            return url.lower().rstrip('/')
    
    def is_same_domain(self, url1: str, url2: str) -> bool:
        """Check if two URLs are from the same domain."""
        try:
            domain1 = urlparse(url1).netloc
            domain2 = urlparse(url2).netloc
            return domain1 == domain2
        except Exception:
            return False
    
    def should_skip_url(self, url: str) -> bool:
        """
        Check if a URL should be skipped based on patterns.
        
        Args:
            url: URL to check
        
        Returns:
            True if URL should be skipped
        """
        skip_patterns = self.config.get('skip_url_patterns', [])
        # Check URL in lowercase for case-insensitive matching
        url_lower = url.lower()
        for pattern in skip_patterns:
            if re.search(pattern, url_lower, re.IGNORECASE):
                logger.debug(f"Skipping URL (matches pattern {pattern}): {url}")
                return True
        return False
    
    def has_relevant_content(self, text: str, search_terms: Optional[List[str]] = None) -> bool:
        """
        Quick check if text contains any of the search terms.
        Used to decide whether to follow links from a page.
        
        Args:
            text: Text content to check
            search_terms: Optional list of terms (defaults to SEARCH_TERMS)
        
        Returns:
            True if text contains at least one search term
        """
        if not text or len(text) < 50:  # Too short to be relevant
            return False
        
        terms = search_terms or SEARCH_TERMS
        text_lower = text.lower()
        
        for term in terms:
            if term.lower() in text_lower:
                return True
        return False
    
    def is_minimal_content_page(self, text: str) -> bool:
        """
        Check if page has minimal content (likely a directory/navigation page).
        
        Args:
            text: Text content to check
        
        Returns:
            True if page has minimal content
        """
        min_length = self.config.get('min_content_length', 200)
        return len(text) < min_length
    
    def is_bot_protection_page(self, html_content: str) -> bool:
        """Check if the page is a bot protection challenge."""
        # #region agent log
        _debug_log("web_scraper.py:296", "is_bot_protection_page called", {"content_length": len(html_content) if html_content else 0, "has_content": bool(html_content)}, "B")
        # #endregion
        if not html_content:
            # #region agent log
            _debug_log("web_scraper.py:300", "Empty content detected, returning True for bot protection", {}, "B")
            # #endregion
            return True  # Empty content might indicate bot protection
        
        try:
            content_lower = html_content.lower()
        except (AttributeError, UnicodeDecodeError) as e:
            logger.debug(f"Error converting content to lowercase for bot check: {e}")
            # #region agent log
            _debug_log("web_scraper.py:307", "Error converting to lowercase", {"error": str(e)}, "B")
            # #endregion
            return False
        
        indicators = [
            'client challenge', 'javascript challenge', 'cloudflare',
            'ddos protection', 'checking your browser', 'please enable javascript',
            'cf-browser-verification', 'challenge-platform', 'cf-ray',
            'just a moment', 'enable javascript and cookies', 'bot protection',
            'client challenge javascript is disabled'
        ]
        
        # Check indicators
        found_indicators = [ind for ind in indicators if ind in content_lower]
        # #region agent log
        _debug_log("web_scraper.py:321", "Indicator check", {"found_indicators": found_indicators, "indicator_count": len(found_indicators)}, "B")
        # #endregion
        
        # Also check for suspiciously short content with no links (might be bot protection)
        if len(html_content) < 1000:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')  # Use html.parser as fallback
                links = soup.find_all('a', href=True)
                # #region agent log
                _debug_log("web_scraper.py:328", "Short content check", {"content_length": len(html_content), "link_count": len(links), "has_early_indicators": any(indicator in content_lower for indicator in indicators[:5])}, "B")
                # #endregion
                if len(links) == 0 and any(indicator in content_lower for indicator in indicators[:5]):
                    # #region agent log
                    _debug_log("web_scraper.py:332", "Bot protection detected: short content with no links and early indicators", {}, "B")
                    # #endregion
                    return True
            except Exception as e:
                logger.debug(f"Error parsing HTML for bot protection check: {e}")
                # #region agent log
                _debug_log("web_scraper.py:337", "Error parsing HTML for bot check", {"error": str(e)}, "B")
                # #endregion
                # If we can't parse, check indicators anyway
                pass
        
        result = any(indicator in content_lower for indicator in indicators)
        # #region agent log
        _debug_log("web_scraper.py:344", "Final bot protection check result", {"is_bot_protection": result, "found_indicators": found_indicators}, "B")
        # #endregion
        return result
    
    def extract_text(self, html_content: str) -> str:
        """Extract text content from HTML."""
        if not html_content:
            return ""
        
        try:
            # Try parsing with lxml first (faster)
            try:
                soup = BeautifulSoup(html_content, 'lxml')
            except Exception as lxml_error:
                # Fallback to html.parser if lxml fails
                logger.debug(f"lxml parser failed, trying html.parser: {lxml_error}")
                try:
                    soup = BeautifulSoup(html_content, 'html.parser')
                except Exception as parser_error:
                    logger.warning(f"HTML parsing failed with both parsers: {parser_error}")
                    return ""
            
            # Remove script and style elements
            try:
                for script in soup(["script", "style", "nav", "footer", "header"]):
                    script.decompose()
            except Exception as e:
                logger.debug(f"Error removing script/style elements: {e}")
                # Continue anyway
            
            # Get text
            try:
                text = soup.get_text(separator=' ', strip=True)
            except Exception as e:
                logger.debug(f"Error getting text from soup: {e}")
                return ""
            
            # Clean up whitespace
            try:
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
            except Exception as e:
                logger.debug(f"Error cleaning whitespace: {e}")
                # Return text as-is if cleaning fails
            
            return text
        except UnicodeDecodeError as e:
            logger.debug(f"Unicode decode error extracting text: {e}")
            # Try with error handling
            try:
                html_content = html_content.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
                soup = BeautifulSoup(html_content, 'html.parser')
                return soup.get_text(separator=' ', strip=True)
            except Exception:
                return ""
        except Exception as e:
            logger.warning(f"Unexpected error extracting text: {e}")
            return ""
    
    def extract_links(self, html_content: str, base_url: str, base_domain: Optional[str] = None) -> List[str]:
        """
        Extract internal links from HTML.
        
        Args:
            html_content: HTML content to parse
            base_url: Base URL for resolving relative links
            base_domain: Original base domain to validate against (if None, uses base_url's domain)
        """
        links = []
        if not html_content:
            return []
        
        try:
            # Use base_domain if provided, otherwise extract from base_url
            if base_domain is None:
                try:
                    base_domain = urlparse(base_url).netloc
                except Exception as e:
                    logger.debug(f"Error parsing base_url domain: {e}")
                    base_domain = None
            
            # Try parsing with lxml first, fallback to html.parser
            try:
                soup = BeautifulSoup(html_content, 'lxml')
            except Exception as lxml_error:
                logger.debug(f"lxml parser failed for links, trying html.parser: {lxml_error}")
                try:
                    soup = BeautifulSoup(html_content, 'html.parser')
                except Exception as parser_error:
                    logger.warning(f"HTML parsing failed for links: {parser_error}")
                    return []
            
            for tag in soup.find_all('a', href=True):
                try:
                    href = tag.get('href', '')
                    if not href:
                        continue
                    
                    # Skip mailto: and tel: links
                    if href.startswith('mailto:') or href.startswith('tel:'):
                        continue
                    
                    # Normalize URL
                    try:
                        full_url = self.normalize_url(href, base_url)
                    except Exception as e:
                        logger.debug(f"Error normalizing URL {href}: {e}")
                        continue
                    
                    # Validate URL and check domain
                    if not self.is_valid_url(full_url):
                        continue
                    
                    # Check against base_domain if available, otherwise use base_url
                    if base_domain:
                        try:
                            link_domain = urlparse(full_url).netloc
                            if link_domain != base_domain:
                                logger.debug(f"Skipping external link: {full_url} (base domain: {base_domain})")
                                continue
                        except Exception as e:
                            logger.debug(f"Error parsing link domain: {e}")
                            continue
                    elif not self.is_same_domain(full_url, base_url):
                        continue
                    
                    links.append(full_url)
                except Exception as e:
                    logger.debug(f"Error processing link tag: {e}")
                    continue  # Skip this link and continue with others
            
            return list(set(links))  # Remove duplicates
        except UnicodeDecodeError as e:
            logger.debug(f"Unicode decode error extracting links: {e}")
            # Try with error handling
            try:
                html_content = html_content.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
                soup = BeautifulSoup(html_content, 'html.parser')
                # Re-extract with cleaned content (simplified)
                links = []
                for tag in soup.find_all('a', href=True):
                    href = tag.get('href', '')
                    if href and not href.startswith(('mailto:', 'tel:')):
                        try:
                            full_url = self.normalize_url(href, base_url)
                            if self.is_valid_url(full_url):
                                if base_domain:
                                    if urlparse(full_url).netloc == base_domain:
                                        links.append(full_url)
                                elif self.is_same_domain(full_url, base_url):
                                    links.append(full_url)
                        except Exception:
                            continue
                return list(set(links))
            except Exception:
                return []
        except Exception as e:
            logger.warning(f"Unexpected error extracting links: {e}")
            return []
    
    def scrape_page(self, url: str, use_cloudscraper_first: bool = False, 
                   add_to_scraped_pages: bool = True, base_domain: Optional[str] = None) -> Optional[Dict]:
        """
        Scrape a single page.
        
        Args:
            url: URL to scrape
            use_cloudscraper_first: If True, try cloudscraper first (useful for known protected sites)
            add_to_scraped_pages: If False, don't add this page to scraped_pages (useful for link-only scraping)
            base_domain: Original base domain to check redirects against (prevents following external redirects)
        
        Returns:
            Dictionary with page data or None if error
        """
        if not self.is_valid_url(url):
            return None
        
        if url in self.visited_urls:
            return None
        
        response = None
        final_url = url
        used_cloudscraper = False
        
        # Helper function to check if a URL is on the same domain
        def is_external_redirect(check_url: str) -> bool:
            """Check if a URL redirects to an external domain."""
            if not base_domain:
                return False
            try:
                check_domain = urlparse(check_url).netloc
                return check_domain != base_domain
            except Exception:
                return False
        
        try:
            # Try cloudscraper first if available and requested, or if regular request fails
            if use_cloudscraper_first and self.cloudscraper_session:
                logger.debug(f"Scraping with cloudscraper first: {url}")
                try:
                    # First, check redirects manually to prevent following external domains
                    response = self.cloudscraper_session.get(
                        url,
                        timeout=self.config.get('timeout', 30),
                        allow_redirects=False  # Don't auto-follow, check manually
                    )
                    # Check for redirects
                    if response.status_code in [301, 302, 303, 307, 308]:
                        redirect_url = response.headers.get('Location', '')
                        if redirect_url:
                            # Resolve relative redirects
                            redirect_url = urljoin(url, redirect_url)
                            if is_external_redirect(redirect_url):
                                logger.debug(f"Skipping external redirect: {url} -> {redirect_url} (base: {base_domain})")
                                return None
                            # Follow redirect manually
                            response = self.cloudscraper_session.get(
                                redirect_url,
                                timeout=self.config.get('timeout', 30),
                                allow_redirects=True  # Allow further redirects after first check
                            )
                    response.raise_for_status()
                    final_url = response.url
                    # Final check after all redirects
                    if is_external_redirect(final_url):
                        logger.debug(f"Final URL is external domain: {url} -> {final_url} (base: {base_domain})")
                        return None
                    used_cloudscraper = True
                    logger.debug(f"Successfully scraped with cloudscraper: {url}")
                except requests.exceptions.SSLError as e:
                    # Check if SSL error is from external domain
                    if base_domain and base_domain not in str(e):
                        logger.debug(f"SSL error from external domain (suppressing): {url}")
                        return None
                    # Try with unverified SSL if fallback is enabled
                    if self.ssl_fallback and 'handshake' in str(e).lower():
                        logger.debug(f"SSL handshake failure, trying without verification: {url}")
                        try:
                            response = self.cloudscraper_session.get(
                                url,
                                timeout=self.config.get('timeout', 30),
                                allow_redirects=False,
                                verify=False
                            )
                            if response.status_code in [301, 302, 303, 307, 308]:
                                redirect_url = response.headers.get('Location', '')
                                if redirect_url:
                                    redirect_url = urljoin(url, redirect_url)
                                    if is_external_redirect(redirect_url):
                                        return None
                                    response = self.cloudscraper_session.get(
                                        redirect_url,
                                        timeout=self.config.get('timeout', 30),
                                        allow_redirects=True,
                                        verify=False
                                    )
                            response.raise_for_status()
                            final_url = response.url
                            if is_external_redirect(final_url):
                                return None
                            used_cloudscraper = True
                            logger.debug(f"Successfully scraped with cloudscraper (unverified SSL): {url}")
                        except Exception as fallback_error:
                            logger.debug(f"Cloudscraper SSL fallback also failed: {fallback_error}")
                            response = None
                    else:
                        logger.debug(f"Cloudscraper SSL error, trying regular session: {e}")
                        response = None
                except requests.exceptions.HTTPError as e:
                    # Check if 403/404 is from external domain
                    if e.response and base_domain:
                        try:
                            error_domain = urlparse(e.response.url).netloc
                            if error_domain != base_domain:
                                logger.debug(f"HTTP error from external domain (suppressing): {url} -> {e.response.url}")
                                return None
                        except Exception:
                            pass
                    logger.debug(f"Cloudscraper failed, trying regular session: {e}")
                    response = None
                except Exception as e:
                    logger.debug(f"Cloudscraper failed, trying regular session: {e}")
                    response = None
            
            # Try regular session if cloudscraper wasn't used or failed
            if response is None:
                logger.debug(f"Scraping: {url}")
                try:
                    # First, check redirects manually to prevent following external domains
                    response = self.session.get(
                        url,
                        timeout=self.config.get('timeout', 30),
                        allow_redirects=False  # Don't auto-follow, check manually
                    )
                    # Check for redirects
                    if response.status_code in [301, 302, 303, 307, 308]:
                        redirect_url = response.headers.get('Location', '')
                        if redirect_url:
                            # Resolve relative redirects
                            redirect_url = urljoin(url, redirect_url)
                            if is_external_redirect(redirect_url):
                                logger.debug(f"Skipping external redirect: {url} -> {redirect_url} (base: {base_domain})")
                                return None
                            # Follow redirect manually
                            response = self.session.get(
                                redirect_url,
                                timeout=self.config.get('timeout', 30),
                                allow_redirects=True,  # Allow further redirects after first check
                                verify=self.verify_ssl
                            )
                    response.raise_for_status()
                    final_url = response.url
                    # Final check after all redirects
                    if is_external_redirect(final_url):
                        logger.debug(f"Final URL is external domain: {url} -> {final_url} (base: {base_domain})")
                        return None
                except requests.exceptions.SSLError as e:
                    # Check if SSL error is from external domain
                    if base_domain and base_domain not in str(e):
                        logger.debug(f"SSL error from external domain (suppressing): {url}")
                        return None
                    # Try with unverified SSL if fallback is enabled and it's a handshake failure
                    if self.ssl_fallback and 'handshake' in str(e).lower():
                        logger.debug(f"SSL handshake failure, retrying without verification: {url}")
                        try:
                            response = self.session.get(
                                url,
                                timeout=self.config.get('timeout', 30),
                                allow_redirects=False,
                                verify=False
                            )
                            if response.status_code in [301, 302, 303, 307, 308]:
                                redirect_url = response.headers.get('Location', '')
                                if redirect_url:
                                    redirect_url = urljoin(url, redirect_url)
                                    if is_external_redirect(redirect_url):
                                        return None
                                    response = self.session.get(
                                        redirect_url,
                                        timeout=self.config.get('timeout', 30),
                                        allow_redirects=True,
                                        verify=False
                                    )
                            response.raise_for_status()
                            final_url = response.url
                            if is_external_redirect(final_url):
                                return None
                            logger.debug(f"Successfully scraped with unverified SSL: {url}")
                        except Exception as fallback_error:
                            logger.warning(f"SSL fallback also failed for {url}: {fallback_error}")
                            raise  # Re-raise original SSL error
                    else:
                        raise  # Re-raise if it's from same domain and no fallback
                except requests.exceptions.HTTPError as e:
                    # Check if 403/404 is from external domain
                    if e.response and base_domain:
                        try:
                            error_domain = urlparse(e.response.url).netloc
                            if error_domain != base_domain:
                                logger.debug(f"HTTP error from external domain (suppressing): {url} -> {e.response.url}")
                                return None
                        except Exception:
                            pass
                    raise  # Re-raise if it's from same domain
            
            # Double-check domain after all redirects (safety check)
            if base_domain and is_external_redirect(final_url):
                logger.debug(f"External domain detected after redirects: {url} -> {final_url}")
                return None
            
            # Check if we hit bot protection (even after cloudscraper, some sites might still block)
            try:
                response_text = response.text
            except (UnicodeDecodeError, AttributeError) as e:
                logger.debug(f"Error reading response text for bot protection check: {e}")
                response_text = ""
            
            # #region agent log
            _debug_log("web_scraper.py:703", "Before bot protection check", {"url": url, "used_cloudscraper": used_cloudscraper, "response_text_length": len(response_text) if response_text else 0, "cloudscraper_session_exists": self.cloudscraper_session is not None}, "C")
            # #endregion
            
            is_bot_protection = self.is_bot_protection_page(response_text)
            # #region agent log
            _debug_log("web_scraper.py:708", "Bot protection check result", {"is_bot_protection": is_bot_protection, "used_cloudscraper": used_cloudscraper}, "C")
            # #endregion
            
            if is_bot_protection and not used_cloudscraper:
                logger.warning(f"Bot protection detected on {url}, retrying with cloudscraper...")
                # #region agent log
                _debug_log("web_scraper.py:712", "Bot protection detected, checking cloudscraper", {"cloudscraper_session_exists": self.cloudscraper_session is not None, "CLOUDSCRAPER_AVAILABLE": CLOUDSCRAPER_AVAILABLE}, "C")
                # #endregion
                if self.cloudscraper_session:
                    try:
                        # Retry with cloudscraper
                        response = self.cloudscraper_session.get(
                            url,
                            timeout=self.config.get('timeout', 30),
                            allow_redirects=True
                        )
                        response.raise_for_status()
                        retry_final_url = response.url
                        # Check if retry redirect went to external domain
                        if is_external_redirect(retry_final_url):
                            logger.debug(f"Bot protection retry redirected to external domain: {url} -> {retry_final_url}")
                            return None
                        final_url = retry_final_url
                        used_cloudscraper = True
                        logger.info(f"Successfully bypassed bot protection for {url}")
                    except requests.exceptions.SSLError as e:
                        if base_domain and base_domain not in str(e):
                            logger.debug(f"SSL error from external domain during bot protection retry (suppressing): {url}")
                            return None
                        # Try with unverified SSL if fallback is enabled
                        if self.ssl_fallback and 'handshake' in str(e).lower():
                            logger.debug(f"SSL handshake failure during bot protection retry, trying without verification: {url}")
                            try:
                                response = self.cloudscraper_session.get(
                                    url,
                                    timeout=self.config.get('timeout', 30),
                                    allow_redirects=True,
                                    verify=False
                                )
                                response.raise_for_status()
                                retry_final_url = response.url
                                if is_external_redirect(retry_final_url):
                                    logger.debug(f"Bot protection retry (unverified SSL) redirected to external domain: {url} -> {retry_final_url}")
                                    return None
                                final_url = retry_final_url
                                used_cloudscraper = True
                                logger.info(f"Successfully bypassed bot protection for {url} (unverified SSL)")
                            except Exception as ssl_fallback_error:
                                logger.warning(f"Cloudscraper also failed for {url} (even with unverified SSL): {ssl_fallback_error}")
                                # Continue with original response but mark as potentially incomplete
                        else:
                            logger.warning(f"Cloudscraper also failed for {url}: {e}")
                            # Continue with original response but mark as potentially incomplete
                    except requests.exceptions.HTTPError as e:
                        if e.response and base_domain:
                            try:
                                error_domain = urlparse(e.response.url).netloc
                                if error_domain != base_domain:
                                    logger.debug(f"HTTP error from external domain during bot protection retry (suppressing): {url}")
                                    return None
                            except Exception:
                                pass
                        logger.warning(f"Cloudscraper also failed for {url}: {e}")
                        # Continue with original response but mark as potentially incomplete
                        logger.info(f"Continuing with original response for {url} (bot protection retry failed, but original response may still be usable)")
                    except Exception as e:
                        logger.warning(f"Cloudscraper also failed for {url}: {e}")
                        # Continue with original response but mark as potentially incomplete
                        logger.info(f"Continuing with original response for {url} (bot protection retry failed, but original response may still be usable)")
                else:
                    # cloudscraper is available but session is None - initialization must have failed
                    # Only log once per scraper instance to avoid spam
                    if not hasattr(self, '_cloudscraper_warned'):
                        if CLOUDSCRAPER_AVAILABLE:
                            if self._cloudscraper_init_failed:
                                logger.debug(f"cloudscraper session initialization failed earlier - skipping bot protection bypass")
                            else:
                                logger.debug(f"cloudscraper session not available - skipping bot protection bypass")
                        else:
                            logger.debug(f"cloudscraper not installed - skipping bot protection bypass")
                        self._cloudscraper_warned = True
                    # Continue with original response - it may still be usable
                    logger.info(f"Continuing with original response for {url} (cloudscraper not available, but original response may still be usable)")
                    # #region agent log
                    _debug_log("web_scraper.py:823", "cloudscraper not available warning", {"cloudscraper_session": self.cloudscraper_session is None, "CLOUDSCRAPER_AVAILABLE": CLOUDSCRAPER_AVAILABLE, "init_failed": getattr(self, '_cloudscraper_init_failed', False), "url": url}, "C")
                    # #endregion
            
            # Extract content (handle encoding errors)
            try:
                html_content = response.text
            except UnicodeDecodeError as e:
                logger.debug(f"Unicode decode error reading response text for {url}: {e}")
                # Try with different encoding
                try:
                    response.encoding = response.apparent_encoding or 'utf-8'
                    html_content = response.text
                except Exception as e2:
                    logger.warning(f"Failed to decode response text for {url}: {e2}")
                    return None
            
            # Extract text and links
            text_content = self.extract_text(html_content)
            # Always extract links using the original base domain for validation, not the redirected URL
            links = self.extract_links(html_content, final_url, base_domain=base_domain)
            
            # Log successful scraping (even if bot protection was detected)
            if len(text_content) > 0:
                logger.info(f"✓ Successfully scraped {url} ({len(text_content)} chars, {len(links)} links)")
            else:
                logger.warning(f"⚠ Scraped {url} but extracted no text content")
            
            logger.debug(f"Extracted {len(links)} links from {final_url} (content length: {len(text_content)})")
            if len(links) == 0:
                logger.debug(f"No links found on page - may be a bot protection page or empty page")
            elif links and len(links) > 0:
                logger.debug(f"Sample links: {links[:5]}")
            
            page_data = {
                'url': final_url,  # Use final URL after redirects
                'text': text_content,
                'links': links,
                'status_code': response.status_code,
                'content_length': len(text_content)
            }
            
            # Mark both original and final URL as visited
            self.visited_urls.add(url)
            if final_url != url:
                self.visited_urls.add(final_url)
            
            # Only add to scraped_pages if requested (default True for normal scraping)
            if add_to_scraped_pages:
                self.scraped_pages.append(page_data)
            
            return page_data
            
        except requests.exceptions.SSLError as e:
            # Check if SSL error is from external domain
            if base_domain and base_domain not in str(e):
                logger.debug(f"SSL error from external domain (suppressing): {url}")
                return None
            # Try with unverified SSL as last resort if enabled
            if self.ssl_retry_unverified and 'handshake' in str(e).lower():
                logger.debug(f"SSL handshake failure, trying unverified SSL as last resort: {url}")
                try:
                    response = self.session.get(
                        url,
                        timeout=self.config.get('timeout', 30),
                        allow_redirects=True,
                        verify=False
                    )
                    response.raise_for_status()
                    final_url = response.url
                    if base_domain and is_external_redirect(final_url):
                        return None
                    # Success - extract content
                    try:
                        html_content = response.text
                    except UnicodeDecodeError:
                        response.encoding = response.apparent_encoding or 'utf-8'
                        html_content = response.text
                    text_content = self.extract_text(html_content)
                    links = self.extract_links(html_content, final_url, base_domain=base_domain)
                    page_data = {
                        'url': final_url,
                        'text': text_content,
                        'links': links,
                        'status_code': response.status_code,
                        'content_length': len(text_content)
                    }
                    self.visited_urls.add(url)
                    if final_url != url:
                        self.visited_urls.add(final_url)
                    if add_to_scraped_pages:
                        self.scraped_pages.append(page_data)
                    logger.debug(f"Successfully scraped with unverified SSL (last resort): {url}")
                    return page_data
                except Exception as last_resort_error:
                    logger.warning(f"SSL last resort attempt also failed for {url}: {last_resort_error}")
            logger.warning(f"SSL certificate error scraping {url}: {e}")
            return None
        except requests.exceptions.HTTPError as e:
            # Check if HTTP error is from external domain
            if e.response and base_domain:
                try:
                    error_domain = urlparse(e.response.url).netloc
                    if error_domain != base_domain:
                        logger.debug(f"HTTP error from external domain (suppressing): {url} -> {e.response.url}")
                        return None
                except Exception:
                    pass
            # Log specific HTTP status codes
            status_code = e.response.status_code if e.response else None
            if status_code == 403:
                logger.debug(f"403 Forbidden (access denied) for {url}")
            elif status_code == 404:
                logger.debug(f"404 Not Found for {url}")
            elif status_code == 429:
                logger.warning(f"429 Too Many Requests (rate limited) for {url}")
            elif status_code == 500:
                logger.warning(f"500 Internal Server Error for {url}")
            elif status_code == 503:
                logger.warning(f"503 Service Unavailable for {url}")
            else:
                logger.warning(f"HTTP error ({status_code}) scraping {url}: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            # Network connectivity issues, DNS resolution, etc.
            if base_domain and base_domain not in str(e):
                logger.debug(f"Connection error from external domain (suppressing): {url}")
                return None
            # Check for specific connection error types
            error_str = str(e).lower()
            if 'dns' in error_str or 'name resolution' in error_str or 'nodename nor servname' in error_str:
                logger.debug(f"DNS resolution failed for {url}: {e}")
            elif 'refused' in error_str or 'connection refused' in error_str:
                logger.debug(f"Connection refused for {url} (server may be down)")
            elif 'reset' in error_str or 'connection reset' in error_str:
                logger.debug(f"Connection reset for {url}")
            else:
                logger.warning(f"Connection error scraping {url}: {e}")
            return None
        except requests.exceptions.Timeout as e:
            # Request timeout
            if base_domain and base_domain not in str(e):
                logger.debug(f"Timeout from external domain (suppressing): {url}")
                return None
            logger.warning(f"Request timeout for {url} (exceeded {self.config.get('timeout', 30)}s)")
            return None
        except requests.exceptions.TooManyRedirects as e:
            # Redirect loop detected
            if base_domain and base_domain not in str(e):
                logger.debug(f"Too many redirects from external domain (suppressing): {url}")
                return None
            logger.warning(f"Too many redirects (redirect loop) for {url}: {e}")
            return None
        except requests.exceptions.InvalidURL as e:
            # Invalid URL format
            logger.debug(f"Invalid URL format: {url}: {e}")
            return None
        except requests.exceptions.MissingSchema as e:
            # Missing URL scheme (http/https)
            logger.debug(f"Missing URL scheme for {url}: {e}")
            return None
        except requests.exceptions.InvalidSchema as e:
            # Invalid URL scheme
            logger.debug(f"Invalid URL scheme for {url}: {e}")
            return None
        except requests.exceptions.ChunkedEncodingError as e:
            # Incomplete chunked response
            if base_domain and base_domain not in str(e):
                logger.debug(f"Chunked encoding error from external domain (suppressing): {url}")
                return None
            logger.debug(f"Incomplete chunked response for {url}: {e}")
            return None
        except requests.exceptions.ContentDecodingError as e:
            # Content decompression error
            if base_domain and base_domain not in str(e):
                logger.debug(f"Content decoding error from external domain (suppressing): {url}")
                return None
            logger.debug(f"Content decompression error for {url}: {e}")
            return None
        except UnicodeDecodeError as e:
            # Encoding error when reading response
            logger.debug(f"Unicode decode error for {url}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            # Generic request exception (catch-all for other request errors)
            if base_domain and base_domain not in str(e):
                logger.debug(f"Request error from external domain (suppressing): {url}")
                return None
            logger.warning(f"Request error scraping {url}: {e}")
            return None
        except Exception as e:
            # Unexpected errors (parsing, etc.)
            error_type = type(e).__name__
            logger.error(f"Unexpected {error_type} error scraping {url}: {e}")
            return None
    
    def scrape_site(self, base_url: str, max_depth: Optional[int] = None, 
                   max_pages: Optional[int] = None) -> List[Dict]:
        """
        Scrape a website following internal links.
        
        Args:
            base_url: Base URL to start scraping from
            max_depth: Maximum depth to follow links (defaults to config)
            max_pages: Maximum pages to scrape (defaults to config)
        
        Returns:
            List of scraped page data dictionaries
        """
        if not self.is_valid_url(base_url):
            logger.warning(f"Invalid base URL: {base_url}")
            return []
        
        max_depth = max_depth or self.config.get('max_depth', 3)
        max_pages = max_pages or self.config.get('max_pages_per_site', 50)
        
        self.visited_urls.clear()
        self.scraped_pages.clear()
        
        # Store the original base domain to prevent following external links
        try:
            base_domain = urlparse(base_url).netloc
        except Exception:
            logger.warning(f"Could not parse base domain from {base_url}")
            return []
        
        # Queue of URLs to scrape: (url, depth)
        url_queue = [(base_url, 0)]
        
        logger.info(f"Starting to scrape site: {base_url} (max_depth={max_depth}, max_pages={max_pages}, base_domain={base_domain})")
        
        while url_queue and len(self.scraped_pages) < max_pages:
            current_url, depth = url_queue.pop(0)
            
            # Skip if already visited or too deep
            if current_url in self.visited_urls or depth > max_depth:
                continue
            
            # Check if this URL matches skip patterns (we'll still scrape it to get links, but won't include content)
            should_skip_content = self.should_skip_url(current_url) and depth > 0
            
            # Use cloudscraper first for the base URL (depth 0) as many school sites have protection
            use_cloudscraper_first = (depth == 0 and self.cloudscraper_session is not None)
            
            # Check if current URL is on the same domain (prevent following external links)
            try:
                current_domain = urlparse(current_url).netloc
                if current_domain != base_domain:
                    logger.debug(f"Skipping external domain URL: {current_url} (base domain: {base_domain})")
                    continue
            except Exception as e:
                logger.warning(f"Error parsing domain from {current_url}: {e}")
                continue
            
            # Scrape the page (even if it matches skip patterns, we need to extract links)
            # If it matches skip patterns, don't add to scraped_pages (won't search its content)
            add_to_scraped = not should_skip_content
            page_data = self.scrape_page(current_url, use_cloudscraper_first=use_cloudscraper_first, 
                                        add_to_scraped_pages=add_to_scraped, base_domain=base_domain)
            
            if page_data:
                if should_skip_content:
                    logger.debug(f"Scraped URL for links only (matches skip pattern, won't search content): {current_url}")
                # Adaptive delay: increase on errors, decrease on success
                self.consecutive_successes += 1
                self.consecutive_errors = 0
                
                # Gradually reduce delay after successful requests
                if self.consecutive_successes > 5 and self.current_delay > self.min_delay:
                    self.current_delay = max(self.min_delay, self.current_delay * 0.9)
                    self.consecutive_successes = 0
                
                if self.current_delay > 0:
                    time.sleep(self.current_delay)
                
                # Check if we should follow links from this page
                text_content = page_data.get('text', '')
                should_follow_links = True
                
                # For pages matching skip patterns, always follow links (even if minimal content)
                # since we're scraping them specifically to discover links
                if not should_skip_content:
                    # Quick content filtering to speed up scraping
                    if self.config.get('quick_content_check', True):
                        # Skip following links from pages with minimal content (directory pages)
                        # UNLESS they contain relevant content (short but relevant pages are worth following)
                        is_minimal = self.is_minimal_content_page(text_content)
                        has_relevant = self.has_relevant_content(text_content)
                        
                        if is_minimal and not has_relevant:
                            logger.debug(f"Skipping links from minimal content page: {current_url} ({len(text_content)} chars)")
                            should_follow_links = False
                        # Optionally require relevant content before following links
                        elif self.config.get('require_relevant_content', False):
                            if not has_relevant:
                                logger.debug(f"Skipping links from page without relevant content: {current_url}")
                                should_follow_links = False
                else:
                    # For skipped pages, always follow links to discover new pages
                    logger.debug(f"Following links from skipped page (for link discovery): {current_url}")
                
                # Add links to queue if not too deep and page passes filters
                if depth < max_depth and should_follow_links:
                    links_added = 0
                    skipped_links = 0
                    for link in page_data.get('links', []):
                        if link not in self.visited_urls:
                            # Skip URLs that match skip patterns
                            if self.should_skip_url(link):
                                skipped_links += 1
                                continue
                            url_queue.append((link, depth + 1))
                            links_added += 1
                    if links_added > 0:
                        logger.debug(f"Added {links_added} links to queue (depth {depth}, queue size: {len(url_queue)}, skipped: {skipped_links})")
                elif not should_follow_links:
                    logger.debug(f"Not following links from {current_url} (filtered out)")
            else:
                # Increase delay on errors (rate limiting, timeouts, etc.)
                self.consecutive_errors += 1
                self.consecutive_successes = 0
                
                if self.consecutive_errors > 2:
                    self.current_delay = min(self.max_delay, self.current_delay * 1.5)
                    logger.debug(f"Increased delay to {self.current_delay:.2f}s due to errors")
            
            # Break if we've reached max pages
            if len(self.scraped_pages) >= max_pages:
                break
        
        logger.info(f"Scraped {len(self.scraped_pages)} pages from {base_url}")
        return self.scraped_pages
    
    def save_cache(self, cache_key: str, data: List[Dict]) -> None:
        """Save scraped data to cache."""
        cache_file = CACHE_DIR / f"{cache_key}.json"
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.debug(f"Cached data to {cache_file}")
        except Exception as e:
            logger.warning(f"Error saving cache: {e}")
    
    def load_cache(self, cache_key: str) -> Optional[List[Dict]]:
        """Load scraped data from cache."""
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.debug(f"Loaded cache from {cache_file}")
                return data
            except Exception as e:
                logger.warning(f"Error loading cache: {e}")
        return None


def scrape_school_urls(school_url: Optional[str], district_url: Optional[str],
                       use_cache: bool = True, school_id: Optional[str] = None) -> List[Dict]:
    """
    Scrape both school and district URLs if available.
    Uses URL-level caching to avoid re-scraping the same URLs across schools.
    Marks each page with its source (school or district).
    
    Args:
        school_url: School website URL
        district_url: District website URL
        use_cache: Whether to use cached results if available
        school_id: Optional school identifier for tracking which schools use which URLs
    
    Returns:
        List of all scraped pages, each with a 'source' field ('school' or 'district')
    """
    scraper = WebScraper()
    all_pages = []
    
    # Normalize URLs for comparison
    def normalize_url(url):
        if not url:
            return None
        return url.rstrip('/').lower()
    
    school_url_norm = normalize_url(school_url)
    district_url_norm = normalize_url(district_url)
    
    # Check URL-level cache first (most efficient - shared across all schools)
    urls_to_scrape = []
    cached_pages = []
    
    if school_url:
        cached = _global_url_cache.get(school_url)
        if cached:
            logger.debug(f"Using URL cache for school URL: {school_url}")
            # Mark cached pages as from school
            for page in cached:
                page['source'] = 'school'
            cached_pages.extend(cached)
            _global_url_cache.set(school_url, cached, school_id)
        else:
            urls_to_scrape.append(('school', school_url))
    
    if district_url and district_url_norm != school_url_norm:
        cached = _global_url_cache.get(district_url)
        if cached:
            logger.debug(f"Using URL cache for district URL: {district_url}")
            # Mark cached pages as from district
            for page in cached:
                page['source'] = 'district'
            cached_pages.extend(cached)
            _global_url_cache.set(district_url, cached, school_id)
        else:
            urls_to_scrape.append(('district', district_url))
    
    # If we got everything from cache, return early
    if not urls_to_scrape and cached_pages:
        logger.info(f"All URLs found in cache for {school_url or district_url}")
        return cached_pages
    
    # Scrape URLs that weren't in cache
    for url_type, url in urls_to_scrape:
        if url:
            logger.debug(f"Scraping {url_type} URL (not in cache): {url}")
            pages = scraper.scrape_site(url)
            # Mark pages with their source
            for page in pages:
                page['source'] = url_type
            all_pages.extend(pages)
            # Cache the scraped pages
            _global_url_cache.set(url, pages, school_id)
            # Also reset visited URLs for new domain if switching
            if url_type == 'district':
                scraper.visited_urls.clear()
    
    # Combine cached and newly scraped pages
    all_pages = cached_pages + all_pages
    
    # Optional per-school file cache (disabled by default - URL-level cache is more efficient)
    if all_pages and scraper.config.get('save_file_cache', False):
        cache_key = f"school_{hash(school_url or '')}_{hash(district_url or '')}"
        scraper.save_cache(cache_key, all_pages)
    
    return all_pages


if __name__ == "__main__":
    # Test the scraper
    from utils.logging_config import setup_logging
    
    setup_logging(log_level=logging.DEBUG)
    
    # Test with a simple URL
    test_url = "https://www.example.com"
    scraper = WebScraper()
    pages = scraper.scrape_site(test_url, max_depth=1, max_pages=3)
    print(f"\nScraped {len(pages)} pages")
    for page in pages:
        print(f"  - {page['url']}: {len(page['text'])} chars")

