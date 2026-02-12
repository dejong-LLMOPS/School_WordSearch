"""Configuration settings for the school policy web scraper."""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT
OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_DIR = OUTPUT_DIR / "cache"

# Create output directories if they don't exist
OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

# Search terms
SEARCH_TERMS = [
"restorative justice",
"race equity",
"restorative practices",
"restorative discipline",
"alternatives to suspension",
"non-punitive discipline",
"student-centered discipline",
"discipline equity",
"equitable discipline",
"closing discipline gaps",
"discipline disparities",
"disproportionate discipline"
]



# Web scraping configuration
SCRAPING_CONFIG = {
    "max_depth": 10,  # Maximum depth for following links
    "timeout": 30,  # Request timeout in seconds
    "delay": 2,  # Delay between requests in seconds (reduced from 2s for speed)
    "min_delay": 0.3,  # Minimum delay for adaptive rate limiting
    "max_delay": 2.0,  # Maximum delay for adaptive rate limiting
    "max_pages_per_site": 1500,  # Maximum pages to scrape per site
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "context_snippet_length": 200,  # Characters around found term
    "workers": 5,  # Default number of worker threads for parallel processing
    # SSL/TLS configuration
    "verify_ssl": True,  # Verify SSL certificates (set to False only for sites with broken certificates - use with caution)
    "ssl_fallback_unverified": True,  # If SSL verification fails, try without verification as fallback (for sites with broken certs)
    "ssl_retry_with_unverified": True,  # Retry failed SSL connections without verification (last resort)
    # Content filtering options
    "min_content_length": 200,  # Minimum text content length to consider following links (skip directory pages)
    "skip_url_patterns": [  # URL patterns to skip content scraping (will still follow links from these pages, but won't search their content) - case-insensitive
        # External domains (these should be caught by domain checking, but adding as extra safeguard)
        r"zazzle\.com",  # External store links
        r"facebook\.com",  # Social media
        r"twitter\.com|x\.com",  # Social media
        r"instagram\.com",  # Social media
        r"youtube\.com",  # Video hosting
        r"linkedin\.com",  # Social media
        r"pinterest\.com",  # Social media
        r"tiktok\.com",  # Social media
        r"snapchat\.com",  # Social media
        r"floridavam\.org",  # External VAM system
        r"idavam\.org",  # External VAM system
        r"ci\.punta-gorda\.fl\.us",  # External city website
        r"/o/[^/]+$",  # Skip individual school/organization directory pages (e.g., /o/fes, /o/bmes)
        r"/portlets/?$",  # Skip portlet pages (usually 404s or empty)
        r"/portlets/",  # Skip any URL containing /portlets/
        r"/cms/portlets",  # Skip CMS portlet pages
        r"/gateway/login",  # Skip login pages
        r"/common/controls/general/email",  # Skip email form pages (encoded keys, not useful)
        r"action=sendemailtous",  # Skip email action URLs
        r"\.pdf$",  # Skip PDF files (we can't search text in PDFs easily)
        r"\.docx?$",  # Skip Word documents
        r"\.xlsx?$",  # Skip Excel files
        r"\.mp4$|\.avi$|\.mov$|\.wmv$|\.flv$|\.webm$",  # Skip video files
        r"\.mp3$|\.wav$|\.ogg$|\.m4a$",  # Skip audio files
        r"\.zip$|\.rar$|\.7z$|\.tar$|\.gz$",  # Skip archive files
        r"\.jpg$|\.jpeg$|\.png$|\.gif$|\.svg$|\.webp$|\.ico$|\.bmp$|\.tiff?$",  # Skip image files
        r"/cms/one\.aspx\?.*view=day",  # Skip calendar day view pages (too many, not useful)
        r"/cms/one\.aspx\?.*currentdate=",  # Skip calendar date-specific pages
        r"/blog\?tag=",  # Skip blog tag pages (often 500 errors)
        r"/wp-content/uploads/",  # Skip WordPress media uploads (PDFs, images, etc.)
        r"/userfiles/servers/",  # Skip SharpSchool file server URLs (often PDFs/images)
        r"/userfiles/",  # Skip any UserFiles directory (often contains PDFs/images)
        r"/servers/",  # Skip any /servers/ path
        r"/image/",  # Skip /Image/ or /image/ directories (usually just files)
        r"/images/",  # Skip /images/ directories
        r"/files/",  # Skip /files/ directories
        r"/documents/",  # Skip /documents/ directories
        r"/uploads/",  # Skip /uploads/ directories
        r"cdnsm\d+-ss\d+\.sharpschool\.com",  # Skip SharpSchool CDN domains (e.g., cdnsm5-ss20.sharpschool.com)
        r"\.sharpschool\.com.*/userfiles",  # Skip any sharpschool.com userfiles paths
        r"/content/uploads/",  # Skip WordPress-style upload directories
        # Long ID patterns in query parameters (CMS systems)
        r"portalId=\d+",  # Skip URLs with portalId parameter (CMS pages with IDs)
        r"pageId=\d+",  # Skip URLs with pageId parameter
        r"objectId\.\d+=\d+",  # Skip URLs with objectId.60974=1339405 style IDs
        r"contextId\.\d+=",  # Skip contextId parameters
        r"parentId\.\d+=",  # Skip parentId parameters
        r"server_\d+",  # Skip URLs with server IDs (e.g., Server_1339247)
        r"/servers/server_\d+",  # Skip server paths with IDs
        r"\?.*id=\d+",  # Skip URLs with generic id= parameter (often dynamic pages)
        r"\?.*ID=\d+",  # Skip URLs with ID= parameter
        r"rec_id=\d+",  # Skip record ID parameters
        r"recordId=\d+",  # Skip recordId parameters
        r"itemId=\d+",  # Skip itemId parameters
        r"entityId=\d+",  # Skip entityId parameters
        r"guid=[a-f0-9-]{20,}",  # Skip GUID/UUID parameters (long hex IDs)
        r"sessionId=",  # Skip session ID parameters
        r"sessionid=",  # Skip sessionid parameters
        r"sid=",  # Skip session ID short form
        r"timestamp=\d+",  # Skip timestamp parameters
        r"t=\d+",  # Skip timestamp short form
        r"ref=[a-zA-Z0-9+/=]{20,}",  # Skip long ref/encoded parameters
        r"key=[a-zA-Z0-9+/=]{20,}",  # Skip long key parameters (encoded keys)
        # Email and form-related patterns
        r"returnurl=",  # Skip redirect/login URLs with returnUrl parameter
        r"\.aspx\?.*key=",  # Skip ASPX pages with encoded keys (usually forms/emails)
        r"/common/controls/general/email",  # Skip email form pages (already exists)
        r"action=sendemailtous",  # Skip email action URLs (already exists)
        r"/email",  # Skip /email pages
        r"/contact.*form",  # Skip contact form pages
        r"/send.*email",  # Skip send email pages
        r"mailto:",  # Skip mailto: links (already handled but good to be explicit)
        r"/mail",  # Skip /mail pages
        r"/message",  # Skip /message pages (often forms)
        r"/form",  # Skip /form pages
        r"/submit",  # Skip /submit pages
        # Timeout-prone or problematic pages
        r"/ncimmunizations",  # Skip immunization pages (often timeout)
        r"ncimmunizations",  # Case-insensitive version
        # More file and media patterns
        r"/filemanager",  # Skip file manager pages
        r"/file-manager",  # Skip file-manager pages
        r"/filebrowser",  # Skip file browser pages
        r"/browse",  # Skip browse pages (often file browsers)
        r"/gallery",  # Skip gallery pages (usually just images)
        r"/photo",  # Skip photo pages
        r"/photos",  # Skip photos pages
        r"/video",  # Skip video pages
        r"/videos",  # Skip videos pages
        # More CMS and dynamic content patterns
        r"/cms/one\.aspx\?",  # Skip CMS One.aspx pages with any query params (too many variations)
        r"/cms/one\.aspx$",  # Skip CMS One.aspx base pages (usually empty)
        r"\.aspx\?.*&.*&",  # Skip ASPX pages with multiple query parameters (often dynamic)
        r"\?.*=.*&.*=.*&.*=",  # Skip URLs with 3+ query parameters (often dynamic/admin)
        # Hash fragments and single-page app patterns
        r"#",  # Skip URLs with hash fragments (SPA navigation, not useful for scraping)
        r"/#/",  # Skip hash-based routing
        # More server and CDN patterns
        r"cdn\d+",  # Skip CDN domains with numbers
        r"\.cloudfront\.net",  # Skip AWS CloudFront CDN
        r"\.s3\.amazonaws\.com",  # Skip AWS S3 buckets
        r"\.azureedge\.net",  # Skip Azure CDN
        r"\.r\.cdn77\.org",  # Skip CDN77
        r"/cdn/",  # Skip /cdn/ directories
        # More specific problematic patterns
        r"/temp/",  # Skip temp directories
        r"/tmp/",  # Skip tmp directories
        r"/cache/",  # Skip cache directories
        r"/backup/",  # Skip backup directories
        r"/old/",  # Skip old directories
        r"/test/",  # Skip test directories
        r"/staging/",  # Skip staging directories
        r"/cdn-cgi/",  # Skip Cloudflare CDN endpoints (not useful content)
        r"email-protection",  # Skip email protection obfuscation pages
        r"/download",  # Skip download pages (usually files)
        r"/attachment",  # Skip attachment pages
        r"/file\.(pdf|doc|docx|xls|xlsx|zip|rar)",  # Skip direct file links
        # API and data endpoints
        r"/api/",  # Skip API endpoints
        r"\.json$",  # Skip JSON files
        r"\.xml$",  # Skip XML files (unless RSS/Atom feeds, but those are usually not useful for text search)
        r"\.rss$|\.atom$|/feed",  # Skip RSS/Atom feeds
        r"/ajax/",  # Skip AJAX endpoints
        r"/rest/",  # Skip REST API endpoints
        r"/graphql",  # Skip GraphQL endpoints
        # Search and filter pages
        r"\?.*search=",  # Skip search result pages
        r"\?.*q=.*&",  # Skip query parameter search pages
        r"/search\?",  # Skip search pages
        r"/filter",  # Skip filter pages
        r"/sort",  # Skip sort pages
        # Print and mobile versions
        r"/print",  # Skip print versions
        r"/print/",  # Skip print directory
        r"print=true",  # Skip print parameter
        r"/mobile",  # Skip mobile-specific pages
        r"/m/",  # Skip mobile directory
        r"/amp/",  # Skip AMP pages
        r"\.amp$",  # Skip AMP file extension
        # Admin and backend pages
        r"/admin",  # Skip admin panels
        r"/administrator",  # Skip administrator pages
        r"/wp-admin",  # Skip WordPress admin
        r"/backend",  # Skip backend pages
        r"/manage",  # Skip management pages
        r"/dashboard",  # Skip dashboard pages
        r"/control",  # Skip control panels
        # Archive and version pages
        r"/archive",  # Skip archive pages
        r"/old",  # Skip old version pages
        r"/backup",  # Skip backup pages
        r"/version",  # Skip version pages
        r"/history",  # Skip history pages
        # Configuration and system files
        r"robots\.txt$",  # Skip robots.txt
        r"sitemap.*\.xml$",  # Skip sitemap XML files
        r"\.htaccess$",  # Skip .htaccess files
        r"\.git/",  # Skip git directories
        r"\.svn/",  # Skip SVN directories
        r"web\.config$",  # Skip web.config files
        # Static assets
        r"\.js$",  # Skip JavaScript files
        r"\.css$",  # Skip CSS files
        r"\.woff2?$|\.ttf$|\.eot$",  # Skip font files
        r"/assets/",  # Skip assets directories (usually static files)
        r"/static/",  # Skip static directories
        r"/media/",  # Skip media directories (usually files)
        # Social and sharing
        r"/share",  # Skip share pages
        r"share=true",  # Skip share parameter
        r"/social",  # Skip social media pages
        r"utm_source=|utm_medium=|utm_campaign=",  # Skip tracking parameters (though these might be on useful pages)
        # Error and redirect pages
        r"/404",  # Skip 404 pages
        r"/error",  # Skip error pages
        r"/redirect",  # Skip redirect pages
        # Test and staging
        r"/test",  # Skip test pages
        r"/staging",  # Skip staging pages
        r"/dev",  # Skip development pages
        r"/demo",  # Skip demo pages
        # Other common non-content pages
        r"/sitemap",  # Skip sitemap pages (HTML versions)
        r"/terms",  # Skip terms of service (usually boilerplate)
        r"/privacy",  # Skip privacy policy (usually boilerplate)
        r"/cookie",  # Skip cookie policy pages
        r"/disclaimer",  # Skip disclaimer pages
        r"/accessibility",  # Skip accessibility pages (usually boilerplate)
        r"/contact.*form",  # Skip contact forms (usually just forms, not content)
        r"/subscribe",  # Skip subscription pages
        r"/newsletter",  # Skip newsletter pages
        r"/signup",  # Skip signup pages
        r"/register",  # Skip registration pages
        r"/login",  # Skip login pages (additional pattern)
        r"/logout",  # Skip logout pages
    ],
    "quick_content_check": True,  # Do a quick content check before following links (speeds up scraping)
    "require_relevant_content": False,  # If True, only follow links from pages that contain search terms
    "save_file_cache": False,  # Save per-school cache files to disk (URL-level cache is more efficient)
}



# Perplexity AI configuration
PERPLEXITY_CONFIG = {
    "api_key": os.getenv("PERPLEXITY_API_KEY", ""),
    "api_url": os.getenv("PERPLEXITY_API_URL", "https://api.perplexity.ai"),
    "model": os.getenv("PERPLEXITY_MODEL", "sonar"),  # Using sonar model (fast, cost-effective)
    "timeout": 60,
    "max_retries": 3,
    "ai_summary_mode": "unified",  # "unified" for one summary per school, "per_term" for individual summaries
}

# Excel file paths
SCHOOLS_FILE = DATA_DIR / "U.S. schools database_NCES.xlsx"
DISTRICTS_FILE = DATA_DIR / "U.S. school districts_NCES.xlsx"

# Output file paths
RESULTS_CSV = OUTPUT_DIR / "results.csv"
PROGRESS_FILE = OUTPUT_DIR / "progress.json"

# State filter (default: North Carolina)
DEFAULT_STATE = "NC"

