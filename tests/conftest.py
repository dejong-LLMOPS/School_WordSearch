"""Pytest configuration. Ensures project root is on sys.path when running tests."""
import sys
from pathlib import Path

# Add project root so tests can import config, web_scraper, utils, etc.
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
