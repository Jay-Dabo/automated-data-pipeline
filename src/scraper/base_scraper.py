# """
# Base scraper class with common functionality.
#
# This module provides a base class for all web scrapers with
# retry logic, error handling, and logging.
#
# @module base_scraper
# @author Jeffrey Dabo
# @date 2025
# """
#
# import time
# from abc import ABC, abstractmethod
# from typing import Optional, Dict, Any
# import requests
# from bs4 import BeautifulSoup
# from loguru import logger
#
# from src.utils.config import Config
#
#
# class BaseScraper(ABC):
#     """
#     Abstract base class for web scrapers.
#
#     @class BaseScraper
#     @abstract
#     """
#
#     def __init__(self, base_url: str):
#         """
#         Initialize base scraper.
#
#         @constructor
#         @param {str} base_url - Base URL for scraping
#         """
#         self.base_url = base_url
#         self.session = requests.Session()
#         self.session.headers.update({
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
#         })
#
#     def fetch_page(self, url: str, retries: int = Config.MAX_RETRIES) -> Optional[BeautifulSoup]:
#         """
#         Fetch and parse a web page with retry logic.
#
#         @param {str} url - URL to fetch
#         @param {int} retries - Number of retry attempts
#         @returns {BeautifulSoup|None} Parsed HTML or None if failed
#         """
#         for attempt in range(retries):
#             try:
#                 response = self.session.get(
#                     url,
#                     timeout=Config.REQUEST_TIMEOUT
#                 )
#                 response.raise_for_status()
#                 logger.info(f"Successfully fetched: {url}")
#                 return BeautifulSoup(response.content, 'html.parser')
#
#             except requests.RequestException as e:
#                 logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
#                 if attempt < retries - 1:
#                     time.sleep(2 ** attempt)  # Exponential backoff
#                 else:
#                     logger.error(f"Failed to fetch {url} after {retries} attempts")
#                     return None
#
#     @abstractmethod
#     def scrape(self) -> list:
#         """
#         Abstract method to be implemented by subclasses.
#
#         @abstract
#         @returns {list} List of scraped data
#         """
#         pass
#
#     def close(self) -> None:
#         """
#         Close the requests session.
#
#         @returns {None}
#         """
#         self.session.close()
#         logger.info("Scraper session closed")

"""
Base scraper class with common functionality.

This module provides a base class for all web scrapers with
retry logic, error handling, and logging.

@module base_scraper
@author Jeffrey Dabo
@date 2025
"""

import time
import ssl
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from loguru import logger

from src.utils.config import Config


class BaseScraper(ABC):
    """
    Abstract base class for web scrapers.

    @class BaseScraper
    @abstract
    """

    def __init__(self, base_url: str, verify_ssl: bool = True):
        """
        Initialize base scraper.

        @constructor
        @param {str} base_url - Base URL for scraping
        @param {bool} verify_ssl - Whether to verify SSL certificates
        """
        self.base_url = base_url
        self.verify_ssl = verify_ssl
        self.session = requests.Session()

        # Configure session headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        })

        # Configure retry strategy
        retry_strategy = Retry(
            total=Config.MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Disable SSL warnings if verify_ssl is False
        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            logger.warning("SSL verification disabled - use only for development!")

    def fetch_page(self, url: str, retries: int = Config.MAX_RETRIES) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page with retry logic.

        @param {str} url - URL to fetch
        @param {int} retries - Number of retry attempts
        @returns {BeautifulSoup|None} Parsed HTML or None if failed
        """
        for attempt in range(retries):
            try:
                response = self.session.get(
                    url,
                    timeout=Config.REQUEST_TIMEOUT,
                    verify=self.verify_ssl
                )
                response.raise_for_status()
                logger.info(f"Successfully fetched: {url}")
                return BeautifulSoup(response.content, 'html.parser')

            except requests.exceptions.SSLError as e:
                logger.error(f"SSL Error for {url}: {e}")
                logger.info("Hint: Try running with verify_ssl=False or install certificates")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None

            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to fetch {url} after {retries} attempts")
                    return None

    @abstractmethod
    def scrape(self) -> list:
        """
        Abstract method to be implemented by subclasses.

        @abstract
        @returns {list} List of scraped data
        """
        pass

    def close(self) -> None:
        """
        Close the requests session.

        @returns {None}
        """
        self.session.close()
        logger.info("Scraper session closed")