"""
Books scraper implementation for Books to Scrape website.

This module implements the actual scraping logic for extracting
book data from books.toscrape.com.

@module books_scraper
@author Jeffrey Dabo
@date 2025
"""

import re
import time
from typing import List, Dict, Any, Optional
from loguru import logger

from src.scraper.base_scraper import BaseScraper


class BooksScraper(BaseScraper):
    """
    Scraper for books.toscrape.com website.

    @class BooksScraper
    @extends {BaseScraper}
    """

    def __init__(self, base_url: str = "https://books.toscrape.com/", verify_ssl: bool = False):
        """
        Initialize books scraper.

        @constructor
        @param {str} base_url - Base URL of the books website
        @param {bool} verify_ssl - Whether to verify SSL certificates (default: False for this site)
        """
        super().__init__(base_url, verify_ssl=verify_ssl)
        self.rating_map = {
            'One': 1,
            'Two': 2,
            'Three': 3,
            'Four': 4,
            'Five': 5
        }

    def extract_price(self, price_text: str) -> float:
        """
        Extract numeric price from price text.

        @param {str} price_text - Price text (e.g., "£51.77")
        @returns {float} Numeric price value
        """
        price_match = re.search(r'[\d.]+', price_text)
        return float(price_match.group()) if price_match else 0.0

    def extract_rating(self, rating_class: str) -> int:
        """
        Extract numeric rating from CSS class.

        @param {str} rating_class - Rating CSS class
        @returns {int} Numeric rating (1-5)
        """
        for word, rating in self.rating_map.items():
            if word in rating_class:
                return rating
        return 0

    def scrape_book_details(self, book_url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape detailed information from a book's detail page.

        @param {str} book_url - URL of the book detail page
        @returns {dict|None} Book details including category
        """
        soup = self.fetch_page(book_url)
        if not soup:
            return None

        try:
            details = {}

            # Extract category from breadcrumb
            breadcrumb = soup.find('ul', class_='breadcrumb')
            if breadcrumb:
                category_links = breadcrumb.find_all('a')
                if len(category_links) >= 3:  # Home > Books > Category
                    details['category'] = category_links[2].text.strip()
                else:
                    details['category'] = 'Unknown'

            # Extract description if available
            product_description = soup.find('div', id='product_description')
            if product_description:
                description_p = product_description.find_next_sibling('p')
                if description_p:
                    details['description'] = description_p.text.strip()

            # Extract image URL
            image_container = soup.find('div', class_='item active')
            if image_container:
                img = image_container.find('img')
                if img and img.get('src'):
                    img_src = img['src']
                    # Build full image URL
                    if img_src.startswith('../../'):
                        details['image_url'] = f"{self.base_url}{img_src.replace('../../', '')}"
                    else:
                        details['image_url'] = img_src

            # Extract UPC and other product information
            product_info_table = soup.find('table', class_='table table-striped')
            if product_info_table:
                rows = product_info_table.find_all('tr')
                for row in rows:
                    header = row.find('th')
                    value = row.find('td')
                    if header and value:
                        key = header.text.strip().lower().replace(' ', '_').replace('(', '').replace(')', '').replace(
                            '.', '')
                        value_text = value.text.strip()

                        # Handle different field types
                        if key == 'upc':
                            details['upc'] = value_text
                        elif key == 'product_type':
                            details['product_type'] = value_text
                        elif key == 'price_excl_tax':
                            details['price_excl_tax'] = self.extract_price(value_text)
                        elif key == 'price_incl_tax':
                            details['price_incl_tax'] = self.extract_price(value_text)
                        elif key == 'tax':
                            details['tax'] = self.extract_price(value_text)
                        elif key == 'number_of_reviews':
                            details['number_of_reviews'] = int(value_text) if value_text.isdigit() else 0

            return details

        except Exception as e:
            logger.error(f"Error scraping book details from {book_url}: {e}")
            return None

    def scrape_page(self, page_url: str, include_details: bool = False) -> List[Dict[str, Any]]:
        """
        Scrape books from a single page.

        @param {str} page_url - URL of the page to scrape
        @param {bool} include_details - Whether to fetch detailed info from each book's page
        @returns {list} List of book dictionaries
        """
        soup = self.fetch_page(page_url)
        if not soup:
            return []

        books = []
        book_containers = soup.find_all('article', class_='product_pod')

        # Determine the base path based on current page URL
        is_catalogue_page = 'catalogue' in page_url

        for idx, book in enumerate(book_containers, 1):
            try:
                # Extract title
                title_elem = book.find('h3').find('a')
                title = title_elem.get('title', '')

                # Extract price
                price_elem = book.find('p', class_='price_color')
                price = self.extract_price(price_elem.text) if price_elem else 0.0

                # Extract availability
                availability_elem = book.find('p', class_='instock availability')
                availability = availability_elem.text.strip() if availability_elem else 'Unknown'

                # Extract rating
                rating_elem = book.find('p', class_='star-rating')
                rating = self.extract_rating(rating_elem.get('class', [])) if rating_elem else 0

                # Extract and construct proper URL
                book_url = title_elem.get('href', '')
                if book_url:
                    # Handle different URL patterns
                    if book_url.startswith('http'):
                        # Already a full URL
                        pass
                    elif book_url.startswith('../../../'):
                        # From main page: ../../../catalogue/book_id/index.html
                        book_url = book_url.replace('../../../', '')
                        book_url = f"{self.base_url}{book_url}"
                    elif book_url.startswith('../'):
                        # From catalogue pages: ../book_id/index.html
                        book_url = book_url.replace('../', '')
                        book_url = f"{self.base_url}catalogue/{book_url}"
                    else:
                        # Relative path without ../
                        if not book_url.startswith('catalogue/'):
                            book_url = f"catalogue/{book_url}"
                        book_url = f"{self.base_url}{book_url}"

                book_data = {
                    'title': title,
                    'price': price,
                    'availability': availability,
                    'rating': rating,
                    'url': book_url,
                    'category': 'Unknown'  # Default value
                }

                # Fetch detailed information if requested
                if include_details and book_url:
                    logger.info(f"Fetching details for book {idx}/{len(book_containers)}: {title[:50]}...")
                    logger.debug(f"Book URL: {book_url}")
                    details = self.scrape_book_details(book_url)
                    if details:
                        book_data.update(details)
                    else:
                        # If details fetch fails, still keep the basic data
                        logger.warning(f"Could not fetch details for '{title[:50]}', using basic data only")
                    time.sleep(0.5)  # Be nice to the server

                books.append(book_data)
                logger.debug(f"Scraped book: {title}")

            except Exception as e:
                logger.error(f"Error scraping book at index {idx}: {e}")
                continue

        return books

    def scrape_categories(self) -> List[Dict[str, str]]:
        """
        Scrape all available categories from the sidebar.

        @returns {list} List of category dictionaries with name and URL
        """
        soup = self.fetch_page(self.base_url)
        if not soup:
            return []

        categories = []
        side_categories = soup.find('div', class_='side_categories')

        if side_categories:
            category_links = side_categories.find_all('a')[1:]  # Skip "Books" link

            for link in category_links:
                category_name = link.text.strip()
                category_url = link.get('href', '')

                if category_url:
                    # Build full URL
                    if category_url.startswith('../'):
                        category_url = f"{self.base_url}{category_url.replace('../', '')}"

                    categories.append({
                        'name': category_name,
                        'url': category_url
                    })

        logger.info(f"Found {len(categories)} categories")
        return categories

    def scrape_category(self, category_name: str, category_url: str, max_pages: Optional[int] = None) -> List[
        Dict[str, Any]]:
        """
        Scrape all books from a specific category.

        @param {str} category_name - Name of the category
        @param {str} category_url - URL of the category
        @param {int|None} max_pages - Maximum pages to scrape for this category
        @returns {list} List of books in this category
        """
        logger.info(f"Scraping category: {category_name}")
        all_books = []
        page_num = 1

        while True:
            if max_pages and page_num > max_pages:
                break

            # Construct page URL
            if page_num == 1:
                page_url = category_url
            else:
                # Replace index.html with page-X.html
                page_url = category_url.replace('index.html', f'page-{page_num}.html')

            logger.info(f"  Scraping page {page_num}: {page_url}")

            soup = self.fetch_page(page_url)
            if not soup:
                break

            book_containers = soup.find_all('article', class_='product_pod')
            if not book_containers:
                logger.info(f"  No books found on page {page_num}")
                break

            for book in book_containers:
                try:
                    # Extract title
                    title_elem = book.find('h3').find('a')
                    title = title_elem.get('title', '')

                    # Extract price
                    price_elem = book.find('p', class_='price_color')
                    price = self.extract_price(price_elem.text) if price_elem else 0.0

                    # Extract availability
                    availability_elem = book.find('p', class_='instock availability')
                    availability = availability_elem.text.strip() if availability_elem else 'Unknown'

                    # Extract rating
                    rating_elem = book.find('p', class_='star-rating')
                    rating = self.extract_rating(rating_elem.get('class', [])) if rating_elem else 0

                    # Extract URL
                    book_url = title_elem.get('href', '')
                    if book_url:
                        if book_url.startswith('../'):
                            book_url = f"{self.base_url}catalogue/{book_url.replace('../../../', '').replace('../', '')}"

                    book_data = {
                        'title': title,
                        'price': price,
                        'availability': availability,
                        'rating': rating,
                        'url': book_url,
                        'category': category_name  # Set category from current context
                    }

                    all_books.append(book_data)

                except Exception as e:
                    logger.error(f"Error scraping book in category {category_name}: {e}")
                    continue

            logger.info(f"  Page {page_num}: Scraped {len(book_containers)} books")
            page_num += 1

        logger.info(f"Category '{category_name}': Total {len(all_books)} books")
        return all_books

    def scrape(self, max_pages: Optional[int] = None, include_details: bool = False, by_category: bool = False) -> List[
        Dict[str, Any]]:
        """
        Scrape all books from the website.

        @param {int|None} max_pages - Maximum number of pages to scrape (None for all)
        @param {bool} include_details - Whether to fetch detailed info from each book's page (slower)
        @param {bool} by_category - Whether to scrape by category (includes category info)
        @returns {list} List of all scraped books
        """
        if by_category:
            # Scrape by categories
            logger.info("Scraping by categories...")
            categories = self.scrape_categories()
            all_books = []

            for category in categories:
                books = self.scrape_category(
                    category['name'],
                    category['url'],
                    max_pages=1 if max_pages else None  # Limit pages per category
                )
                all_books.extend(books)
                time.sleep(1)  # Be nice to the server

            logger.info(f"Total books scraped across all categories: {len(all_books)}")
            return all_books

        else:
            # Original scraping method
            all_books = []
            page_num = 1

            while True:
                if max_pages and page_num > max_pages:
                    break

                page_url = f"{self.base_url}catalogue/page-{page_num}.html" if page_num > 1 else self.base_url
                logger.info(f"Scraping page {page_num}: {page_url}")

                books = self.scrape_page(page_url, include_details=include_details)
                if not books:
                    logger.info(f"No books found on page {page_num}. Stopping.")
                    break

                all_books.extend(books)
                logger.info(f"Page {page_num}: Scraped {len(books)} books (Total: {len(all_books)})")
                page_num += 1

            logger.info(f"Total books scraped: {len(all_books)}")
            return all_books