"""
Main entry point for the data pipeline application.

This script provides CLI commands for running different components
of the application.

@module main
@author Jeffrey Dabo
@date 2025
"""

import sys
import argparse
from loguru import logger

from src.database.connection import db_manager
from src.scraper.books_scraper import BooksScraper
from src.utils.config import Config


def setup_logging():
    """
    Configure logging settings.

    @returns {None}
    """
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    logger.add(
        Config.LOGS_DIR / "app_{time}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO"
    )


def init_db():
    """
    Initialize database.

    @returns {None}
    """
    logger.info("Initializing database...")
    db_manager.initialize()
    db_manager.create_tables()
    logger.info("Database initialized successfully")


def run_scraper(max_pages: int = None, include_details: bool = False, by_category: bool = False):
    """
    Run the scraper.

    @param {int|None} max_pages - Maximum pages to scrape
    @param {bool} include_details - Whether to fetch book details
    @param {bool} by_category - Whether to scrape by category
    @returns {None}
    """
    logger.info("Starting scraper...")

    if by_category:
        logger.info("Scraping mode: BY CATEGORY (includes category information)")
    elif include_details:
        logger.info("Scraping mode: WITH DETAILS (slower, includes full book info)")
    else:
        logger.info("Scraping mode: BASIC (faster, limited info)")

    scraper = BooksScraper()
    books = scraper.scrape(
        max_pages=max_pages,
        include_details=include_details,
        by_category=by_category
    )
    scraper.close()

    if books:
        logger.info(f"Scraped {len(books)} books. Inserting into database...")
        inserted = db_manager.insert_books_bulk(books)
        logger.info(f"Successfully inserted {inserted} books")

        # Show sample of scraped data
        if books:
            logger.info(f"Sample book: {books[0]}")
    else:
        logger.warning("No books were scraped")


def run_api():
    """
    Run the FastAPI server.

    @returns {None}
    """
    import uvicorn
    from src.api.routes import app

    logger.info("Starting API server...")
    uvicorn.run(
        app,
        host=Config.API_HOST,
        port=Config.API_PORT,
        log_level="info"
    )


def run_dashboard():
    """
    Run the Streamlit dashboard.

    @returns {None}
    """
    import subprocess

    logger.info("Starting Streamlit dashboard...")
    subprocess.run([
        "streamlit",
        "run",
        "dashboard/app.py",
        "--server.port",
        str(Config.DASHBOARD_PORT)
    ])


def main():
    """
    Main CLI function.

    @returns {None}
    """
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Data Pipeline Application",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic scraping (no categories)
  python main.py scrape --max-pages 5

  # Scrape with detailed info (includes categories, slower)
  python main.py scrape --max-pages 2 --include-details

  # Scrape by category (recommended for categories)
  python main.py scrape --by-category

  # Scrape first page of each category
  python main.py scrape --by-category --max-pages 1
        """
    )

    parser.add_argument(
        "command",
        choices=["init-db", "scrape", "api", "dashboard"],
        help="Command to execute"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum pages to scrape (for scrape command)"
    )
    parser.add_argument(
        "--include-details",
        action="store_true",
        help="Fetch detailed info from each book's page (slower)"
    )
    parser.add_argument(
        "--by-category",
        action="store_true",
        help="Scrape books by category (includes category information)"
    )

    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
    elif args.command == "scrape":
        init_db()
        run_scraper(
            max_pages=args.max_pages,
            include_details=args.include_details,
            by_category=args.by_category
        )
    elif args.command == "api":
        run_api()
    elif args.command == "dashboard":
        run_dashboard()


if __name__ == "__main__":
    main()