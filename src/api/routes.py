"""
FastAPI routes for the data pipeline API.

This module defines REST API endpoints for accessing scraped data.

@module routes
@author Jeffrey Dabo
@date 2025
"""

from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from loguru import logger

from src.database.connection import db_manager
from src.scraper.books_scraper import BooksScraper


# Pydantic models for API
class BookResponse(BaseModel):
    """
    Book response model.

    @class BookResponse
    """
    id: int
    title: str
    price: float
    availability: Optional[str]
    rating: Optional[int]
    category: Optional[str]
    url: Optional[str]


class StatisticsResponse(BaseModel):
    """
    Statistics response model.

    @class StatisticsResponse
    """
    total_books: int
    avg_price: float
    min_price: float
    max_price: float


class ScrapeRequest(BaseModel):
    """
    Scrape request model.

    @class ScrapeRequest
    """
    max_pages: Optional[int] = Field(None, description="Maximum pages to scrape")


# Initialize FastAPI app
app = FastAPI(
    title="Data Pipeline API",
    description="API for automated data extraction and retrieval",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """
    Initialize database on startup.

    @event startup
    @returns {None}
    """
    db_manager.initialize()
    db_manager.create_tables()
    logger.info("API startup complete")


@app.get("/")
async def root():
    """
    Root endpoint.

    @route GET /
    @returns {dict} Welcome message
    """
    return {
        "message": "Data Pipeline API",
        "version": "1.0.0",
        "endpoints": ["/books", "/statistics", "/scrape"]
    }


@app.get("/books", response_model=List[BookResponse])
async def get_books(
        limit: Optional[int] = Query(100, description="Maximum number of books to return"),
        min_price: Optional[float] = Query(None, description="Minimum price filter"),
        max_price: Optional[float] = Query(None, description="Maximum price filter")
):
    """
    Get all books with optional filters.

    @route GET /books
    @queryparam {int} limit - Maximum number of results
    @queryparam {float} min_price - Minimum price filter
    @queryparam {float} max_price - Maximum price filter
    @returns {list} List of books
    @throws {HTTPException} 500 if database error occurs
    """
    try:
        if min_price is not None and max_price is not None:
            books = db_manager.get_books_by_price_range(min_price, max_price)
        else:
            books = db_manager.get_all_books(limit=limit)

        return books
    except Exception as e:
        logger.error(f"Error retrieving books: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve books")


@app.get("/statistics", response_model=StatisticsResponse)
async def get_statistics():
    """
    Get database statistics.

    @route GET /statistics
    @returns {dict} Statistics data
    @throws {HTTPException} 500 if database error occurs
    """
    try:
        stats = db_manager.get_statistics()
        return stats
    except Exception as e:
        logger.error(f"Error retrieving statistics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve statistics")


@app.post("/scrape")
async def trigger_scrape(request: ScrapeRequest):
    """
    Trigger scraping process.

    @route POST /scrape
    @bodyparam {ScrapeRequest} request - Scrape configuration
    @returns {dict} Scrape results
    @throws {HTTPException} 500 if scraping fails
    """
    try:
        scraper = BooksScraper()
        books = scraper.scrape(max_pages=request.max_pages)
        scraper.close()

        if books:
            inserted_count = db_manager.insert_books_bulk(books)
            return {
                "status": "success",
                "books_scraped": len(books),
                "books_inserted": inserted_count
            }
        else:
            raise HTTPException(status_code=500, detail="No books scraped")

    except Exception as e:
        logger.error(f"Error during scraping: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    """
    Health check endpoint.

    @route GET /health
    @returns {dict} Health status
    """
    return {"status": "healthy"}