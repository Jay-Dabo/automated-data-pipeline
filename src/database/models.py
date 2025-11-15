"""
Database models for the data pipeline.

This module defines SQLAlchemy ORM models for storing scraped data.

@module models
@author Jeffrey Dabo
@date 2025
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Book(Base):
    """
    Book model representing scraped book data.

    @class Book
    @extends {Base}

    @property {int} id - Primary key
    @property {str} title - Book title
    @property {float} price - Book price
    @property {str} availability - Stock availability status
    @property {int} rating - Book rating (1-5)
    @property {str} category - Book category/genre
    @property {str} url - Source URL
    @property {str} description - Book description
    @property {str} upc - Universal Product Code
    @property {str} product_type - Product type
    @property {float} price_excl_tax - Price excluding tax
    @property {float} price_incl_tax - Price including tax
    @property {float} tax - Tax amount
    @property {int} number_of_reviews - Number of reviews
    @property {str} image_url - Book cover image URL
    @property {datetime} scraped_at - Timestamp of scraping
    @property {datetime} created_at - Record creation timestamp
    @property {datetime} updated_at - Record update timestamp
    """

    __tablename__ = "books"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Basic Information
    title = Column(String(500), nullable=False, index=True)
    price = Column(Float, nullable=False)
    availability = Column(String(100))
    rating = Column(Integer)
    category = Column(String(100), index=True)
    url = Column(Text)

    # Detailed Information (from book detail pages)
    description = Column(Text, nullable=True)
    upc = Column(String(100), nullable=True, unique=True)
    product_type = Column(String(100), nullable=True)
    price_excl_tax = Column(Float, nullable=True)
    price_incl_tax = Column(Float, nullable=True)
    tax = Column(Float, nullable=True)
    number_of_reviews = Column(Integer, nullable=True, default=0)
    image_url = Column(Text, nullable=True)

    # Metadata
    scraped_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    def __repr__(self) -> str:
        """
        String representation of Book object.

        @returns {str} Book representation
        """
        return f"<Book(id={self.id}, title='{self.title[:50]}...', price={self.price}, category='{self.category}')>"

    def to_dict(self) -> dict:
        """
        Convert Book object to dictionary.

        @returns {dict} Dictionary representation of the book
        """
        return {
            "id": self.id,
            "title": self.title,
            "price": self.price,
            "availability": self.availability,
            "rating": self.rating,
            "category": self.category,
            "url": self.url,
            "description": self.description,
            "upc": self.upc,
            "product_type": self.product_type,
            "price_excl_tax": self.price_excl_tax,
            "price_incl_tax": self.price_incl_tax,
            "tax": self.tax,
            "number_of_reviews": self.number_of_reviews,
            "image_url": self.image_url,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }