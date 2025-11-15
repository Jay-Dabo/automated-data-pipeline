"""
Database connection and operations module.

This module handles all database connections and CRUD operations.

@module connection
@author Jeffrey Dabo
@date 2025
"""

from typing import List, Optional, Dict, Any
from contextlib import contextmanager
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

from src.utils.config import Config
from src.database.models import Base, Book


class DatabaseManager:
    """
    Database manager class for handling all database operations.

    @class DatabaseManager
    """

    def __init__(self):
        """
        Initialize database manager with connection settings.

        @constructor
        """
        self.database_url = Config.get_database_url()
        self.engine = None
        self.SessionLocal = None

    def initialize(self) -> None:
        """
        Initialize database engine and create tables.

        @returns {None}
        @throws {SQLAlchemyError} If database connection fails
        """
        try:
            self.engine = create_engine(
                self.database_url,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20,
                echo=False
            )
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            logger.info("Database engine initialized successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to initialize database: {e}")
            raise

    def create_tables(self) -> None:
        """
        Create all tables defined in models.

        @returns {None}
        """
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def drop_tables(self) -> None:
        """
        Drop all tables (use with caution!).

        @returns {None}
        """
        try:
            Base.metadata.drop_all(bind=self.engine)
            logger.warning("All database tables dropped")
        except SQLAlchemyError as e:
            logger.error(f"Failed to drop tables: {e}")
            raise

    @contextmanager
    def get_session(self):
        """
        Context manager for database sessions.

        @yields {Session} SQLAlchemy session
        @example
        with db_manager.get_session() as session:
            books = session.query(Book).all()
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()

    def insert_book(self, book_data: Dict[str, Any]) -> Optional[int]:
        """
        Insert a single book record.

        @param {dict} book_data - Dictionary containing book information
        @returns {int|None} Inserted book ID or None if failed
        """
        try:
            with self.get_session() as session:
                book = Book(**book_data)
                session.add(book)
                session.flush()
                logger.info(f"Inserted book: {book.title}")
                return book.id
        except SQLAlchemyError as e:
            logger.error(f"Failed to insert book: {e}")
            return None

    def insert_books_bulk(self, books_data: List[Dict[str, Any]]) -> int:
        """
        Insert multiple book records in bulk with optimized error handling.

        @param {list} books_data - List of book dictionaries
        @returns {int} Number of books inserted
        """
        inserted_count = 0
        failed_books = []

        # First pass: Try bulk insert
        try:
            with self.get_session() as session:
                books_to_insert = []

                for book_data in books_data:
                    # Skip if UPC already exists in the batch
                    if 'upc' in book_data and book_data.get('upc'):
                        existing = session.query(Book).filter_by(upc=book_data['upc']).first()
                        if existing:
                            logger.debug(f"Book with UPC {book_data['upc']} already exists")
                            continue

                    books_to_insert.append(Book(**book_data))

                if books_to_insert:
                    session.bulk_save_objects(books_to_insert)
                    session.commit()
                    inserted_count = len(books_to_insert)
                    logger.info(f"Bulk insert successful: {inserted_count} books")
                    return inserted_count

        except Exception as e:
            logger.warning(f"Bulk insert failed: {e}. Trying individual inserts...")
            session.rollback()

        # Second pass: Insert individually if bulk fails
        for book_data in books_data:
            try:
                with self.get_session() as session:
                    # Check for duplicates
                    if 'upc' in book_data and book_data.get('upc'):
                        existing = session.query(Book).filter_by(upc=book_data['upc']).first()
                        if existing:
                            continue

                    book = Book(**book_data)
                    session.add(book)
                    session.commit()
                    inserted_count += 1

            except Exception as e:
                logger.warning(f"Failed to insert '{book_data.get('title', 'Unknown')}': {str(e)[:100]}")
                failed_books.append(book_data.get('title', 'Unknown'))
                continue

        if failed_books:
            logger.warning(f"Failed to insert {len(failed_books)} books: {failed_books[:5]}")

        logger.info(f"Insert complete: {inserted_count} books inserted")
        return inserted_count

    def get_all_books(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Retrieve all books from database.

        @param {int|None} limit - Maximum number of records to return
        @returns {list} List of book dictionaries
        """
        try:
            with self.get_session() as session:
                query = session.query(Book).order_by(Book.created_at.desc())
                if limit:
                    query = query.limit(limit)
                books = query.all()
                return [book.to_dict() for book in books]
        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve books: {e}")
            return []

    def get_books_by_price_range(
            self,
            min_price: float,
            max_price: float
    ) -> List[Dict[str, Any]]:
        """
        Get books within a specific price range.

        @param {float} min_price - Minimum price
        @param {float} max_price - Maximum price
        @returns {list} List of book dictionaries
        """
        try:
            with self.get_session() as session:
                books = session.query(Book).filter(
                    Book.price >= min_price,
                    Book.price <= max_price
                ).all()
                return [book.to_dict() for book in books]
        except SQLAlchemyError as e:
            logger.error(f"Failed to retrieve books by price: {e}")
            return []

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.

        @returns {dict} Dictionary containing various statistics
        """
        try:
            with self.get_session() as session:
                from sqlalchemy import func

                stats = session.query(
                    func.count(Book.id).label('total_books'),
                    func.avg(Book.price).label('avg_price'),
                    func.min(Book.price).label('min_price'),
                    func.max(Book.price).label('max_price')
                ).first()

                return {
                    'total_books': stats.total_books or 0,
                    'avg_price': float(stats.avg_price) if stats.avg_price else 0.0,
                    'min_price': float(stats.min_price) if stats.min_price else 0.0,
                    'max_price': float(stats.max_price) if stats.max_price else 0.0,
                }
        except SQLAlchemyError as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}


# Singleton instance
db_manager = DatabaseManager()