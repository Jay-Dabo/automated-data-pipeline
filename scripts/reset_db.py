"""
Script to reset the database with new schema.

@module reset_db
"""

from src.database.connection import db_manager
from src.utils.config import Config
from loguru import logger


def reset_database():
    """
    Drop all tables and recreate them.

    @returns {None}
    """
    logger.info("Resetting database...")

    # Initialize connection
    db_manager.initialize()

    # Drop all tables
    logger.warning("Dropping all tables...")
    db_manager.drop_tables()

    # Create new tables with updated schema
    logger.info("Creating tables with new schema...")
    db_manager.create_tables()

    logger.info("Database reset complete!")


if __name__ == "__main__":
    reset_database()