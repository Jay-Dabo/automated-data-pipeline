"""
Configuration management module.

This module handles loading and managing configuration settings
from environment variables and config files.

@module config
@author Jeffrey Dabo
@date 2025
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()


class Config:
    """
    Configuration class for managing application settings.

    This class provides a centralized way to access configuration
    parameters throughout the application.

    @class Config
    """

    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "data_pipeline")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")

    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))

    # Scraping Configuration
    TARGET_URL: str = os.getenv("TARGET_URL", "https://books.toscrape.com/")
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

    # Dashboard Configuration
    DASHBOARD_PORT: int = int(os.getenv("DASHBOARD_PORT", "8501"))

    # Project Paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    LOGS_DIR: Path = BASE_DIR / "logs"

    @classmethod
    def get_database_url(cls) -> str:
        """
        Construct and return the database connection URL.

        @returns {str} PostgreSQL connection URL
        """
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"

    @classmethod
    def setup_directories(cls) -> None:
        """
        Create necessary directories if they don't exist.

        @returns {None}
        """
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.LOGS_DIR.mkdir(exist_ok=True)
        logger.info("Directories setup complete")


# Initialize directories on import
Config.setup_directories()