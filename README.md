# 📚 Automated Data Extraction, Storage, and Reporting Dashboard

A complete data pipeline project that demonstrates web scraping, database storage, and data visualization using Python.

## 🎯 Features

- **Web Scraping**: Automated data extraction from Books to Scrape website
- **Database Storage**: PostgreSQL database with SQLAlchemy ORM
- **REST API**: FastAPI-based API for data access
- **Interactive Dashboard**: Streamlit dashboard with real-time visualizations
- **Robust Error Handling**: Retry logic, logging, and error management
- **Modular Architecture**: Clean, maintainable, and extensible code

## 🛠️ Technologies

- Python 3.8+
- PostgreSQL
- SQLAlchemy
- FastAPI
- Streamlit
- BeautifulSoup4
- Plotly

## 📦 Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd automated-data-pipeline
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Setup environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize database:
```bash
python main.py init-db
```

## 🚀 Usage

### Scrape Data
```bash
python main.py scrape --max-pages 5
```

### Run API Server
```bash
python main.py api
# API will be available at http://localhost:8000
# API docs at http://localhost:8000/docs
```

### Run Dashboard
```bash
python main.py dashboard
# Dashboard will open at http://localhost:8501
```

## 📊 API Endpoints

- `GET /books` - Retrieve all books
- `GET /statistics` - Get database statistics
- `POST /scrape` - Trigger scraping process
- `GET /health` - Health check

## 🧪 Testing

```bash
pytest tests/ -v --cov=src
```

## 📁 Project Structure

```
automated-data-pipeline/
├── src/
│   ├── scraper/       # Web scraping modules
│   ├── database/      # Database models and connection
│   ├── api/           # FastAPI routes
│   └── utils/         # Utility functions
├── dashboard/         # Streamlit dashboard
├── tests/            # Unit tests
├── config/           # Configuration files
└── main.py           # CLI entry point
```

## 👨‍💻 Author

Jeffrey M. Dabo

## 📄 License

MIT License
