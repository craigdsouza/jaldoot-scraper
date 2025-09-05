# config/settings.py

from pathlib import Path
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file BEFORE reading any env-dependent settings
BASE_DIR = Path(__file__).resolve().parent.parent  # Root directory
ENV_FILE = BASE_DIR / '.env'
load_dotenv(dotenv_path=ENV_FILE)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Load configurations from environment variables with defaults
CHROME_DRIVER_PATH = Path(os.getenv("CHROME_DRIVER_PATH", "chromedriver.exe"))
STATUS_FILE = BASE_DIR / os.getenv("STATUS_FILE", "status.json")
HEADLESS = os.getenv("HEADLESS", "True") == "True"
TABLE_ID = os.getenv("TABLE_ID", "default_table_id")
EXCEL_FILE = BASE_DIR / os.getenv("EXCEL_FILE_PATH", "data/jaldoot.xlsx")
LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "logs/jaldoot.log")
# Season-to-URL mapping for automatic URL selection
SEASON_URLS = {
    "post-monsoon-2022": "https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WaterCoveredReport.aspx",
    "pre-monsoon-2023": "https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WaterLevelReport2023.aspx",
    "post-monsoon-2023": "https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReport.aspx",
    "pre-monsoon-2024": "https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReportPre_2024.aspx",
    "post-monsoon-2024": "https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReportPost_2024.aspx",
    "pre-monsoon-2025": "https://mnregaweb4.nic.in/Jaldootweb/ReportABA/WellCoveredReportPre_2025.aspx",
}

SEASON = os.getenv("SEASON", "unknown-season")  # Track data collection season

# Automatic URL selection based on season, or manual override
AUTO_URL = os.getenv("AUTO_URL", "true").lower() == "true"
if AUTO_URL and SEASON in SEASON_URLS:
    BASE_URL = SEASON_URLS[SEASON]
    print(f"🔗 Auto-selected URL for {SEASON}: {BASE_URL}")
else:
    BASE_URL = os.getenv("BASE_URL", "http://defaulturl.com")
    print(f"🔗 Using manual URL: {BASE_URL}")

TEST_MODE = os.getenv("TEST_MODE", "False")  # Stop after states for testing

# Define sheet names
SHEET_NAMES = os.getenv("SHEET_NAMES", "states,districts,blocks,panchayats").split(',')

# Ensure the logs directory exists
LOG_DIR = LOG_FILE.parent
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Configure centralized logging
logging.basicConfig(
    filename=LOG_FILE,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Create a custom logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set the desired logging level

# Create handlers
console_handler = logging.StreamHandler()
file_handler = logging.FileHandler(LOG_FILE, mode='a')  # 'a' for append mode

# Set levels for handlers (optional, inherits from logger if not set)
console_handler.setLevel(logging.INFO)
file_handler.setLevel(logging.INFO)

# Create a formatter and set it for both handlers
formatter = logging.Formatter(
    fmt='%(asctime)s - %(module)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to the logger
if not logger.hasHandlers():
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# Enable console logging only if .env DEBUG is set to True
DEBUG = os.getenv("DEBUG", "False") == "True"

if DEBUG:
    logger.setLevel(logging.DEBUG)
    console_handler.setLevel(logging.DEBUG)
    logger.debug("Debug mode enabled.")
