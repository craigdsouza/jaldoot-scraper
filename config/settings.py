# config/settings.py

from pathlib import Path
import os
import logging
from dotenv import load_dotenv

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# Load environment variables from .env file
BASE_DIR = Path(__file__).resolve().parent.parent  # Root directory
ENV_FILE = BASE_DIR / '.env'
load_dotenv(dotenv_path=ENV_FILE)

# Load configurations from environment variables with defaults
CHROME_DRIVER_PATH = Path(os.getenv("CHROME_DRIVER_PATH", "chromedriver.exe"))
STATUS_FILE = BASE_DIR / os.getenv("STATUS_FILE", "status.json")
HEADLESS = os.getenv("HEADLESS", "True") == "True"
TABLE_ID = os.getenv("TABLE_ID", "default_table_id")
EXCEL_FILE = BASE_DIR / os.getenv("EXCEL_FILE_PATH", "data/jaldoot.xlsx")
LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "logs/jaldoot.log")
BASE_URL = os.getenv("BASE_URL", "http://defaulturl.com")

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
