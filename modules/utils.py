# modules/utils.py

from pathlib import Path
import pandas as pd
from selenium.webdriver.chrome.service import Service
from selenium.webdriver import ChromeOptions
import selenium.webdriver as webdriver
from selenium.common.exceptions import WebDriverException
from config.settings import EXCEL_FILE, STATUS_FILE,CHROME_DRIVER_PATH, HEADLESS, logger
import time
import json
from datetime import datetime
import logging
from sqlalchemy import create_engine
from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

def update_status(status, message=""):
    """Update the scraper status in status.json located at the root directory."""
    try:
        with open(STATUS_FILE, "w") as f:
            json.dump({
                "status": status,
                "message": message,
                "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }, f)
    except Exception as e:
        logger.error(f"Failed to update status.json: {e}")
    
def begin_scraping_log():
    """
    Log the beginning of the scraping process with a prominently visible message and timestamp.
    Returns:
        datetime: The UTC start time of the scraping process.
    """
    start_time = datetime.utcnow()
    logger.info("##################################################")
    logger.info("### SCRAPING PROCESS STARTED AT %s UTC ###", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("##################################################")
    return start_time

def end_scraping_log(start_time):
    """
    Log the end of the scraping process with a prominently visible message and time taken.
    
    Args:
        start_time (datetime): The UTC start time of the scraping process.
    
    Returns:
        float: Time taken for the scraping process in seconds.
    """
    end_time = datetime.utcnow()
    time_taken = end_time - start_time  # This returns a timedelta object

    logger.info("##################################################")
    logger.info("### SCRAPING PROCESS ENDED AT %s UTC ###", end_time.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("### TOTAL DURATION: %s ###", str(time_taken))
    logger.info("##################################################")

    # Optionally, you can return the total seconds
    return time_taken.total_seconds()

def initialize_driver():
    """
    Initialize the Selenium WebDriver with specified options.

    Returns:
        webdriver.Chrome: Configured Selenium WebDriver instance.
    """
    options = ChromeOptions()
    options.add_argument("--log-level=3")  # Suppress unnecessary logs
    if HEADLESS:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
    try:
        logger.info("Initializing headless Chrome driver at path: %s", str(CHROME_DRIVER_PATH))
        service = Service(str(CHROME_DRIVER_PATH))
        driver = webdriver.Chrome(service=service, options=options)
        logger.info("Headless Chrome driver initialized successfully.")
        return driver
    except WebDriverException as e:
        logger.error(f"Error initializing WebDriver: {e}")
        raise

def sheet_empty(file_path: Path, sheet_name: str) -> bool:
    """
    Check if a specific sheet in an Excel file is empty or does not exist.

    Args:
        file_path (Path): Path to the Excel file.
        sheet_name (str): Name of the sheet to check.

    Returns:
        bool: True if the sheet is empty or does not exist, False otherwise.
    """
    logger.info(f"Checking if sheet '{sheet_name}' is empty in '{file_path}'...")
    excel = pd.ExcelFile(file_path)
    df = excel.parse(sheet_name)
    is_empty = df.empty
    logger.info(f"Sheet '{sheet_name}' empty: {is_empty}")
    return is_empty

def postgres_table_empty(states_table) -> bool:
    """
    Check if postgres states table is empty
    """
   


def create_excel_file(file_path: Path, sheets: list):
    """
    Create an Excel file with specified sheet names if it doesn't exist.

    Args:
        file_path (Path): Path to the Excel file.
        sheets (list): List of sheet names to create.
    """
    logger.info(f"Creating Excel file '{file_path}' with sheets: {sheets}")
    try:
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            for sheet in sheets:
                pd.DataFrame().to_excel(writer, sheet_name=sheet, index=False)
                logger.info(f"Sheet '{sheet}' created.")
        logger.info(f"Excel file '{file_path}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating Excel file '{file_path}': {e}")
        raise

def verify_excel_file(file_path: Path, sheets: list):
    """
    Verify if an Excel file exists with specified sheets, creating it if necessary.

    Args:
        file_path (Path): Path to the Excel file.
        sheets (list): List of sheet names to verify.
    """
    existing_sheets = pd.ExcelFile(file_path).sheet_names
    missing_sheets = [sheet for sheet in sheets if sheet not in existing_sheets]
    if missing_sheets:
        logger.info("Adding missing sheets to '%s': %s", file_path, missing_sheets)
        with pd.ExcelWriter(file_path, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            for sheet in missing_sheets:
                pd.DataFrame().to_excel(writer, sheet_name=sheet, index=False)
                logger.info("Sheet '%s' added.", sheet)

def count_records() -> pd.DataFrame:
    """
    Count the actual number of well records records per state.

    Args:

    Returns:
        pd.DataFrame: DataFrame with 'States/UT's' and 'Actual_Records'.
    """
    try:
        panchayats_df = pd.read_excel(EXCEL_FILE, sheet_name="panchayats")
        actual_counts = panchayats_df.groupby("States/UT's").size().reset_index(name='Actual_Records')
        logger.info("Actual records counted per state.")
        return actual_counts
    except Exception as e:
        logger.error(f"Error counting panchayat records: {e}")
        return pd.DataFrame()

def get_expected_counts() -> pd.DataFrame:
    """
    Retrieve the expected number of well records per state from the 'States/UT's' sheet.

    Args:

    Returns:
        pd.DataFrame: DataFrame with 'States/UT's' and 'Expected_Records'.
    """
    try:
        states_df = pd.read_excel(EXCEL_FILE, sheet_name="states")
        expected_counts = states_df[["States/UT\'s", "No. of Well Covered"]].rename(
            columns={'No. of Well Covered': 'Expected_Records'}
        )
        logger.info("Expected records retrieved from 'States/UT's' sheet.")
        return expected_counts
    except Exception as e:
        logger.error(f"Error retrieving expected counts: {e}")
        return pd.DataFrame()
    
def get_db_connection():
    return engine.connect()

def get_db_session():
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DATABASE_URL,echo=False)
    
    # Create tables if they don't exist
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    return Session()