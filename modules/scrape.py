from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from config.settings import TABLE_ID, logger
import pandas as pd

class Scraper:
    def __init__(self, driver, base_url):
        """
        Initialize the Scraper with a WebDriver instance and base URL.

        Args:
            driver (webdriver.Chrome): Selenium WebDriver instance.
            base_url (str): The base URL to start scraping from.
        """
        self.driver = driver
        self.base_url = base_url

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(6),
        retry=retry_if_exception_type((TimeoutException, WebDriverException)),
        reraise=True
    )
    def get_states(self):
        """
        Scrape the states from the base URL.

        Returns:
            pd.DataFrame: DataFrame containing states and their URLs.
        """
        logger.info("Beginning get_states, loading page: %s", self.base_url)
        try:
            self.driver.get(self.base_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, TABLE_ID))
            )
            logger.info("Page loaded and State table located!")
        except (TimeoutException, WebDriverException) as e:
            logger.error(f"Error loading page OR locating table: {e}")
            raise  # Trigger Tenacity retry

        data = []
        try:
            state_table = self.driver.find_element(By.ID, TABLE_ID)
            header_row = state_table.find_element(By.CSS_SELECTOR, "tr.header")
            headers = [td.text for td in header_row.find_elements(By.TAG_NAME, "td")]
            logger.info("State Headers extracted: %s", headers)

            all_rows = state_table.find_elements(By.TAG_NAME, "tr")
            data_rows = [row for row in all_rows if row != header_row]

            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                if cols:
                    for i, header in enumerate(headers):
                        row_data[header] = cols[i].text
                        if header == "States/UT's":
                            try:
                                url = cols[i].find_element(By.TAG_NAME, "a").get_attribute("href")
                                row_data['URL'] = url
                            except Exception:
                                logger.warning(f"No URL found for {cols[i].text} under {header}")
                data.append(row_data)

            df = pd.DataFrame(data)
            df = df.dropna()
            if not df.empty and df.columns.size > 1:
                df = df.drop(df.columns[0], axis=1)
            logger.info(f"Extracted {len(df)} State URLs successfully")
            return df
        except Exception as e:
            logger.error(f"Error in get_states: {e}")
            return pd.DataFrame()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(6),
        retry=retry_if_exception_type((TimeoutException, WebDriverException)),
        reraise=True
    )
    def get_districts(self, state, url):
        """
        Scrape the districts for a given state.

        Args:
            state (str): Name of the state.
            url (str): URL to scrape districts from.

        Returns:
            pd.DataFrame: DataFrame containing districts and their URLs.
        """
        logger.info("Beginning get_districts for state: %s, loading page: %s", state, url)
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, TABLE_ID))
            )
            logger.info("Page loaded and table located for districts in state: %s", state)
        except (TimeoutException, WebDriverException) as e:
            logger.error("Error loading page OR locating table for districts: %s", e)
            raise  # Trigger Tenacity retry

        data = []
        try:
            district_table = self.driver.find_element(By.ID, TABLE_ID)
            header_row = district_table.find_element(By.CSS_SELECTOR, "tr.header")
            headers = [td.text for td in header_row.find_elements(By.TAG_NAME, "td")]
            logger.info("District Headers extracted: %s", headers)

            all_rows = district_table.find_elements(By.TAG_NAME, "tr")
            data_rows = [row for row in all_rows if row != header_row]

            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                if cols:
                    for i, header in enumerate(headers):
                        row_data[header] = cols[i].text
                        if header == "District":
                            try:
                                url = cols[i].find_element(By.TAG_NAME, "a").get_attribute("href")
                                row_data['URL'] = url
                            except Exception:
                                logger.warning(f"No URL found for {cols[i].text} under {header}")
                data.append(row_data)

            df = pd.DataFrame(data)
            df = df.dropna()
            if not df.empty and df.columns.size > 1:
                df = df.drop(df.columns[0], axis=1)
            df.insert(0, "States/UT\'s", state)
            logger.info("Extracted %d district URLs for state: %s successfully", len(df), state)
            return df
        except Exception as e:
            logger.error("Error in get_districts: %s", e)
            return pd.DataFrame()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_fixed(6),
        retry=retry_if_exception_type((TimeoutException, WebDriverException)),
        reraise=True
    )
    def get_blocks(self, state, district, url):
        """
        Scrape the blocks for a given district.

        Args:
            state (str): Name of the state.
            district (str): Name of the district.
            url (str): URL to scrape blocks from.

        Returns:
            pd.DataFrame: DataFrame containing blocks and their URLs.
        """
        logger.info("Beginning get_blocks for state: %s, district: %s, loading page: %s", state, district, url)
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, TABLE_ID))
            )
            logger.info("Page loaded and table located for blocks in district: %s", district)
        except (TimeoutException, WebDriverException) as e:
            logger.error("Error loading page OR locating table for blocks: %s", e)
            raise  # Trigger Tenacity retry

        data = []
        try:
            block_table = self.driver.find_element(By.ID, TABLE_ID)
            header_row = block_table.find_element(By.CSS_SELECTOR, "tr.header")
            headers = [td.text for td in header_row.find_elements(By.TAG_NAME, "td")]
            logger.info("Block Headers extracted: %s", headers)

            all_rows = block_table.find_elements(By.TAG_NAME, "tr")
            data_rows = [row for row in all_rows if row != header_row]

            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                if cols:
                    for i, header in enumerate(headers):
                        row_data[header] = cols[i].text
                        if header == "Block":
                            try:
                                url = cols[i].find_element(By.TAG_NAME, "a").get_attribute("href")
                                row_data['URL'] = url
                            except Exception:
                                logger.warning(f"No URL found for {cols[i].text} under {header}")
                data.append(row_data)

            df = pd.DataFrame(data)
            df = df.dropna()
            if not df.empty and df.columns.size > 1:
                df = df.drop(df.columns[0], axis=1)
            df.insert(0, 'District', district)
            df.insert(0, "States/UT\'s", state)
            logger.info("Extracted %d block URLs for district: %s successfully", len(df), district)
            return df
        except Exception as e:
            logger.error("Error in get_blocks: %s", e)
            return pd.DataFrame()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(6),
        retry=retry_if_exception_type((TimeoutException, WebDriverException)),
        reraise=True
    )
    def get_panchayats(self, state, district, block, url):
        """
        Scrape the panchayats for a given block.

        Args:
            state (str): Name of the state.
            district (str): Name of the district.
            block (str): Name of the block.
            url (str): URL to scrape panchayats from.

        Returns:
            pd.DataFrame: DataFrame containing panchayats.
        """
        logger.info("Beginning get_panchayats for state: %s, district: %s, block: %s, loading page: %s", state, district, block, url)
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, TABLE_ID))
            )
            logger.info("Page loaded and table located for panchayats in block: %s", block)
        except (TimeoutException, WebDriverException) as e:
            logger.error("Error loading page OR locating table for panchayats: %s", e)
            raise  # Trigger Tenacity retry

        data = []
        try:
            panchayat_table = self.driver.find_element(By.ID, TABLE_ID)
            header_row = panchayat_table.find_element(By.CSS_SELECTOR, "tr.header")
            headers = [th.text for th in header_row.find_elements(By.TAG_NAME, "th")]
            # logger.info("Panchayat Headers extracted: %s", headers)

            all_rows = panchayat_table.find_elements(By.TAG_NAME, "tr")
            data_rows = [row for row in all_rows if row != header_row]

            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                if cols:
                    for i, header in enumerate(headers):
                        row_data[header] = cols[i].text
                row_data['URL'] = url
                data.append(row_data)

            df = pd.DataFrame(data)
            df = df.dropna()
            if not df.empty and df.columns.size > 1:
                df = df.drop(df.columns[0], axis=1)
            df.rename(columns={'State': "States/UT's"}, inplace=True)
            df.drop(columns=['Image'], inplace=True, errors='ignore')
            return df
        except Exception as e:
            logger.error("Error in get_panchayats: %s", e)
            return pd.DataFrame()