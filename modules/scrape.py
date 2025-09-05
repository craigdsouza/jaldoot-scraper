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

            # If there's a second header row (often rendered with th) containing the
            # depth-band subheaders, replace the last group header with those.
            try:
                second_header_candidates = state_table.find_elements(By.CSS_SELECTOR, "tr.header + tr")
                if second_header_candidates:
                    sub_header_cells = second_header_candidates[0].find_elements(By.XPATH, ".//th|.//td")
                    sub_headers = [c.text.strip() for c in sub_header_cells if c.text.strip()]
                    has_depth_bands = any(h.endswith("Feet") or h.startswith(">") for h in sub_headers)
                    if has_depth_bands and len(headers) > 0:
                        # Drop the last group label and extend with the real sub-headers
                        headers = headers[:-1] + sub_headers
            except Exception:
                pass

            # Some seasons render a second header row for wells depth bands (0-2, 3-5, 6-10, >10)
            # Our original code ignored it, causing missing columns. If the data rows have
            # more columns than the first header row, extend headers with the expected labels.
            all_rows_tmp = state_table.find_elements(By.TAG_NAME, "tr")
            # Find the first non-header data row to compare column counts
            candidate_data_row = None
            for r in all_rows_tmp:
                if r != header_row:
                    candidate_data_row = r
                    break
            if candidate_data_row is not None:
                data_col_count = len(candidate_data_row.find_elements(By.TAG_NAME, "td"))
                if data_col_count > len(headers):
                    missing = data_col_count - len(headers)
                    expected_tail = ["0-2 Feet", "3-5 Feet", "6-10 Feet", ">10 Feet"]
                    headers.extend(expected_tail[:missing])
            logger.info("State Headers extracted: %s", headers)

            all_rows = state_table.find_elements(By.TAG_NAME, "tr")
            data_rows = [row for row in all_rows if row != header_row]

            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                if cols:
                    # Map by index so we don't drop any trailing well-depth columns
                    for i in range(min(len(cols), len(headers))):
                        header = headers[i]
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
            # Drop the "#" column if present
            if not df.empty and df.columns.size > 1 and df.columns[0].strip() in ["#", "Sr."]:
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
        stop=stop_after_attempt(5),
        wait=wait_fixed(10),
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

            # Try dynamic table discovery: configured id, common alternates, then heuristic scan
            def try_find_by_ids(driver):
                candidate_ids = [TABLE_ID, "example", "ContentPlaceHolder1_gvReport", "ContentPlaceHolder1_GridView1"]
                tried = []
                for cid in [c for c in candidate_ids if c]:
                    tried.append(cid)
                    try:
                        tbl = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.ID, cid))
                        )
                        # Do not require a specific header structure here; header parsing is handled later
                        return tbl, cid, tried
                    except Exception:
                        continue
                return None, None, tried

            table, matched_id, tried_ids = try_find_by_ids(self.driver)
            if table is None:
                # Heuristic: any table with header containing typical panchayat columns
                all_tables = self.driver.find_elements(By.TAG_NAME, "table")
                debug_tables = []
                for t in all_tables:
                    tid = t.get_attribute("id") or "<no-id>"
                    try:
                        header_cells = t.find_elements(By.CSS_SELECTOR, "thead th")
                        if not header_cells:
                            header_cells = t.find_elements(By.CSS_SELECTOR, "tr.header th, tr.header td")
                        if not header_cells:
                            header_cells = t.find_elements(By.CSS_SELECTOR, "thead tr:first-child th, thead tr:first-child td")
                        if not header_cells:
                            header_cells = t.find_elements(By.CSS_SELECTOR, "tr:first-child th, tr:first-child td")
                        headers = [c.text.strip() for c in header_cells if c.text.strip()]
                    except Exception:
                        headers = []
                    debug_tables.append((tid, headers))
                    # Match if we see at least two of these expected headers
                    expected_hits = sum(1 for key in ["Well ID", "Village", "Panchayat"] if any(key.lower() in h.lower() for h in headers))
                    if expected_hits >= 2 and headers:
                        table = t
                        matched_id = tid
                        break

                logger.info("Panchayat table discovery: tried_ids=%s, found_tables=%s",
                            tried_ids, [(tid, headers[:6]) for tid, headers in debug_tables])

            if table is None:
                raise TimeoutException("Could not locate panchayat table by known IDs or header heuristics")

            # With a table chosen, wait for header OR at least one data cell
            def has_header_or_cells(t):
                try:
                    if t.find_elements(By.CSS_SELECTOR, "thead th"):
                        return True
                    if t.find_elements(By.CSS_SELECTOR, "tr.header th, tr.header td"):
                        return True
                    if t.find_elements(By.CSS_SELECTOR, "tbody tr td"):
                        return True
                    if t.find_elements(By.CSS_SELECTOR, "tr:not(thead tr) td"):
                        return True
                except Exception:
                    return False
                return False

            WebDriverWait(self.driver, 60).until(lambda d: has_header_or_cells(table))

            # Log a summary of detected headers for diagnostics
            header_cells_dbg = table.find_elements(By.CSS_SELECTOR, "thead th")
            if not header_cells_dbg:
                header_cells_dbg = table.find_elements(By.CSS_SELECTOR, "tr.header th, tr.header td")
            header_dbg = [c.text.strip() for c in header_cells_dbg if c.text.strip()]
            logger.info("Page loaded; using table id='%s' for panchayats in block: %s; headers=%s", matched_id, block, header_dbg[:8])
        except (TimeoutException, WebDriverException) as e:
            # Extra diagnostics to help analyze mismatched structures
            try:
                all_tables = self.driver.find_elements(By.TAG_NAME, "table")
                diag = []
                for t in all_tables:
                    tid = t.get_attribute("id") or "<no-id>"
                    cls = t.get_attribute("class") or ""
                    try:
                        header_row = t.find_element(By.CSS_SELECTOR, "tr.header")
                        header_cells = header_row.find_elements(By.XPATH, ".//th|.//td")
                        headers = [c.text.strip() for c in header_cells if c.text.strip()]
                    except Exception:
                        headers = []
                    diag.append({"id": tid, "class": cls, "headers": headers[:8]})
                logger.error("Panchayat table discovery failed. Available tables: %s", diag)
            except Exception:
                pass
            logger.error("Error loading page OR locating table for panchayats: %s", e)
            raise  # Trigger Tenacity retry

        data = []
        try:
            # Use the previously found table element
            panchayat_table = table
            # Extract headers from thead or first row; do not assume 'tr.header'
            header_cells = panchayat_table.find_elements(By.CSS_SELECTOR, "thead tr th, thead tr td")
            if not header_cells:
                header_cells = panchayat_table.find_elements(By.CSS_SELECTOR, "tr.header th, tr.header td")
            if not header_cells:
                header_cells = panchayat_table.find_elements(By.CSS_SELECTOR, "tr:first-child th, tr:first-child td")
            headers = [cell.text.strip() for cell in header_cells]
            # logger.info("Panchayat Headers extracted: %s", headers)
            # Prefer tbody rows for data; fall back to any tr with td cells
            data_rows = panchayat_table.find_elements(By.CSS_SELECTOR, "tbody tr")
            if not data_rows:
                all_rows = panchayat_table.find_elements(By.TAG_NAME, "tr")
                data_rows = [r for r in all_rows if len(r.find_elements(By.TAG_NAME, "td")) > 0]

            for row in data_rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                row_data = {}
                if cols and headers:
                    limit = min(len(cols), len(headers))
                    for i in range(limit):
                        row_data[headers[i]] = cols[i].text
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