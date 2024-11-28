# tests/test_scraper.py
from config.settings import TABLE_ID, EXCEL_FILE, SHEET_NAMES
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from modules.scrape import Scraper
from config.settings import EXCEL_FILE, SHEET_NAMES
from pandas.testing import assert_frame_equal

class TestScraperGetStates(unittest.TestCase):
    @patch('modules.scrape.sheet_empty')
    @patch('modules.scrape.pd.read_excel')
    def test_get_states_existing_data(self, mock_read_excel, mock_sheet_empty):
        """
        Test that get_states loads data from Excel when it already exists.
        """
        # Arrange
        # Mock sheet_empty to return False, indicating the sheet is not empty
        mock_sheet_empty.return_value = False

        # Create a sample DataFrame to be returned by read_excel
        sample_data = {
            "States/UT's": ["State1", "State2"],
            "URL": ["http://example.com/state1", "http://example.com/state2"]
        }
        expected_df = pd.DataFrame(sample_data)
        mock_read_excel.return_value = expected_df

        # Create a mocked WebDriver (it should not be used in this test)
        mock_driver = MagicMock()

        # Instantiate the Scraper with the mocked driver
        scraper = Scraper(mock_driver, "http://example.com")

        # Act
        result_df = scraper.get_states()

        # Assert
        # Verify that sheet_empty was called with correct arguments
        mock_sheet_empty.assert_called_once_with(EXCEL_FILE, SHEET_NAMES[0])

        # Verify that pd.read_excel was called to load the data
        mock_read_excel.assert_called_once_with(EXCEL_FILE, sheet_name=SHEET_NAMES[0])

        # Verify that the result DataFrame matches the expected DataFrame
        assert_frame_equal(result_df.reset_index(drop=True), expected_df)

        # Additionally, ensure that the WebDriver's get method was NOT called
        mock_driver.get.assert_not_called()

    @patch('modules.scrape.sheet_empty')
    @patch('modules.scrape.WebDriverWait')
    @patch('modules.scrape.By')
    @patch('modules.scrape.EC')
    def test_get_states_no_existing_data(self, mock_ec, mock_by, mock_webdriver_wait, mock_sheet_empty):
        """
        Test that get_states performs scraping when no existing data is found.
        """
        # Arrange
        # Mock sheet_empty to return True, indicating the sheet is empty
        mock_sheet_empty.return_value = True

        # Mock WebDriverWait to bypass waiting (simulate successful load)
        mock_webdriver_wait.return_value.until.return_value = True

        # Create a mocked WebDriver
        mock_driver = MagicMock()

        # Instantiate the Scraper with the mocked driver
        scraper = Scraper(mock_driver, "http://example.com")

        # Mock the table and its elements
        mock_table = MagicMock()
        mock_header_row = MagicMock()
        mock_header_row.find_elements.return_value = [
            MagicMock(text="States/UT's"), MagicMock(text="Another Header")
        ]
        mock_table.find_element.return_value = mock_header_row

        # Mock data rows
        mock_row1 = MagicMock()
        mock_cell1 = MagicMock(text="State1")
        mock_link1 = MagicMock()
        mock_link1.get_attribute.return_value = "http://example.com/state1"
        mock_cell1.find_element.return_value = mock_link1
        mock_row1.find_elements.return_value = [mock_cell1, MagicMock(text="Data1")]

        mock_row2 = MagicMock()
        mock_cell2 = MagicMock(text="State2")
        mock_link2 = MagicMock()
        mock_link2.get_attribute.return_value = "http://example.com/state2"
        mock_cell2.find_element.return_value = mock_link2
        mock_row2.find_elements.return_value = [mock_cell2, MagicMock(text="Data2")]

        # Configure the table to return the header and two data rows
        mock_table.find_elements.return_value = [mock_row1, mock_row2]
        mock_driver.find_element.return_value = mock_table

        # Act
        result_df = scraper.get_states()

        # Assert
        # Verify that sheet_empty was called with correct arguments
        mock_sheet_empty.assert_called_once_with(EXCEL_FILE, SHEET_NAMES[0])

        # Verify that the WebDriver navigated to the correct URL
        mock_driver.get.assert_called_once_with("http://example.com")

        # Verify that the table was located
        mock_table.find_element.assert_called_once_with('id', TABLE_ID)

        # Define the expected DataFrame
        expected_data = {
            "States/UT's": ["State1", "State2"],
            "URL": ["http://example.com/state1", "http://example.com/state2"]
        }
        expected_df = pd.DataFrame(expected_data)

        # Verify that the result DataFrame matches the expected DataFrame
        assert_frame_equal(result_df.reset_index(drop=True), expected_df)

    @patch('modules.scrape.sheet_empty')
    @patch('modules.scrape.pd.read_excel')
    def test_get_states_excel_load_failure(self, mock_read_excel, mock_sheet_empty):
        """
        Test that get_states proceeds to scrape if loading from Excel fails.
        """
        # Arrange
        # Mock sheet_empty to return False, indicating the sheet is not empty
        mock_sheet_empty.return_value = False

        # Mock read_excel to raise an exception
        mock_read_excel.side_effect = Exception("Failed to load Excel")

        # Create a mocked WebDriver
        mock_driver = MagicMock()

        # Instantiate the Scraper with the mocked driver
        scraper = Scraper(mock_driver, "http://example.com")

        # Act
        result_df = scraper.get_states()

        # Assert
        # Verify that sheet_empty was called with correct arguments
        mock_sheet_empty.assert_called_once_with(EXCEL_FILE, SHEET_NAMES[0])

        # Verify that pd.read_excel was called to load the data
        mock_read_excel.assert_called_once_with(EXCEL_FILE, sheet_name=SHEET_NAMES[0])

        # Since loading failed, the method should proceed to scrape
        # Hence, the driver.get should be called
        mock_driver.get.assert_called_once_with("http://example.com")

        # Verify that if scraping succeeds, data is returned
        # For this test, we need to mock the scraping process as well
        # However, since it's beyond the scope, we assume it returns an empty DataFrame
        self.assertIsInstance(result_df, pd.DataFrame)