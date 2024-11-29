# dashboard.py

import streamlit as st
import json
from pathlib import Path
import time
from config.settings import STATUS_FILE, EXCEL_FILE, LOG_FILE  # Import LOG_FILE
from modules.utils import count_records, get_expected_counts
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def load_status():
    """Load the scraper status from status.json."""
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {"status": "Error", "message": "Invalid JSON format."}
        except Exception as e:
            return {"status": "Error", "message": str(e)}
    else:
        return {"status": "Unknown", "message": "status.json not found."}

def display_status(status_data):
    """Display the scraper status based on the loaded data."""
    status = status_data.get("status", "Unknown")
    message = status_data.get("message", "")

    if status == "Running":
        st.success("🟢 Scraper is **Running**.")
    elif status == "Stopped":
        st.success("🟢 Scraper has **Stopped** successfully.")
    elif status == "Error":
        st.error(f"🔴 Scraper encountered an **Error**: {message}")
    else:
        st.warning("⚪ Scraper status is **Unknown**.")

    # Display the last updated time
    st.write(f"**Last Updated:** {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

def load_counts(excel_file: Path):
    """Load expected and actual counts for each state."""
    expected_counts = get_expected_counts()
    actual_counts = count_records()
    
    if expected_counts.empty or actual_counts.empty:
        st.error("Unable to load counts data.")
        return pd.DataFrame()
    
    # Merge on "States/UT's"
    merged_counts = pd.merge(expected_counts, actual_counts, on="States/UT's", how="left")
    merged_counts['Actual_Records'] = merged_counts['Actual_Records'].fillna(0).astype(int)
    
    return merged_counts

def plot_counts(merged_counts: pd.DataFrame):
    """Plot expected vs actual records per state."""
    try:
        # Set the figure size
        plt.figure(figsize=(12, 8))
        
        # Create a bar plot for Expected Records
        sns.barplot(
            data=merged_counts,
            x="States/UT's",
            y="Expected_Records",
            color='blue',
            label='Expected Records'
        )
        
        # Overlay Actual Records
        sns.barplot(
            data=merged_counts,
            x="States/UT's",
            y="Actual_Records",
            color='green',
            label='Actual Records'
        )
        
        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45, ha='right')
        
        # Add labels and title
        plt.xlabel("States/UT's")
        plt.ylabel("Number of Records")
        plt.title("Expected vs Actual Panchayat Records per State")
        plt.legend()
        
        # Adjust layout
        plt.tight_layout()
        
        # Display the plot in Streamlit
        st.pyplot(plt)
        
        # Clear the current figure
        plt.clf()
        
    except Exception as e:
        st.error(f"Error plotting counts: {e}")

def main():
    st.title("🛠️ Scraper Status Dashboard")

    # Load the current status
    status_data = load_status()

    # Display the status
    display_status(status_data)

    # Add a refresh button
    if st.button("Refresh"):
        st.rerun()

    st.markdown("---")  # Separator

    st.header("📊 Records Status Chart")
    
    # Load counts data
    counts_df = load_counts(EXCEL_FILE)
    
    if not counts_df.empty:
        # Display the DataFrame (optional)
        st.dataframe(counts_df)
        
        # Plot the chart
        plot_counts(counts_df)
    else:
        st.warning("Counts data is unavailable.")
    
    st.markdown("---")  # Separator for the log file download section

    st.header("📥 Download Logs")

    if LOG_FILE.exists():
        with open(LOG_FILE, "rb") as log_file:
            log_data = log_file.read()
        st.download_button(
            label="Download Latest Log File",
            data=log_data,
            file_name="jaldoot.log",
            mime="text/plain"
        )
    else:
        st.warning("Log file not found.")

    st.markdown("---")  # Separator for the data download section

    st.header("📥 Download Data")

    if EXCEL_FILE.exists():
        with open(EXCEL_FILE, "rb") as excel_file:
            excel_data = excel_file.read()
        st.download_button(
            label="Download Latest Data File",
            data=excel_data,
            file_name="jaldoot-remote.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning("Data file not found.")

if __name__ == "__main__":
    main()