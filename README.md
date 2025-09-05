# Jaldoot Scraper - Groundwater Data Collection

A robust Python-based web scraper for collecting pre-monsoon and post-monsoon groundwater level data from India's MGNREGA Jaldoot platform. Features real-time monitoring dashboard, PostgreSQL storage, and cloud deployment support.

## 📊 Overview

The Jaldoot programme data has been collected since Post-monsoon 2022 and is published on the MGNREGA platform at [https://mnregaweb4.nic.in/jaldootweb/Home.aspx](https://mnregaweb4.nic.in/jaldootweb/Home.aspx).

### How It Works
The scraper navigates through a hierarchical structure:
1. **States** → Gathers all state URLs from the main page
2. **Districts** → Recursively scrapes district links within each state
3. **Blocks** → Extracts block/sub-district links from each district
4. **Panchayats** → Collects individual well records from each block

None of the state, district, or block level pages implement pagination, making the scraping process deterministic.

![Scraping Flow](https://github.com/user-attachments/assets/cac857b2-be22-4eea-a70d-5ad6ca8490a8)

## 🚀 Key Features

- **Hierarchical Scraping**: Automated navigation through state → district → block → panchayat levels
- **Real-time Dashboard**: Streamlit-based monitoring with progress visualization
- **Robust Error Handling**: Tenacity-based retry logic with exponential backoff
- **Multi-format Storage**: PostgreSQL database + Excel/CSV exports
- **Headless Operation**: Configurable headless Chrome for server deployment
- **Resume Capability**: Intelligent resume from last successful scrape
- **Cloud Ready**: Optimized for GCP deployment (Cloud Run, Compute Engine)

## 📋 Prerequisites

- Python 3.8+
- PostgreSQL database
- Google Chrome browser
- Chrome WebDriver (matching your Chrome version)

## 🛠️ Local Development Setup

### 1. Clone and Install Dependencies

```bash
git clone https://github.com/craigdsouza/jaldoot-scraper.git
cd jaldoot-scraper
pip install -r requirements.txt
```

### 2. Chrome WebDriver Setup

Download the Chrome WebDriver that matches your Chrome browser version:

1. Check your Chrome version: `chrome://version`
2. Download from [Chrome for Testing](https://googlechromelabs.github.io/chrome-for-testing/#stable)
3. Extract and place `chromedriver.exe` in the project root directory

### 3. PostgreSQL Setup

Follow these steps to create the database, user, and apply the schema defined in `sql/add_primary_keys.sql`.

#### Step 1: Create Database and User
```bash
# Connect to PostgreSQL as superuser
psql -U postgres -h localhost

# In the psql prompt:
CREATE DATABASE jaldoot_db;
CREATE USER jaldoot_user WITH PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE jaldoot_db TO jaldoot_user;

-- Switch to the new database and grant schema permissions
\c jaldoot_db
GRANT CREATE, USAGE ON SCHEMA public TO jaldoot_user;
ALTER SCHEMA public OWNER TO jaldoot_user;
\q
```

#### Step 2: Apply Schema
```bash
# Connect to the new database as the app user
psql -U jaldoot_user -d jaldoot_db -h localhost

# In the psql prompt:
\i ./sql/add_primary_keys.sql
\q
```

This script creates the `states`, `districts`, and `blocks` tables, sets primary keys, unique indexes, and grants, aligning ownership and sequence permissions to `jaldoot_user`.

#### Step 3: Verify
```bash
# List tables
psql -U jaldoot_user -d jaldoot_db -c "\\dt"

# Describe key tables
psql -U jaldoot_user -d jaldoot_db -c "\\d states"
psql -U jaldoot_user -d jaldoot_db -c "\\d districts"
psql -U jaldoot_user -d jaldoot_db -c "\\d blocks"
```

### 4. Environment Configuration

Create a minimal `.env` in the project root (only what the current CLI needs):

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=jaldoot_db
DB_USER=jaldoot_user
DB_PASSWORD=your_secure_password

# Chrome / Runtime
CHROME_DRIVER_PATH=chromedriver.exe
HEADLESS=true
DEBUG=false

# Logging
LOG_FILE=logs/jaldoot.log
STATUS_FILE=status.json

# Scraping (optional override; defaults are sensible for current seasons)
# TABLE_ID=ContentPlaceHolder1_gvReport
```

### 5. Run the Scraper
See CLI usage in `docs/cli.md` for scraping and URL backfill commands.

