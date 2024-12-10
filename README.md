# Scraper for Pre-monsoon and Post monsoon groundwater level data collected through the Jaldoot programme
The Jaldoot programme data has been collected since Post-monsoon 2022, and is published on the MGNREGA platform "https://mnregaweb4.nic.in/jaldootweb/Home.aspx". The web pages are hierarchical in structure. To access individual well level data requires clicking through state, district, block links as is the case with many Govt of India websites. 

# How it works
Scrapes through the states table first, gathering all the state URLs, then loops through each state link and recursively scrapes district links and block links. Finally the clicking through individual block (sub-district) links shows all well records from that block. None of the state,district,block level pages implement pagination.

![image](https://github.com/user-attachments/assets/cac857b2-be22-4eea-a70d-5ad6ca8490a8)

# Quickstart
# Clone this repo
use git clone https://github.com/craigdsouza/jaldoot-scraper.git

## Chrome Driver
This can be found [https://googlechromelabs.github.io/chrome-for-testing/](https://googlechromelabs.github.io/chrome-for-testing/#stable)
Make sure to check your version of Chrome and find the compatible driver, pick the Stable one if you're unsure.
Download, unzip and save the exe to your project root folder.

## Postgresql


## Run main.py
Use the command python main.py

## Check logs
Log file is stored in logs/jaldoot.log , use this to monitor progress.
Alternatively, you can use the streamlit dashboard, with streamlit run dashboard.py

# Setup checklist
- .env file
  - CHROME_DRIVER_PATH=chromedriver.exe
  - BASE_URL= URL_FOR_DESIRED_YEAR     # SET THIS VALUE
  - HEADLESS = True
  - DEBUG = False
 
For any feedback, comments you can reach me at craig.dsouza@ifmr.ac.in
