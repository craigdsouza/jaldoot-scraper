# Scraper for Pre-monsoon and Post monsoon groundwater level data collected through the Jaldoot programme
The Jaldoot programme data has been collected since Post-monsoon 2022, and is published on the MGNREGA platform "https://mnregaweb4.nic.in/jaldootweb/Home.aspx". The web pages are hierarchical in structure. To access individual well level data requires clicking through state, district, block links as is the case with many Govt of India websites. 

# How it works
Scrapes through the states table first, gathering all the state URLs, then loops through each state link and recursively scrapes district links and block links. Finally the clicking through individual block (sub-district) links shows all well records from that block.

[screenshot]


# Setup checklist
- .env file
  - CHROME_DRIVER_PATH=chromedriver.exe
  - BASE_URL= "URL for the desired year"
  - HEADLESS = True
  - DEBUG = False
- modules/settings.py
  - 