# USDR-Project
Project to enhance and analyze data from the U.S. Digital Registry (USDR) using platform APIs.

**USDR and its API documentation:** https://usdigitalregistry.digitalgov.gov

**Progress:**

- [x] Pull records from USDR API
- [x] Integrate Twitter API
- [x] Integrate Facebook API
- [ ] Integrate Google API for YouTube
- [x] Create basic analysis report
- [ ] Create interactive dashboard app

**Installation and Use:**

1. Rename `settings_example.py` to `settings.py` and add your own platform API keys
2. Use the Python module: `import usdr`
3. Run the dashboard app: `$ python app.py`

**Main Functions:**

`usdr.fetchUSDR()` - fetch U.S. Digital Registry social media records, save locally as a .json, and return as a Pandas dataframe

`usdr.loadUSDR()` - load previously saved USDR .json record as a dataframe

`usdr.fetchTwitter(username_list)` - fetch Twitter API records for a list of usernames, save locally as a .json, and return as a Pandas dataframe

`usdr.loadTwitter()` - load previously saved Twitter .json record as a dataframe 

`usdr.fetchFacebook(url_list)` - fetch Facebook API records for a list of Facebook URLs, fetch API records for the resulting Facebook IDs, save both locally as .json files, and return the final results as a Pandas dataframe

`usdr.loadFacebook()` - load previously saved Facebook .json record as a dataframe 

**Helper Functions:**

`usdr.get_username(url)` - determines platform and uses regex to parse a username from a URL if possible
