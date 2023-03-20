# w4h-etl-container
Python extract-transform-load data pipeline for creating forecasts of [Universal Thermal Climate Index](https://utci.lobelia.earth/what-is-utci) (UTCI) and [Wet Bulb Globe Temperature](https://www.weather.gov/news/211009-WBGT) (WBGT) and creating images of global daily UTCI highs and lows using data from the [Global Forecast System](https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast) (GFS). Run periodically as a Docker container in Google Cloud Run to maintain the data for  [weatherforhumans.com](https://www.weatherforhumans.com).

## Operational Overview
- Verifies that database is not already updating via the "status" record
- Web scrapes for the OpenDAP data endpoint for the latest GFS data
- if the latest data endpoint differs from the last successful endpoint used, ETL:
	- update database "status" record with isUpdating = true to prevent additional containers from running during ETL
	- To minimize memory consumption, for each weather parameter calculation:
		- download any necessary raw data for calculating a necessary weather parameter
		- calculate the weather parameter
		- drop any data that is no longer needed
	- After calculating UTCI and WBGT forecasts, download old data from a GCP storage bucket and merge it with the new, prefering newer data and clipping out data earlier than the earliest necessary time
	- To save space, encode each 64-bit UTCI, 64-bit WBGT, and 64-bit time record into a single 32-bit int carrying UTCI, WBGT, and forecast-start-time-offset.
	- Create a list of records to upload to the database, masking out records far from land, and bulk-upload in chunks
	- Update the database "status" record to notify backend instances of updated forecasts
	- Update the GCP storage bucket to persist the latest forecasts to the next ETL run
	- Shift the data in time according to longitude hour angle to find daily "highs" and "lows"
	- Create a base figure for global daily "highs" and "lows"
	- For each day with complete data (24hrs of data at every lat and lon):
		- for "highs" and "lows":
			- plot contours on the base figure
			- create a png file of the figure
			- upload the png file to cdn at the appropriate endpoint
			- clear contours from the base figure for next plot and remove the png file from memory
		- update the database "status" record to notify backend instances of the latest charts available for that date
	- update the database "status" record to reflect that database is no longer updating

## Other features:
- Retries network requests with an exponential backoff
- Will send a text message to a developer if the web scraper is unable to scrape through the NOMADS data directories as expected
- Timer class (utils.py) allows for easy logging of wall and cpu time for sections of code
