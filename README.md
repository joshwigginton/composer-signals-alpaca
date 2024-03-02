# Local and GCP Composer Signal Alpaca Trader

This project is a trading system built with the Alpaca Trade API. It uses signals from the [Signals project](https://github.com/scarplus/signals). 

## Description

This project supports both local and Google Cloud Platform (GCP) based trading. It uses a Google Cloud Function to trigger a portfolio rebalance just before the end of the trading day. This requires the use of Cloud Scheduler, Google Cloud Function, and Google Kafka Topic. While tutorials for these services can be found online, this repository includes all the necessary files for setup. Separate configurations are provided for paper trading and live trading. 

This could be easily modified to trading your own signals / symphonys and also take your config files as environment variables. 

The application will output a daily log file and the GCP Cloud function will log to Google logging. 

## Installation

1. Install the necessary dependencies for this project by running the following command:

```bash
pip install -r requirements.txt

```

1. Install and requirements.txt
2. You need to put your information in config_paper.json. 

```
{
  "ALPACA_API_KEY": "", // Your Alpaca Trading Key
  "ALPACA_SECRET_KEY": "", // Your Secret Key 
  "ALPACA_BASE_URL": "https://paper-api.alpaca.markets", // Alpaca base url, paper or live
  "symphony_to_trade": "BEST20d", // the signal from scarplus signals project. See link above
  "symphony_url": "",  // The URL To your scarplus signals google drive file. 
  "cash_weight": "1.15", // The percentage of your account to trade, example 115% or 15% margin. 
  "timeout": "30",
  "service_account_file": "alpaca-trading-python-5c84eb50665d.json" // location to Google Drive service account Auth file. 
}
```

3. **Google Drive Authentication**: This project requires access to a Signals file stored on Google Drive. To facilitate this, I've implemented a Google Service Account and enabled the Google Drive API for authentication. This allows the system to access and download the Signals file from Google Drive. If authentication is not successful, a quota error may occur. Please note that the Signals setup may change in the future. To set up authentication, create a JSON file for the Google Service Account and ensure it's accessible by the code. You can use the `test_auth.py` script to verify your authentication setup.

This project can be run locally or deployed as a Google Cloud Function. For the latter, follow these steps:

1. **Create a Google Topic**: This will be used to trigger the Cloud Function.
2. **Set up a Cloud Scheduler**: Configure the scheduler cron job to trigger the Google Topic at a specific time, for example, at 3:50 PM EST. `50 15 * * 1-5` 
3. **Create a Google Cloud Function**: This function will run the code. The function should include the following files:
    - `gcp_function_main.py` (rename this to `main.py`)
    - `requirements.txt`
    - `config.json` (alternatively, you can modify the code to use environment variables)
    - `alpaca-trading-python-123aBcD456.json` The Google Service Account JSON file for Google Drive authentication
