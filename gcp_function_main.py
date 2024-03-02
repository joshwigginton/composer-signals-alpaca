import io
import os
import base64
import requests
import pandas as pd
from io import StringIO
from datetime import datetime
import logging
import sys
import json
import time
from time import sleep
from alpaca_trade_api.rest import REST
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload  
from urllib.parse import urlparse, parse_qs

# Utility functions setup_logging, load_configuration, validate_config, initialize_alpaca remain unchanged

def setup_logging():
    """
    Configure the logging system to write logs to a file named with the current date.
    
    Logs include the timestamp, log level, and log message, saved to a file that helps in auditing and debugging.
    """
    #log_file_name = f"log_live_{datetime.now().strftime('%Y%m%d')}.txt"
    #logging.basicConfig(filename=log_file_name, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

def load_configuration(config_file_path="config_paper.json"):
    """
    Load configuration settings from a JSON file.
    
    Parameters:
    - config_file_path: The file path for the configuration JSON file. Defaults to "config.json".
    
    Returns:
    - A dictionary containing configuration settings.
    
    Raises:
    - SystemExit: If the configuration file cannot be found.
    """
    if not os.path.exists(config_file_path):
        logging.error(f"Error: Configuration file {config_file_path} not found.")
        sys.exit(1)
    with open(config_file_path, 'r') as config_file:
        return json.load(config_file)

def validate_config(config):
    """
    Validate that all required configuration parameters are present in the configuration dictionary.
    
    Parameters:
    - config: The configuration dictionary to validate.
    
    Raises:
    - SystemExit: If any required configuration parameters are missing.
    """
    required_params = ["ALPACA_API_KEY", "ALPACA_SECRET_KEY", "ALPACA_BASE_URL", "symphony_to_trade", "symphony_url", "cash_weight", "timeout"]
    if not all(param in config for param in required_params):
        logging.error("Error: Configuration parameters missing.")
        sys.exit(1)

def initialize_alpaca(config):
    """
    Initialize and return an Alpaca API REST client using settings from the configuration.
    
    Parameters:
    - config: A dictionary containing configuration settings, including API credentials.
    
    Returns:
    - An instance of the Alpaca REST client.
    """
    return REST(config["ALPACA_API_KEY"], config["ALPACA_SECRET_KEY"], config["ALPACA_BASE_URL"])

def calculate_target_investment(api, cash_weight):
    """
    Calculate the target investment amount based on the account's equity and a specified cash weight.
    
    Parameters:
    - api: The Alpaca API client instance.
    - cash_weight: A multiplier representing the desired investment level relative to the account's equity.
    
    Returns:
    - The target investment amount as a float.
    """
    account_info = api.get_account()
    return float(account_info.equity) * cash_weight

def is_market_open(api):
    """
    Check if the stock market is currently open using the Alpaca API.
    
    Parameters:
    - api: The Alpaca API client instance.
    
    Returns:
    - True if the market is open, False otherwise.
    """
    clock = api.get_clock()
    return clock.is_open

def wait_for_order_fill(api, client_order_id, timeout):
    """
    Wait for an order to be filled within a specified timeout period.
    
    Parameters:
    - api: The Alpaca API client instance.
    - client_order_id: The client order ID of the order to monitor.
    - timeout: The timeout period in seconds.
    
    Returns:
    - True if the order was filled within the timeout period, False otherwise.
    """
    start_time = time.time()
    retries = 0
    max_retries = 10
    retry_interval = 3
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Correct method to use when querying by client_order_id
            order = api.get_order_by_client_order_id(client_order_id)
            if order.status == 'filled':
                return True
            elif order.status in ['cancelled', 'expired']:
                logging.error(f"Order {client_order_id} was not filled due to status: {order.status}")
                return False
        except Exception as e:
            logging.error(f"Error checking order status for {client_order_id}: {e}")
        sleep(3)  # Adjust sleep time as necessary
    logging.error(f"Order {client_order_id} not filled within timeout.")
    return False

def create_and_submit_order(api, symbol, qty, side):
    """
    Create and submit a market order for a specific symbol through the Alpaca API.
    
    Parameters:
    - api: The Alpaca API client instance.
    - symbol: The stock symbol for which to place the order.
    - qty: The quantity of shares to order.
    - side: The side of the order ('buy' or 'sell').
    
    Returns:
    - The client order ID if the order was successfully submitted, None otherwise.
    """
    
    try:
        order = api.submit_order(symbol=symbol, qty=qty, side=side, type="market", time_in_force="day")
        return order.client_order_id        
    except Exception as e:
        logging.error(f"Failed to submit order for {symbol}: {e}")
        return None

def get_current_positions(api):
    """
    Fetch the current open positions from Alpaca and return their details.
    
    Parameters:
    - api: The Alpaca API client instance.
    
    Returns:
    - A dictionary with symbols as keys and position details (quantity and market value) as values.
    """
    positions = api.list_positions()
    return {position.symbol: {'qty': float(position.qty), 'market_value': float(position.market_value)} for position in positions}


def get_authenticated_service(service_account_file):
    """
    Authenticate using the service account file and return the service.
    """
    credentials = service_account.Credentials.from_service_account_file(
        service_account_file,
        scopes=['https://www.googleapis.com/auth/drive.readonly'])
    service = build('drive', 'v3', credentials=credentials, cache_discovery=False)  # Added cache_discovery=False
    return service

def extract_file_id(url):
    query_string = urlparse(url).query
    parameters = parse_qs(query_string)
    return parameters.get('id', [None])[0]  # Extract 'id' parameter value


def download_file_from_drive(service, file_id):
    """
    Download a file from Google Drive using the Drive service and file ID.
    """
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return fh.getvalue().decode()

def get_target_allocations(symphony_url, service_account_file, symphony_to_trade):
    """
    Fetch and process target allocation percentages from a specified CSV URL for a given symphony.
    
    Parameters:
    - symphony_url: The URL to the CSV file containing allocation data.
    - service_account_file: Path to the Google service account credentials file.
    - symphony_to_trade: The name of the symphony for which to retrieve allocations.
    
    Returns:
    - A dictionary with symbols as keys and normalized allocation percentages as values.
    """
    service = get_authenticated_service(service_account_file)
    file_id = extract_file_id(symphony_url)  # Adjust based on actual URL format
    csv_data = download_file_from_drive(service, file_id)
    df = pd.read_csv(io.StringIO(csv_data))
    
    # Ensure the expected columns exist in the DataFrame
    expected_columns = ['Symphony', 'Ticker', 'Ticker Allocation Percent']
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing expected columns in CSV: {missing_columns}")
        return {}
    
    try:
        # Filter the DataFrame for the specific symphony and convert allocation percentages to float
        filtered_df = df[df['Symphony'] == symphony_to_trade].copy()
        filtered_df['Ticker Allocation Percent'] = pd.to_numeric(filtered_df['Ticker Allocation Percent'], errors='coerce')


        # Check for NaN values that may result from conversion and handle them as needed
        if filtered_df['Ticker Allocation Percent'].isnull().any():
            logging.error("NaN values found in 'Ticker Allocation Percent' after conversion. Check CSV data.")
            return {}

        target_allocations = filtered_df.set_index('Ticker')['Ticker Allocation Percent'].to_dict()

        # Normalize the allocation percentages
        total_allocation_percent = sum(target_allocations.values())
        normalized_allocations = {ticker: percent / total_allocation_percent for ticker, percent in target_allocations.items()}

    except KeyError as e:
        logging.error(f"Error accessing column in DataFrame: {e}")
        return {}

    return normalized_allocations

def calculate_orders(api, target_allocations, current_positions, target_investment_amount):
    orders = {}
    current_symbols = set(current_positions.keys())
    target_symbols = set(target_allocations.keys())
    
    # Symbols to sell because they're not in target allocations
    sell_off_symbols = current_symbols - target_symbols
    
    for symbol in sell_off_symbols:
        # Assuming we want to sell all shares of symbols not in target allocations
        orders[symbol] = {'side': 'sell', 'value': current_positions[symbol]['market_value'], 'qty': current_positions[symbol]['qty']}
    
    for symbol, target_allocation in target_allocations.items():
        desired_value = target_investment_amount * target_allocation
        current_value = current_positions.get(symbol, {}).get('market_value', 0)
        difference = desired_value - current_value

        if difference > 0:
            orders[symbol] = {'side': 'buy', 'value': difference}
        elif difference < 0:
            orders[symbol] = {'side': 'sell', 'value': -difference}

    for symbol, order_details in orders.items():
        if symbol in target_symbols:  # Only fetch price for symbols in target allocations
            price = api.get_latest_trade(symbol).price
            qty = order_details['value'] / price
            orders[symbol]['qty'] = qty if api.get_asset(symbol).fractionable else round(qty)

    return orders


def separate_and_prioritize_orders(orders):
    """
    Separate and prioritize orders into sell and buy orders.
    
    Parameters:
    - orders: A dictionary of all calculated orders.
    
    Returns:
    - Two dictionaries: one for sell orders and one for buy orders.
    """
    sell_orders = {symbol: details for symbol, details in orders.items() if details['side'] == 'sell'}
    buy_orders = {symbol: details for symbol, details in orders.items() if details['side'] == 'buy'}
    return sell_orders, buy_orders

def execute_prioritized_orders(api, sell_orders, buy_orders, timeout):
    """
    Execute sell and buy orders prioritizing sell orders to ensure sufficient cash for buy orders.
    
    Parameters:
    - api: The Alpaca API client instance.
    - sell_orders: A dictionary of sell orders to execute.
    - buy_orders: A dictionary of buy orders to execute.
    - timeout: The timeout in seconds to wait for each order to fill.
    """
    # Execute sell orders first to ensure there is enough cash for buy orders
    for orders, action in [(sell_orders, "sell"), (buy_orders, "buy")]:
        for symbol, details in orders.items():
            qty = details['qty']
            if qty > 0:  # Proceed if the quantity is meaningful
                # Log the attempt to execute an order
                logging.info(f"Attempting to {action} {qty} shares of {symbol}")
                
                client_order_id = create_and_submit_order(api, symbol, qty, action)
                if client_order_id and not wait_for_order_fill(api, client_order_id, timeout):
                    logging.error(f"Failed to execute {action} order for {symbol}")
                else:
                    # Log the successful execution of the order
                    logging.info(f"Successfully executed {action} order for {symbol}: {qty} shares.")
                    



def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    """
    The main function to execute the portfolio rebalancing strategy.
    
    It loads configuration, initializes the Alpaca API client, fetches current positions and target allocations,
    calculates necessary orders, separates and prioritizes sell and buy orders, and executes them accordingly.
    """
    
    setup_logging()
    config = load_configuration()
    validate_config(config)
    
    alpaca_api = initialize_alpaca(config)
    logging.info("Alpaca API connected successfully.")
    print("Alpaca API connected successfully.")
    
    # Extracting values from config to pass to get_target_allocations
    symphony_url = config['symphony_url']
    service_account_file = config['service_account_file']
    symphony_to_trade = config['symphony_to_trade']

    # Updated function call with the new parameters
    target_allocations = get_target_allocations(symphony_url, service_account_file, symphony_to_trade)
    logging.info("New Portfolio Allocations %s", target_allocations)
    print(f"New Portfolio Allocations {target_allocations}")
    
    current_positions = get_current_positions(alpaca_api)
    logging.info("Current Portfolio %s", current_positions)
    print(f"Current Portfolio {current_positions}")
    
    target_investment_amount = calculate_target_investment(alpaca_api, float(config['cash_weight']))
    logging.info("Target Investment Amount %s", target_investment_amount)
    print(f"Target Investment Amount {target_investment_amount}")
    
    
    if not is_market_open(alpaca_api):
        logging.info("Market is currently closed. No orders will be placed.")
        print("Market is currently closed. No orders will be placed.")
        return
    
    # Calculate necessary orders
    orders = calculate_orders(alpaca_api, target_allocations, current_positions, target_investment_amount)
    
    # Separate sell and buy orders, prioritizing sells
    sell_orders, buy_orders = separate_and_prioritize_orders(orders)
    
    # Execute sell orders first, then buy orders
    execute_prioritized_orders(alpaca_api, sell_orders, buy_orders, int(config['timeout']))

    logging.info("Rebalancing complete.")
    print("Rebalancing complete.")  # Print statement

    print('Pub/sub message:', pubsub_message)