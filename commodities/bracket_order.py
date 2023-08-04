from risk_strategy import RiskManagement, risk_params
from alpha_vantage.timeseries import TimeSeries
import alpaca_trade_api as tradeapi
import random
import pandas as pd
from credentials import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPHA_VANTAGE_API
from concurrent.futures import ThreadPoolExecutor, as_completed
from alpha_vantage.techindicators import TechIndicators
import requests
import os
from trade_stats import record_trade
from azure.storage.blob import BlobServiceClient
from s3connector import azure_connection_string, connect_to_storage_account, download_blob
import logging
import json
import time
import math


def send_teams_message(teams_url, message):
    data = {'text': message}
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(teams_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        logging.info(f"Message sent to Teams successfully: {message}")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred when sending message to Teams: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred when sending message to Teams: {req_err}")
    except Exception as e:
        logging.error(f"Unexpected error occurred when sending message to Teams: {e}")

# Your Microsoft Teams channel webhook URL
teams_url = 'https://data874.webhook.office.com/webhookb2/9cb96ee7-c2ce-44bc-b4fe-fe2f6f308909@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/7e8bd751e7b4457aba27a1fddc7e8d9f/6d2e1385-bdb7-4890-8bc5-f148052c9ef5'

send_teams_message(teams_url, "Bracket Order Script Being Run.")

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)

# Get the path of the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# Your Microsoft Teams channel webhook URL
teams_url = 'https://data874.webhook.office.com/webhookb2/9cb96ee7-c2ce-44bc-b4fe-fe2f6f308909@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/7e8bd751e7b4457aba27a1fddc7e8d9f/6d2e1385-bdb7-4890-8bc5-f148052c9ef5'

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

rm = RiskManagement(api, risk_params)

# List to keep track of symbols that did not get purchased
symbols_not_purchased = []


def get_symbols_from_csv():
    # Setup Azure connection
    blob_service_client = connect_to_storage_account(azure_connection_string)

    # Define the container and blob names
    container_name = 'historic'
    blob_name = 'selected_pairs.csv'

    # Define the local path where the downloaded file will be stored
    csv_file = os.path.join(script_dir, "selected_pairs.csv")

    # Download the file from Azure Blob Storage
    download_blob(blob_service_client, container_name, blob_name, csv_file)

    # Load the csv_data into a DataFrame
    csv_df = pd.read_csv(csv_file)

    # Extract the symbols and return them as a list
    return csv_df["Symbol"].unique().tolist()


def get_holdings(api):
    current_positions = api.list_positions()
    holdings = {}
    for position in current_positions:
        holdings[position.symbol] = position.qty
    return holdings


def is_fractionable(api, symbol):
    # Get asset information
    asset = api.get_asset(symbol)

    # Return the fractionable status
    return asset.fractionable


def place_order(api, symbol, shares, recent_close):
    # Ensure recent_close is a number
    try:
        recent_close = float(recent_close)
    except ValueError:
        print(f"recent_close for {symbol} is not a number: {recent_close}")
        return

    # Check if asset is fractionable
    if not is_fractionable(api, symbol):
        # Use the floor quantity for non-fractionable assets
        shares = math.floor(shares) if shares >= 1 else math.ceil(shares)

    # Ensure shares is a number and greater than zero
    try:
        shares = float(shares)
        if shares <= 0:
            raise ValueError
    except ValueError:
        print(f"Shares for {symbol} is not a number or not positive: {shares}")
        return

    # Calculate the stop and limit prices
    take_profit_price = round(recent_close * 1.0443, 2)
    stop_loss_price = round(recent_close * 0.9821, 2)
    client_order_id = f"gcos_{random.randrange(100000000)}"

    print("Submitting order...")

    try:
        # Place initial order
        initial_order = api.submit_order(
            symbol=symbol,
            qty=shares,
            side='buy',
            type='market',
            time_in_force='day',
            client_order_id=client_order_id
        )

        # Place take-profit order
        take_profit_order = api.submit_order(
            symbol=symbol,
            qty=shares,
            side='sell',
            type='limit',
            limit_price=take_profit_price,
            time_in_force='day'
        )

        # Place stop-loss order
        stop_loss_order = api.submit_order(
            symbol=symbol,
            qty=shares,
            side='sell',
            type='stop',
            stop_price=stop_loss_price,
            time_in_force='day'
        )

        print(f"{symbol}: order placed successfully!")

        # Record the trade
        record_trade(symbol, shares, recent_close)

        # Create a message to send to Teams channel
        message = f"Order placed successfully! Symbol: {symbol}, Shares: {shares}, Price: {recent_close}"
        # Send message to Teams
        send_teams_message(message)

        return initial_order, take_profit_order, stop_loss_order

    except Exception as e:
        print(f"Order for {symbol} could not be placed: {str(e)}")
        return False


cash_balance = api.get_account().cash
portfolio_balance = float(api.get_account().portfolio_value)
maximum_risk_per_trade = rm.risk_params['max_risk_per_trade']

# Alpha Vantage connection
ts = TimeSeries(key=ALPHA_VANTAGE_API)
ti = TechIndicators(key=ALPHA_VANTAGE_API)

# Get a list of all symbols from the CSV file in Azure storage
symbols = get_symbols_from_csv()

def get_open_orders(api):
    open_orders = api.list_orders(status='open')
    open_orders_symbols = [order.symbol for order in open_orders]
    return open_orders_symbols


def handle_symbol(symbol):
    try:
        # Get the current holdings and open orders before checking the conditions
        current_holdings = get_holdings(api)
        open_orders_symbols = get_open_orders(api)

        # Prepare API URLs
        daily_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API}'
        rsi_url = f'https://www.alphavantage.co/query?function=RSI&symbol={symbol}&interval=daily&time_period=14&series_type=close&apikey={ALPHA_VANTAGE_API}'
        macd_url = f'https://www.alphavantage.co/query?function=MACD&symbol={symbol}&interval=daily&series_type=close&apikey={ALPHA_VANTAGE_API}'
        sma_url = f'https://www.alphavantage.co/query?function=SMA&symbol={symbol}&interval=daily&time_period=30&series_type=close&apikey={ALPHA_VANTAGE_API}'

        # Make API requests
        daily_data = requests.get(daily_url).json()
        rsi_data = requests.get(rsi_url).json()
        macd_data = requests.get(macd_url).json()
        sma_data = requests.get(sma_url).json()

        # Extract the first data point for each technical indicator
        daily_point = list(daily_data['Time Series (Daily)'].values())[0]
        rsi_point = list(rsi_data['Technical Analysis: RSI'].values())[0]
        macd_point = list(macd_data['Technical Analysis: MACD'].values())[0]
        sma_point = list(sma_data['Technical Analysis: SMA'].values())[0]

        recent_close = float(daily_point['4. close'])

        recent_rsi = float(rsi_point['RSI'])
        recent_macd = float(macd_point['MACD'])
        recent_signal = float(macd_point['MACD_Signal'])
        recent_sma = float(sma_point['SMA'])

        if recent_rsi <= 70:
            print(f"{symbol}: RSI condition met. Current: {recent_rsi}")
        else:
            print(f"{symbol}: RSI condition not met. Current: {recent_rsi}")

        if recent_macd >= recent_signal:
            print(f"{symbol}: MACD condition met. Current: {recent_macd}")
        else:
            print(f"{symbol}: MACD condition not met. Current: {recent_macd}")

        if recent_close <= recent_sma:
            print(f"{symbol}: SMA condition met. Current: {recent_close} / {recent_sma}")
        else:
            print(f"{symbol}: SMA condition not met. Current: {recent_close} / {recent_sma}. ")


        if recent_rsi <= 70 and recent_macd >= recent_signal and recent_close >= recent_sma:
            # Calculate shares once here
            shares = int(risk_params['max_portfolio_size'] * risk_params['max_risk_per_trade']) / recent_close / 3

            # Print shares type
            print(f"Shares type for {symbol}: {type(shares)}")

            # Check if we already have a position or an open order for this symbol
            print(current_holdings)
            print(open_orders_symbols)
            if symbol in current_holdings or symbol in open_orders_symbols:
                print(f"Already hold a position or have an open order in {symbol}, skipping order...")
            else:
                shares = int(risk_params['max_portfolio_size'] * risk_params['max_risk_per_trade']) / recent_close / 3

                # Check if shares exceed maximum position size
                if shares > risk_params['max_position_size']:
                    print(
                        f"Requested {shares} exceeds maximum position size, adjusting to {risk_params['max_position_size']}")
                    shares = risk_params['max_position_size']
                    print(f"Shares type: {type(shares)}")

                print(f"{symbol}: All conditions met. Place order for: {shares} shares.")

                place_order(api, symbol, shares, recent_close)
        else:
            print(f"{symbol}: Not all conditions met. No order placed.")
            symbols_not_purchased.append(symbol)

    except ValueError:
        print(f"Unable to fetch data for {symbol}. Skipping...")
    except Exception as e:
        print(f"An unexpected error occurred for {symbol}: {str(e)}")


# Use ThreadPoolExecutor to handle the symbols in parallel
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(handle_symbol, symbol) for symbol in symbols]

for future in as_completed(futures):
    try:
        data = future.result()
    except Exception as exc:
        print(f"An exception occurred in a thread: {str(exc)}")


if symbols_not_purchased:
    unpurchased_tickers_message = f"Tickers that did not get purchased: {', '.join(symbols_not_purchased)}"
    send_teams_message(teams_url, unpurchased_tickers_message)
else:
    send_teams_message(teams_url, "All tickers were purchased successfully.")

