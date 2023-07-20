import pandas as pd
import alpaca_trade_api as tradeapi
from credentials import ALPACA_API_KEY, ALPACA_SECRET_KEY
from risk_strategy import RiskManagement, risk_params, send_teams_message
from trade_stats import record_trade
import os
import logging

# Set up logging
logging.basicConfig(filename='master_script.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def get_files_in_current_directory():
    return [f for f in os.listdir() if os.path.isfile(f)]

# Setup Alpaca API connection
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

teams_url = 'https://data874.webhook.office.com/webhookb2/9cb96ee7-c2ce-44bc-b4fe-fe2f6f308909@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/7e8bd751e7b4457aba27a1fddc7e8d9f/6d2e1385-bdb7-4890-8bc5-f148052c9ef5'

# Initialize RiskManagement
risk_management = RiskManagement(api, risk_params)

# Load all CSV data
for filename in get_files_in_current_directory():
    if filename.endswith(".csv"):  # Check if the file is a CSV
        data = pd.read_csv(filename)


# Load the data
data = pd.read_csv("crypto_results.csv")

# Convert the Date column to datetime
data['Date'] = pd.to_datetime(data['Date'])

# Sort by date and drop duplicate symbols, keeping only the latest record for each symbol
data = data.sort_values('Date').drop_duplicates('Crypto', keep='last')

# Create an empty list to store the symbol details
symbol_details = []

# Iterate over the rows of the DataFrame
for _, row in data.iterrows():
    crypto = row["Crypto"]
    date = row["Date"]
    quote = row["Quote"]
    symbol = f"{crypto}{quote}"  # Combine Crypto and Quote to get the symbol

    # Skip the loop iteration if the symbol is 'nannan'
    if symbol == 'nannan':
        logging.info("Ignoring invalid symbol 'nannan'...")
        continue

    signal = row["Signal"]
    momentum_signal = row["Momentum Signal"]
    quantity_to_sell = 0  # default quantity to sell

    logging.info(f"Processing symbol: {symbol}, Signal: {signal}, Momentum Signal: {momentum_signal}, Date Chose: {date}")

    # Sell the entire position if momentum is negative
    risk_management.check_momentum(symbol, momentum_signal)

    if pd.isnull(signal):
        continue

    if signal == "Buy":
        logging.info(f"Buy signal detected for {symbol}")

        # Get the average entry price for the symbol
        avg_entry_price = risk_management.get_avg_entry_price(symbol)

        if avg_entry_price is not None:
            logging.info(f"Average entry price for {symbol}: {avg_entry_price}")

            # Calculate the quantity to buy based on average entry price and available equity
            quantity = risk_management.calculate_quantity(symbol)

            logging.info(f"Calculated quantity to buy: {quantity}")

            # Validate the trade
            if risk_management.validate_trade(symbol, quantity, "buy"):
                logging.info(f"Buy order validated for {symbol}")

                if quantity > 0:
                    # Place a market buy order
                    api.submit_order(
                        symbol=symbol,
                        qty=quantity,
                        side='buy',
                        type='market',
                        time_in_force='gtc'
                    )
                    logging.info(f'Buy order placed for {quantity} units of {symbol}')
                else:
                    logging.info(f"Order quantity for symbol {symbol} is not greater than 0. Can't place the order.")

                # Send a message to the team
                message = {
                    "text": f"Placed a BUY order for {quantity} units of {symbol}"
                }
                send_teams_message(teams_url, {"text": f"Placed a BUY order for {quantity} units of {symbol}"})

                # Record the trade
                record_trade(crypto, 'buy', quantity, date)
            else:
                logging.info(f"Buy order not validated for {symbol}")
        else:
            logging.info(f"No average entry price found for {symbol}. Not placing a buy order.")
    elif signal == "Sell":
        try:
            # Get current position
            position = api.get_position(symbol)

            # Get the quantity currently held for selling
            if isinstance(position.qty, str):
                quantity = float(position.qty)
            else:
                quantity = position.qty

            if quantity > 0:
                # Calculate the trend by comparing the current price with a moving average
                current_price = api.get_last_trade(symbol).price
                moving_avg = api.get_barset(symbol, 'day', limit=10).df[symbol]['close'].mean()

                if current_price > moving_avg:
                    # If the price is above the moving average, sell less than 50%
                    quantity_to_sell = max(1, int(float(quantity) * 0.3))
                else:
                    # If the price is below the moving average, sell more than 50%
                    quantity_to_sell = max(1, int(float(quantity) * 0.7))
            else:
                logging.info(f"Order quantity for symbol {symbol} is not greater than 0. Can't place the order.")
                quantity_to_sell = 0
        except Exception as e:
            logging.info(f"No position in {crypto} to sell")
            symbol_details.append(symbol)  # Add the symbol to the list

    # Validate the trade
    if quantity_to_sell > 0 and risk_management.validate_trade(symbol, quantity_to_sell, "sell"):
        # Place a market sell order
        api.submit_order(
            symbol=symbol,
            qty=quantity_to_sell,
            side='sell',
            type='market',
            time_in_force='gtc'
        )
        logging.info(f'Sell order placed for {quantity_to_sell} units of {crypto}')
        # Send a message to the team
        message = {
            "text": f"Placed a SELL order for {quantity_to_sell} units of {crypto}"
        }
        send_teams_message(teams_url, {"text": f"Placed a SALE order for {quantity_to_sell} units of {symbol}"})

        # Record the trade
        record_trade(crypto, 'sale', quantity_to_sell, date)

    else:
        symbol_details.append(symbol)

# After the iteration
if symbol_details:  # If there are symbols with no trades
    symbols_string = ", ".join(symbol_details)
    message = {
        "text": f"No trades for the following symbols:\n\n{symbols_string}"
    }
    send_teams_message(teams_url, message)

else:  # If there are no symbols with no trades
    message = {
        "text": "All symbols have trades."
    }
    send_teams_message(teams_url, message)
