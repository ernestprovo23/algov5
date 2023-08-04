import os
import logging
import pandas as pd
import alpaca_trade_api as tradeapi
from credentials import ALPACA_API_KEY, ALPACA_SECRET_KEY
from risk_strategy import RiskManagement, risk_params, send_teams_message, CryptoAsset, PortfolioManager
from trade_stats import record_trade

# Set up logging
logging.basicConfig(filename='master_script.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


def get_files_in_current_directory():
    return [f for f in os.listdir() if os.path.isfile(f)]


def get_symbol(row):
    crypto = row["Crypto"]
    quote = row["Quote"]
    symbol = f"{crypto}{quote}"  # Combine Crypto and Quote to get the symbol
    return symbol if symbol != 'nannan' else None

def calculate_SMA(data, window=5):
    return data['Close'].rolling(window=window).mean()


def calculate_EMA(data, window=5):
    return data['Close'].ewm(span=window, adjust=False).mean()


def calculate_RSI(data, window=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)

    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_MACD(data, short_window=12, long_window=26, signal_window=9):
    ema_short = calculate_EMA(data, window=short_window)
    ema_long = calculate_EMA(data, window=long_window)
    macd_line = ema_short - ema_long
    signal_line = macd_line.ewm(span=signal_window, adjust=False).mean()
    return macd_line, signal_line


def analyze_trend(short_term_SMA, long_term_SMA):
    return short_term_SMA > long_term_SMA


def process_buy(api, data, row, risk_management, teams_url, manager):
    symbol = get_symbol(row)

    if symbol is None:
        return

    # Get the last row for the symbol
    row = data[data['Symbol'] == symbol].iloc[-1]

    # Calculate Indicators
    short_term_SMA = calculate_SMA(data, window=5)
    long_term_SMA = calculate_SMA(data, window=10)
    rsi = calculate_RSI(data)
    macd_line, signal_line = calculate_MACD(data)

    # Analyzing trend (upward or downward)
    upward_trend = analyze_trend(short_term_SMA.iloc[-1], long_term_SMA.iloc[-1])

    signal = row["Momentum Signal"]
    date = row["Date"]
    momentum_signal = row["Momentum Signal"]

    logging.info(
        f"Processing symbol: {symbol}, Signal: {signal}, Date Chose: {date}")
    print(f"Processing symbol: {symbol}, Signal: {signal}, Date Chose: {date}")

    # Sell the entire position if momentum is negative
    risk_management.check_momentum(symbol, momentum_signal)

    if pd.isnull(signal) or signal != "Buy":
        return

    # Additional Buy Logic based on calculated indicators
    if upward_trend and rsi.iloc[-1] < 55:  # Add any other conditions if needed
        logging.info(f"Buy conditions met for {symbol}")

    # Get the average entry price for the symbol
    avg_entry_price = risk_management.get_avg_entry_price(symbol)
    print(f'Your average entry price was: {avg_entry_price}')
    entry_price = risk_management.get_current_price(symbol)

    if avg_entry_price is not None:
        logging.info(f"Average entry price for {symbol}: {avg_entry_price}")

        # Calculate the quantity to buy based on average entry price and available equity
        quantity = risk_management.calculate_quantity(symbol)

        logging.info(f"Calculated quantity to buy: {quantity}")

        # Validate the trade
        if risk_management.validate_trade(symbol, quantity, "buy"):
            logging.info(f"Buy order validated for {symbol}")
            print(f"Buy order validated for {symbol}")

            if quantity > 0:
                try:
                    # Place a market buy order
                    api.submit_order(
                        symbol=symbol,
                        qty=quantity,
                        side='buy',
                        type='market',
                        time_in_force='gtc'
                    )
                    manager.add_asset(symbol, quantity, avg_entry_price * quantity)
                    manager.increment_operations()  # increment the number of operations
                except Exception as e:
                    logging.error(f'Error placing buy order for {quantity} units of {symbol}: {str(e)}')
                    print(f'Error placing buy order for {quantity} units of {symbol}: {str(e)}')
                    return

                logging.info(f'Buy order placed for {quantity} units of {symbol}')
                # Send a message to the team
                send_teams_message(teams_url, {"text": f"Placed a BUY order for {quantity} units of {symbol}"})

                # Record the trade
                record_trade(symbol, 'buy', quantity, date)
            else:
                logging.info(f"Order quantity for symbol {symbol} is not greater than 0. Can't place the order.")
        else:
            logging.info(f"Buy order not validated for {symbol}")
    else:
        logging.info(f"No average entry price found for {symbol}. Not placing a buy order.")


def process_sell(api, data, row, risk_management, teams_url, manager):
    symbol = get_symbol(row)
    if symbol is None:
        return

    # Get the last row for the symbol
    row = data[data['Symbol'] == symbol].iloc[-1]

    signal = row["Momentum Signal"]
    date = row["Date"]

    if pd.isnull(signal) or signal != "Sell":
        return

    try:
        # Get current position
        position = api.list_positions(symbol)

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

        if quantity_to_sell > 0 and risk_management.validate_trade(symbol, quantity_to_sell, "sell"):
            try:
                # Place a market sell order
                api.submit_order(
                    symbol=symbol,
                    qty=quantity_to_sell,
                    side='sell',
                    type='market',
                    time_in_force='gtc'
                )
                manager.update_asset_value(symbol, (
                            quantity - quantity_to_sell) * current_price)  # Update asset value after selling
                manager.increment_operations()  # increment the number of operations
            except Exception as e:
                logging.error(f'Error placing sell order for {quantity_to_sell} units of {symbol}: {str(e)}')
                return

            logging.info(f'Sell order placed for {quantity_to_sell} units of {symbol}')

            # Send a message to the team
            message = {
                "text": f"Placed a SELL order for {quantity_to_sell} units of {symbol}"
            }
            send_teams_message(teams_url, {"text": f"Placed a SELL order for {quantity_to_sell} units of {symbol}"})

            # Record the trade
            record_trade(symbol, 'sell', quantity_to_sell, date)

        else:
            logging.info(f"Sell order not validated for {symbol}")

    except Exception as e:
        logging.error(f'Error getting position or placing sell order for {symbol}: {str(e)}')


def process_signals():
    # Setup Alpaca API connection
    api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

    # Instantiate PortfolioManager
    manager = PortfolioManager(api)

    teams_url = 'https://data874.webhook.office.com/webhookb2/9cb96ee7-c2ce-44bc-b4fe-fe2f6f308909@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/7e8bd751e7b4457aba27a1fddc7e8d9f/6d2e1385-bdb7-4890-8bc5-f148052c9ef5'

    # Initialize RiskManagement
    risk_management = RiskManagement(api, risk_params)

    # Update asset values from 24 hours ago
    manager.update_asset_values_24h()

    # get the current directory
    current_directory = os.getcwd()

    # create a relative path to the csv file
    file_path = os.path.join(current_directory, 'crypto_results.csv')

    # load the csv file into a pandas DataFrame
    data = pd.read_csv(file_path)

    data['Symbol'] = data.apply(get_symbol, axis=1)
    data['Date'] = pd.to_datetime(data['Date'])
    data.sort_values(by='Date', ascending=True, inplace=True)

    grouped = data.sort_values('Date').groupby('Symbol').tail(1)

    # Create an empty list to store the symbol details
    symbol_details = []

    # Iterate over the rows of the DataFrame and process the signals
    for index, row in grouped.iterrows():
        process_buy(api, data, row, risk_management, teams_url, manager)
        process_sell(api, data, row, risk_management, teams_url, manager)

    if manager.operations == 0:
        # Send a message to the team
        message = {
            "text": "No 'Buy' or 'Sell' operations were made."
        }
        send_teams_message(teams_url, message)


if __name__ == "__main__":
    print('script started')

    process_signals()

    print('script ended')
