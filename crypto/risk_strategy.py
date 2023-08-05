import alpaca_trade_api as tradeapi
from credentials import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPHA_VANTAGE_API
import requests
import json
from trade_stats import download_trades
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies
import time
from datetime import datetime
import os
from port_op import optimize_portfolio
import logging

alpha_vantage_ts = TimeSeries(key=ALPHA_VANTAGE_API, output_format='pandas')
alpha_vantage_crypto = CryptoCurrencies(key=ALPHA_VANTAGE_API, output_format='pandas')

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

account = api.get_account()
equity = float(account.equity)

# Maximum amount of equity that can be held in cryptocurrencies
max_crypto_equity = equity * 0.45

# Maximum amount of equity that can be held in commodities
max_commodity_equity = equity * 0.45

# read the risk_params in and use the values
# these are updated by the algorithm below by pnl
with open('risk_params.json', 'r') as f:
    risk_params = json.load(f)

print(risk_params['max_position_size'])

# Define a class to represent a crypto asset
class CryptoAsset:
    def __init__(self, symbol, quantity, value_usd):
        self.symbol = symbol
        self.quantity = quantity
        self.value_usd = value_usd
        self.value_24h_ago = None  # to store the value 24 hours ago
        self.crypto_symbols = ['AAVE/USD', 'AVAX/USD', 'BCH/USD', 'BTC/USD', 'ETH/USD',
                  'LINK/USD', 'LTC/USD', 'TRX/USD', 'UNI/USD', 'SHIB/USD']

    def profit_loss_24h(self):
        if self.value_24h_ago is not None:
            return (self.value_usd - self.value_24h_ago) / self.value_24h_ago * 100
        else:
            return None


# Define a class to manage the portfolio
class PortfolioManager:
    def __init__(self, api):
        self.api = api
        self.assets = {}
        self.operations = 0  # track the number of operations

    def increment_operations(self):
        self.operations += 1

    def add_asset(self, symbol, quantity, value_usd):
        self.assets[symbol] = CryptoAsset(symbol, quantity, value_usd)

    def update_asset_value(self, symbol, value_usd):
        if symbol in self.assets:
            self.assets[symbol].value_usd = value_usd

    def portfolio_value(self):
        return sum(asset.value_usd for asset in self.assets.values())

    def portfolio_balance(self):
        return {symbol: (asset.value_usd / self.portfolio_value()) * 100 for symbol, asset in self.assets.items()}

    def sell_decision(self, symbol):
        balance = self.portfolio_balance()

        if balance[symbol] > 25 or balance[symbol] > 0.4 * sum(balance.values()):
            return True
        else:
            return False

    def scale_out(self, symbol):
        quantity_to_sell = int(self.assets[symbol].quantity * 0.1)  # Sell 10% of holdings
        return quantity_to_sell

    def update_asset_values_24h(self):
        for asset in self.assets.values():
            asset.value_24h_ago = asset.value_usd


class RiskManagement:
    crypto_symbols = ['AAVE/USD', 'ALGO/USD', 'AVAX/USD', 'BCH/USD', 'BTC/USD', 'ETH/USD',
                  'LINK/USD', 'LTC/USD', 'TRX/USD', 'UNI/USD', 'USDT/USD', 'SHIB/USD']
    def __init__(self, api, risk_params):
        self.api = api
        self.risk_params = risk_params
        self.alpha_vantage_crypto = CryptoCurrencies(key=ALPHA_VANTAGE_API, output_format='pandas')
        self.manager = PortfolioManager(api)  # Initialize PortfolioManager here


        # Get account info
        account = self.api.get_account()

        # Initialize self.peak_portfolio_value with the current cash value
        self.peak_portfolio_value = float(account.cash)

    def update_max_crypto_equity(self):
        # Get the current buying power of the account
        account = self.api.get_account()
        buying_power = float(account.buying_power)

        # Compute max_crypto_equity
        max_crypto_equity = buying_power * 0.45

        # Update the JSON file with the new value
        self.risk_params['max_crypto_equity'] = max_crypto_equity
        with open('risk_params.json', 'w') as f:
            json.dump(self.risk_params, f, indent=4)

        print(f"Updated max_crypto_equity: {max_crypto_equity}")
        return max_crypto_equity

    def get_commodity_equity(self):
        equity = float(self.api.get_account().equity)
        max_commodity_equity = equity * 0.50  # Adjust the percentage as per your strategy
        return max_commodity_equity


    def get_crypto_equity(self):
        equity = float(self.api.get_account().equity)
        max_crypto_equity = equity * 0.50  # Adjust the percentage as per your strategy
        return max_crypto_equity


    def max_commodity_equity(self):
        equity = float(self.api.get_account().equity)
        max_equity = equity * 0.50  # or whatever percentage
        return max_equity

    def optimize_portfolio(self):
        # Get historical data for each symbol
        historical_data = {}
        for symbol in self.crypto_symbols:
            data, _ = alpha_vantage_crypto.get_digital_currency_daily(symbol=symbol, market='USD')
            historical_data[symbol] = data['4b. close (USD)']

        # Calculate expected returns and covariance matrix
        returns_data = pd.DataFrame(historical_data).pct_change()
        expected_returns = returns_data.mean()
        covariance_matrix = returns_data.cov()

        # Total investment amount
        total_investment = max_crypto_equity

        # Run optimization in separate script
        quantities_to_purchase = optimize_portfolio(expected_returns, covariance_matrix, risk_aversion,
                                                    total_investment)

        return quantities_to_purchase

    def rebalance_positions(self):
        account = self.api.get_account()
        equity = float(account.equity)
        positions = self.api.list_positions()

        crypto_value = 0
        commodity_value = 0

        # Calculating the overall portfolio value and separate values for crypto and commodities
        for position in positions:
            symbol = position.symbol
            current_price = float(position.current_price)
            position_value = float(position.qty) * current_price

            if symbol in self.crypto_symbols:
                crypto_value += position_value
            else:  # Assuming other symbols are commodities
                commodity_value += position_value

        # Check if crypto or commodity positions exceed 50% of total equity
        if crypto_value > 0.5 * equity or commodity_value > 0.5 * equity:
            print("Rebalancing positions as crypto or commodity exceeds 50% of equity.")

            # Get today's date
            current_date = datetime.now().date()

            # Get all activities of type 'FILL' that occurred today
            activities = self.api.get_activities()
            fill_activities = [activity for activity in activities if
                               activity.activity_type == 'FILL' and activity.transaction_time.to_pydatetime().date() == current_date]

            # Loop through positions and analyze each one to rebalance
            for position in positions:
                symbol = position.symbol
                qty = float(position.qty)
                current_price = float(position.current_price)

                # Check if this position's symbol is a crypto or commodity and act accordingly
                if (symbol in self.crypto_symbols and crypto_value > 0.5 * equity) or (
                        symbol not in self.crypto_symbols and commodity_value > 0.5 * equity):

                    # Check if account equity is less than 25k
                    if equity < 25000:
                        # Check if this symbol was bought today
                        if any(activity.symbol == symbol and activity.side == 'buy' for activity in fill_activities):
                            print(f"Cannot sell {symbol} as it was bought today and equity is less than 25k.")
                            continue

                    shares_to_sell = int(qty * 0.35)  # Example: selling 35% of holdings
                    price_at_which_to_sell = current_price * 0.99  # Setting a limit price 1% below current price
                    price_at_which_to_sell = round(price_at_which_to_sell, 2)

                    print(f"Trying to sell {shares_to_sell} shares of {symbol}.")

                    if shares_to_sell > 0:
                        self.api.submit_order(
                            symbol=symbol,
                            qty=shares_to_sell,
                            side='sell',
                            type='limit',
                            limit_price=price_at_which_to_sell,
                            time_in_force='gtc'
                        )

    def get_position(self, symbol):
        """
        Get position details for a specific symbol
        """
        positions = self.api.list_positions()

        # Filter positions to find matches for the symbol
        symbol_positions = [p for p in positions if p.symbol == symbol]

        if not symbol_positions:
            print(f"No positions found for {symbol}")
            return None

        # Assuming there's only one position per symbol
        p = symbol_positions[0]
        pos = {
            "symbol": p.symbol,
            "qty": p.qty,
            "avg_entry_price": p.avg_entry_price
        }

        return pos

    def validate_trade(self, symbol, qty, order_type):

        try:
            qty = float(qty)

            print(f"Running validation logic against trade for {symbol}...")

            portfolio = self.api.list_positions()

            portfolio_value = sum([float(p.current_price) * float(p.qty) for p in portfolio])
            print(f"Current portfolio value (market value of all positions): {portfolio_value}")

            print('##################################################################')
            print('##################################################################')
            print('##################################################################')

            print('retreiving the price details from the get_current_price method....')

            # get the current price from the get_current_price method
            current_price = self.get_current_price(symbol)

            print(f"Current Alpaca API price for {symbol} is: {current_price}")

            # get the proposed trade value from the new trade being run using current price * qty
            proposed_trade_value = current_price * qty
            print(f"Total proposed shares to purchase: {proposed_trade_value}")

            # get the list of open orders
            open_orders = self.api.list_orders(status='open')
            open_symbols = [o.symbol for o in open_orders]

            # current account cash (for crypto spending)
            account_cash = float(self.api.get_account().cash)
            print(f"Current account cash to buy: {account_cash}")

            print('##################################################################')
            print('##################################################################')
            print('##################################################################')

            print('processing propsed_trade_value logic against current cash holdings...')

            # check if proposed new value is more than current account cash holdings - if so, reject it
            if proposed_trade_value > account_cash:
                print("Proposed trade exceeds cash available to purchase crypto.")
                return False

            # crypto specific - check if proposed new value is more than current account cash holdings - if so, reject it
            if symbol in self.crypto_symbols:
                crypto_equity = self.get_crypto_equity()
                print(crypto_equity)
                if float(crypto_equity + proposed_trade_value) > self.max_crypto_equity:
                    print(f'New Crypto Equity for this trade would be: {max_crypto_equity}')
                    print("Proposed trade exceeds max crypto equity limit.")
                    return False
            else:
                commodity_equity = self.get_commodity_equity()
                updated_max_crypto_equity = self.update_max_crypto_equity()
                print(f'Here is the commodity equity after purchase: {commodity_equity}')
                print(f'Here is the maximum commodity equity after purchase: {updated_max_crypto_equity}')
                print(f'Here is the proposed traade value after analysis: {proposed_trade_value}')

                if (commodity_equity + proposed_trade_value) > updated_max_crypto_equity:
                    print("Proposed trade exceeds max commodity equity limit.")
                    return False

                print(self.crypto_symbols)

                crypto_equity = self.get_crypto_equity()

                if (crypto_equity + proposed_trade_value) > self.risk_params['max_crypto_equity']:
                    print("Proposed trade exceeds max crypto equity")
                    return False

            if order_type == 'buy':

                if qty > self.risk_params['max_position_size']:
                    print("Buy exceeds max position size")
                    return False


            elif order_type == 'sell':

                position = self.get_position(symbol)

                position_qty = float(position['qty'])  # Convert to float

                if qty > position_qty:  # Now the comparison should work

                    print("Sell quantity exceeds position size")

                    return False

            return True

        except Exception as e:
            print(f"Error validating trade: {e}")
            return False


    def monitor_account_status(self):
        # Monitor and report on account status
        try:
            account = self.api.get_account()
            print(f"Equity: {account.equity}")
            print(f"Cash: {account.cash}")
            print(f"Buying Power: {account.buying_power}")
            return account
        except Exception as e:
            print(f"An exception occurred while monitoring account status: {str(e)}")
            return None

    def monitor_positions(self):
        # Monitor and report on open positions
        try:
            positions = self.api.list_positions()
            for position in positions:
                print(
                    f"Symbol: {position.symbol}, Quantity: {position.qty}, Avg Entry Price: {position.avg_entry_price}")
            return positions
        except Exception as e:
            print(f"An exception occurred while monitoring positions: {str(e)}")
            return None

    def report_profit_and_loss(self):
        # Calculate and report profit and loss
        try:
            account = self.api.get_account()
            portfolio = self.api.list_positions()
            cost_basis = sum([float(position.avg_entry_price) * float(position.qty) for position in portfolio])
            pnl = float(account.equity) - cost_basis
            print(f"Profit/Loss: {pnl}")
            return pnl
        except Exception as e:
            print(f"An exception occurred while reporting profit and loss: {str(e)}")
            return None

    def get_equity(self):
        return float(self.api.get_account().equity)

    def update_risk_parameters(self, current_equity):
        # Dynamically adjust risk parameters based on account performance
        pnl = self.report_profit_and_loss()
        account = self.api.get_account()
        current_equity = float(account.equity)


        self.risk_params[
            'max_portfolio_size'] = current_equity  # Update the max_portfolio_size with the current equity

        if pnl <= 105:
            print("PnL is negative, reducing risk parameters...")
            self.risk_params['max_position_size'] *= 0.96  # reduce by 4%
            self.risk_params['max_portfolio_size'] *= 0.96  # reduce by 4%
        elif pnl >= 110:
            print("PnL is positive, increasing risk parameters...")
            self.risk_params['max_position_size'] *= 1.0032  # increase by 10%
            self.risk_params['max_portfolio_size'] *= 1.0032  # increase by 10%
        else:
            print("PnL is neutral, no changes to risk parameters.")
        with open('risk_params.json', 'w') as f:
            json.dump(self.risk_params, f)
        print("Risk parameters updated.")
        return self.risk_params

    def calculate_drawdown(self):
        try:
            portfolio = self.api.list_positions()
            portfolio_value = sum([float(position.current_price) * float(position.qty) for position in portfolio])

            # Update peak portfolio value if current portfolio value is higher
            if portfolio_value > self.peak_portfolio_value:
                self.peak_portfolio_value = portfolio_value

            # Calculate drawdown if portfolio is not empty
            if portfolio_value > 0 and self.peak_portfolio_value > 0:
                drawdown = (self.peak_portfolio_value - portfolio_value) / self.peak_portfolio_value
            else:
                drawdown = 0

            return drawdown
        except Exception as e:
            print(f"An exception occurred while calculating drawdown: {str(e)}")
            return None

    def check_risk_before_order(self, symbol, new_shares):
        """
        Check the risk parameters before placing an order.

        The function will prevent an order if the new shares would result in a position size
        that violates the risk parameters.
        """
        # Get the current position
        try:
            current_position = self.api.get_position(symbol)
            current_shares = float(current_position.qty)
        except:
            current_shares = 0

        # Calculate the new quantity of shares after the purchase
        total_shares = current_shares + float(new_shares)

        # Check if the new quantity violates the risk parameters
        if total_shares > self.risk_params['max_position_size']:
            # If the new quantity violates the max position size, prevent the order
            return False
        else:
            # If the new quantity doesn't violate the risk parameters, adjust the quantity and place the order
            delta_shares = self.risk_params['max_position_size'] - current_shares

            if delta_shares > 0:
                # Get the average entry price
                avg_entry_price = self.get_avg_entry_price(symbol)

                if avg_entry_price is not None and avg_entry_price != 0:
                    # Calculate the adjusted quantity based on the average entry price
                    adjusted_quantity = int(delta_shares / avg_entry_price)

                    # Place the order with the adjusted quantity
                    self.api.submit_order(
                        symbol=symbol,
                        qty=adjusted_quantity,
                        side='buy',
                        type='limit',
                        time_in_force='gtc',
                        limit_price=avg_entry_price
                    )

            return True


    def check_momentum(self, symbol, momentum_signal):
        """
        Checks the momentum signal and decides whether to sell the entire position.
        """
        # Get position
        position_list = [position for position in self.api.list_positions() if position.symbol == symbol]

        if len(position_list) == 0:
            print(f"No position exists for {symbol}.")
            return

        position = position_list[0]

        # If momentum signal is 'Sell' and the percentage change is negative, sell the entire position
        if momentum_signal == "Sell" and float(position.unrealized_plpc) < 0:
            qty = position.qty
            if self.validate_trade(symbol, qty, "sell"):
                # Place a market sell order
                self.api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='sell',
                    type='market',
                    time_in_force='gtc'
                )
                print(f"Selling the entire position of {symbol} due to negative momentum.")

    def get_momentum_at_time(self, symbol, datetime):
        """
        Given a symbol and a datetime, this function calculates the momentum
        at that specific time.
        """
        # Check if symbol includes a '/'
        if '/' in symbol:
            # Split the symbol into crypto_symbol and market
            crypto_symbol, market = symbol.split('/')

            # If the market is 'USD', we assume it's a cryptocurrency
            if market == 'USD':
                data, meta_data = self.alpha_vantage_crypto.get_digital_currency_daily(symbol=crypto_symbol,
                                                                                       market=market)
        else:
            # If the symbol does not include a '/', we assume it's a commodity
            data, meta_data = self.alpha_vantage_ts.get_daily(symbol=symbol)

        # Calculate momentum
        print(data.info())
        close_data = data['4a. close (USD)']
        momentum = close_data.pct_change().rolling(window=5).mean()

        # Check if the date exists in the momentum DataFrame
        try:
            print(f"Accessing momentum for date: {datetime.date()}")
            return momentum.loc[datetime.date()].iloc[0]
        except KeyError:
            # If the date doesn't exist, find the nearest date's momentum
            try:
                print("Date not found. Attempting to find nearest date.")
                nearest_date = momentum.index[momentum.index.get_loc(datetime.date(), method='nearest')]
                print(f"Nearest date found: {nearest_date}")
                return momentum.loc[nearest_date].iloc[0]
            except Exception as e:
                print(f"Failed to find nearest date. Exception: {e}")
                print("Momentum index:")
                print(momentum.index)
                return None  # or some appropriate fallback value


    def calculate_quantity(self, symbol):
        """
        Calculates the quantity to purchase based on available equity and current price.
        """
        # Get account info
        account = self.api.get_account()
        available_cash = float(account.cash)
        equity = float(account.equity)

        # Determine how much of the equity can be invested
        max_crypto_equity = equity * 0.45  # You should define this within the method
        investable_amount = min(available_cash, max_crypto_equity)

        # Check if investable amount is less than 1
        if investable_amount < 1:
            print(f"Investable amount for {symbol} is less than 1. Returning quantity 0.")
            return 0

        # Use the current price
        current_price = self.get_current_price(symbol)

        if current_price == 0 or current_price is None:
            return 0

        # Calculate a preliminary quantity based on the available cash
        preliminary_quantity = investable_amount / current_price

        # Tiered system for quantity adjustment
        if current_price > 4001:  # High priced assets like BTC
            quantity = preliminary_quantity * 0.01  # buy less of high priced assets
        elif 3001 < current_price <= 4000:  # Mid-priced assets
            quantity = preliminary_quantity * 0.0234
        elif 1000 < current_price <= 3000:  # Mid-priced assets
            quantity = preliminary_quantity * 0.0334
        elif 100 < current_price <= 999:  # Mid-priced assets
            quantity = preliminary_quantity * 0.04534
        elif 1 < current_price <= 99:  # Mid-priced assets
            quantity = preliminary_quantity * 0.07434
        elif 0.10 < current_price <= .99:  # Mid-priced assets
            quantity = preliminary_quantity * 0.011434
        else:  # Low-priced assets
            quantity = preliminary_quantity  # buy more of low priced assets

        quantity = round(quantity, 5)

        print(f"Calculated quantity for {symbol}: {quantity}")
        return quantity


    def execute_profit_taking(self, symbol, pct_gain=0.05):
        """
        Executes a profit-taking strategy.
        If the profit for a specific crypto reaches a certain percentage, sell enough shares to realize the profit.
        """
        position_list = [position for position in self.api.list_positions() if position.symbol == symbol]

        if len(position_list) == 0:
            print(f"No position exists for {symbol}.")
            return

        position = position_list[0]

        # If the unrealized profit percentage is greater than the specified percentage, sell a portion of the position
        if float(position.unrealized_plpc) > pct_gain:
            qty = int(float(position.qty) * pct_gain)  # Selling enough shares to realize the 5% gain

            if self.validate_trade(symbol, qty, "sell"):
                self.api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='sell',
                    type='market',
                    time_in_force='gtc'
                )
                print(f"Selling {qty} shares of {symbol} to realize profit.")


    def execute_stop_loss(self, symbol, pct_loss=0.07):
        """
        Executes a stop-loss strategy.
        If the loss for a specific crypto reaches a certain percentage, sell the entire position.
        """
        position_list = [position for position in self.api.list_positions() if position.symbol == symbol]

        if len(position_list) == 0:
            print(f"No position exists for {symbol}.")
            return

        position = position_list[0]

        # If the unrealized loss percentage is greater than the specified percentage, sell the entire position
        unrealized_loss_pct = float(position.unrealized_plpc)
        if unrealized_loss_pct < -pct_loss:
            print(f"Unrealized loss for {symbol} exceeds {pct_loss}%: {unrealized_loss_pct}%")
            qty = position.qty

            if self.validate_trade(symbol, qty, "sell"):
                self.api.submit_order(
                    symbol=symbol,
                    qty=qty,
                    side='sell',
                    type='market',
                    time_in_force='gtc'
                )
                print(f"Selling the entire position of {symbol} due to stop loss.")

    def enforce_diversification(self, symbol, max_pct_portfolio=0.30):
        """
        Enforces diversification by ensuring that no crypto makes up more than a certain percentage of the portfolio.
        """
        portfolio = self.api.list_positions()
        portfolio_value = sum([float(position.current_price) * float(position.qty) for position in portfolio])
        position_list = [position for position in portfolio if position.symbol == symbol]

        if len(position_list) == 0:
            print(f"No position exists for {symbol}.")
            return

        position = position_list[0]
        position_value = float(position.current_price) * float(position.qty)

        # If the value of this position exceeds the maximum percentage of the portfolio, sell enough shares to get below the maximum
        if position_value / portfolio_value > max_pct_portfolio:
            excess_value = position_value - (portfolio_value * max_pct_portfolio)
            qty_to_sell = int(excess_value / float(position.current_price))

            if self.validate_trade(symbol, qty_to_sell, "sell"):
                self.api.submit_order(
                    symbol=symbol,
                    qty=qty_to_sell,
                    side='sell',
                    type='market',
                    time_in_force='gtc'
                )
                print(f"Selling {qty_to_sell} shares of {symbol} to maintain diversification.")

    def generate_momentum_signal(self, symbol):
        """
        Generate a momentum signal for the given symbol.

        Returns "Buy" if the symbol has increased in value by 5% or more since purchase,
        and "Sell" if it has decreased by 7% or more. Otherwise, returns "Hold".
        """
        # Get the purchase price for this stock
        # TODO: Replace this with your own logic
        purchase_price = self.get_purchase_price(symbol)

        # Get the current price for this stock
        current_price = get_avg_entry_price(self, symbol)

        # Calculate the percentage change since purchase
        pct_change = (current_price - purchase_price) / purchase_price * 100

        # Generate the momentum signal
        if pct_change >= 5:
            return "Buy"
        elif pct_change <= -7:
            return "Sell"
        else:
            return "Hold"


    def get_exchange_rate(base_currency, quote_currency):
        # Your Alpha Vantage API key
        api_key = ALPHA_VANTAGE_API

        # Prepare the URL
        url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base_currency}&to_currency={quote_currency}&apikey={api_key}"

        # Send GET request
        response = requests.get(url)

        # Parse JSON response
        data = json.loads(response.text)

        # Extract exchange rate
        exchange_rate = data["Realtime Currency Exchange Rate"]["5. Exchange Rate"]

        return float(exchange_rate)


    def get_purchase_price(self, symbol):
        """
        Retrieve the purchase price of the given symbol.
        """
        trades = download_trades()

        # Filter trades for the given symbol
        trades = [trade for trade in trades if trade[0] == symbol]

        if not trades:
            return None

        # Get the last trade for the symbol
        last_trade = trades[-1]

        # The price is the third element in the trade
        return float(last_trade[2])

    def get_avg_entry_price(self, symbol):
        try:
            position = self.api.get_position(symbol)
            avg_entry_price = float(position.avg_entry_price)
            print(f"For symbol {symbol}, average entry price is {avg_entry_price}.")
            return avg_entry_price
        except Exception as e:
            print(f"No position in {symbol} to calculate average entry price. Error: {str(e)}")
            return 0


    def get_current_price(self, symbol, ALPHA_VANTAGE_API=None):
        print(f'Current Price Lookup for : {symbol}')

        try:
            # Attempt to fetch price from Alpha Vantage
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API}"
            response = requests.get(url)
            data = response.json()

            last_update = list(data['Time Series (Daily)'].keys())[0]
            current_price_str = data['Time Series (Daily)'][last_update]['4. close']
            current_price = float(current_price_str)

            print(
                f"Current price for {symbol} is {current_price} (string value: {current_price_str}) at {last_update}.")
            return current_price
        except Exception as e:
            pass

        # If Alpha Vantage fails, attempt to fetch price from Alpaca
        if symbol.endswith("USD"):
            crypto, sort = symbol[:-3], "USD"
            alpaca_symbol = f"{crypto}/{sort}"

            try:
                print(f"Fetching price from Alpaca API for {symbol}.")
                url = f"https://data.alpaca.markets/v1beta3/crypto/us/latest/bars?symbols={alpaca_symbol}"
                headers = {"accept": "application/json"}
                response = requests.get(url, headers=headers)

                if response.status_code != 200:
                    print(f"Connection failure {symbol} from Alpaca. Status code: {response.status_code}")
                    return None

                crypto_data = response.json()
                current_price = float(crypto_data['bars'][alpaca_symbol]['c'])

                print(f"Current price for {symbol} is {current_price}.")
                return current_price
            except Exception as e:
                print(f"Failed to get current price from Alpaca or Alpha Vantage for {symbol}. Error: {str(e)}")
                return None


def get_alpha_vantage_data(base_currency, quote_currency):
    url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base_currency}&to_currency={quote_currency}&apikey={ALPHA_VANTAGE_API}"

    response = requests.get(url)
    data = response.json()

    if "Realtime Currency Exchange Rate" in data:
        # Get the exchange rate
        exchange_rate = data["Realtime Currency Exchange Rate"]["5. Exchange Rate"]
        return exchange_rate
    else:
        print("Error getting data from Alpha Vantage")
        return None

def send_teams_message(teams_url, message):
    headers = {
        "Content-type": "application/json",
    }
    response = requests.post(teams_url, headers=headers, data=json.dumps(message))
    return response.status_code


def get_profit_loss(positions):
    profit_loss = 0
    for position in positions:
        profit_loss += (position['current_price'] - position['avg_entry_price']) * float(position['quantity'])
    return profit_loss



facts = []

if __name__ == "__main__":
    risk_manager = RiskManagement(api, risk_params)

    risk_manager.monitor_account_status()
    risk_manager.monitor_positions()
    risk_manager.report_profit_and_loss()
    account = risk_manager.api.get_account()
    current_equity = float(account.equity)
    risk_manager.update_risk_parameters(current_equity=current_equity)

    risk_manager.rebalance_positions()

    account = risk_manager.api.get_account()
    portfolio = risk_manager.api.list_positions()

    commodity_equity = risk_manager.get_commodity_equity()
    crypto_equity = risk_manager.get_crypto_equity()
    max_commodity_equity = risk_manager.max_commodity_equity()

    print(f"Commodity Equity: {commodity_equity}")
    print(f"Crypto Equity: {crypto_equity}")

    # Call the new method to update max_crypto_equity
    updated_max_crypto_equity = risk_manager.update_max_crypto_equity()
    print(f"Updated max_crypto_equity value is: {updated_max_crypto_equity}")


    portfolio_summary = {}
    portfolio_summary['equity'] = float(account.equity)
    portfolio_summary['cash'] = float(account.cash)
    portfolio_summary['buying_power'] = float(account.buying_power)
    portfolio_summary['positions'] = []

    for position in portfolio:
        symbol = position.symbol
        average_entry_price = float(position.avg_entry_price)

        if symbol.endswith("USD"):
            base_currency = symbol[:-3]
            quote_currency = "USD"
            closing_price = get_alpha_vantage_data(base_currency, quote_currency)
            if closing_price is not None:
                current_price = float(closing_price)
        else:
            current_price = float(api.get_latest_bar(symbol).c)

        average_entry_price = float(position.avg_entry_price)

        # Check if average entry price is zero and skip to the next iteration if so
        if average_entry_price == 0:
            print(f"Warning: Average Entry Price for {symbol} is zero. Skipping this symbol.")
            continue

        current_price = round(current_price, 2)
        profitability = (current_price - float(position.avg_entry_price)) / float(position.avg_entry_price) * 100

        print(
            f"Symbol: {symbol}, Average Entry Price: {average_entry_price}, Current Price: {current_price}, "
            f"Profitability: {profitability}%")

        pos_details = {
            'symbol': symbol,
            'avg_entry_price': average_entry_price,
            'current_price': current_price,
            'profitability': profitability,
            'quantity': round(float(position.qty), 2)
        }

        portfolio_summary['positions'].append(pos_details)

        if profitability <= -0.07:
            # Sell the position
            pass

        elif profitability >= 0.05:
            # Place buy orders to take profit
            pass

    portfolio_summary['equity'] = round(float(account.equity), 2)
    portfolio_summary['cash'] = round(float(account.cash), 2)
    portfolio_summary['buying_power'] = round(float(account.buying_power), 2)
    portfolio_summary['profit_loss'] = round(get_profit_loss(portfolio_summary['positions']), 2)
    portfolio_summary['risk_parameters_updated'] = True if portfolio_summary['profit_loss'] > 0 else False

    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": "Trade Orders Summary",
        "sections": [{
            "activityTitle": "Trade Orders Placed",
            "activitySubtitle": "Summary of Buy and Sell Orders",
            "facts": []
        }]
    }

    facts = message["sections"][0]["facts"]

    for position in portfolio_summary['positions']:
        facts.append({
            'name': position['symbol'],
            'value': f"Average Entry Price: {position['avg_entry_price']}, Current Price: {position['current_price']}, Profitability: {position['profitability']}%, Quantity: {position['quantity']}"
        })

    facts.append({'name': 'Equity', 'value': round(portfolio_summary['equity'], 2)})
    facts.append({'name': 'Cash', 'value': round(portfolio_summary['cash'], 2)})
    facts.append({'name': 'Buying Power', 'value': round(portfolio_summary['buying_power'], 2)})
    facts.append({'name': 'Profit/Loss', 'value': round(portfolio_summary['profit_loss'], 2)})
    facts.append({'name': 'Risk Parameters Updated', 'value': portfolio_summary['risk_parameters_updated']})

    teams_url = 'https://data874.webhook.office.com/webhookb2/9cb96ee7-c2ce-44bc-b4fe-fe2f6f308909@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/7e8bd751e7b4457aba27a1fddc7e8d9f/6d2e1385-bdb7-4890-8bc5-f148052c9ef5'

    send_teams_message(teams_url, message)

