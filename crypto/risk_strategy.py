import alpaca_trade_api as tradeapi
from credentials import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPHA_VANTAGE_API
import requests
import json
from trade_stats import download_trades
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.cryptocurrencies import CryptoCurrencies
import time
import os

alpha_vantage_ts = TimeSeries(key=ALPHA_VANTAGE_API, output_format='pandas')
alpha_vantage_crypto = CryptoCurrencies(key=ALPHA_VANTAGE_API, output_format='pandas')

api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

account = api.get_account()
equity = float(account.equity)

# Maximum amount of equity that can be held in cryptocurrencies
max_crypto_equity = equity * 0.2

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
    def __init__(self, api, risk_params):
        self.api = api
        self.risk_params = risk_params
        self.alpha_vantage_crypto = CryptoCurrencies(key=ALPHA_VANTAGE_API, output_format='pandas')
        self.manager = PortfolioManager(api)  # Initialize PortfolioManager here

        # Get account info
        account = self.api.get_account()

        # Initialize self.peak_portfolio_value with the current cash value
        self.peak_portfolio_value = float(account.cash)

    def rebalance_positions(self):

        account = self.api.get_account()

        equity = float(account.equity)
        margin = float(account.initial_margin)

        rebalance_threshold = (equity - margin) * 0.9685

        positions = self.api.list_positions()

        for position in positions:

            qty = float(position.qty)

            position_value = qty * float(position.current_price)

            if position_value > rebalance_threshold:

                shares_to_sell = int((position_value - rebalance_threshold) / float(position.current_price))

                if shares_to_sell > qty:
                    shares_to_sell = qty

                print(f"Selling {shares_to_sell} shares of {position.symbol}")

                api.submit_order(symbol=position.symbol, qty=shares_to_sell, side='sell', type='market',
                                 time_in_force='gtc')



    def validate_trade(self, symbol, qty, order_type):
        try:
            qty = float(qty)  # convert qty to float vs string
            print(f"Validating trade for {symbol}...")
            portfolio = self.api.list_positions()
            portfolio_symbols = [position.symbol for position in portfolio]
            print(f"Current portfolio symbols: {portfolio_symbols}")
            portfolio_value = sum([float(position.current_price) * float(position.qty) for position in portfolio])
            print(f"Current portfolio value: {portfolio_value}")

            current_price = self.get_current_price(symbol)  # Use current price instead of average entry price

            if current_price is None:
                raise Exception(f"Error: could not fetch current price for {symbol}")

            asset = self.manager.assets.get(symbol)

            if asset:
                pnl_24h = asset.profit_loss_24h()

                # if there has been a loss in the last 24 hours, disallow the trade
                if pnl_24h is not None and pnl_24h < 0:
                    return False

            if qty <= 0 or current_price <= 0:
                print(
                    f"Quantity or current price for {symbol} is zero or less. Skipping validation and order placement.")
                return False

            print(f"Current price for {symbol}: {current_price}")

            print(f"Quantity (qty): {qty}, Type: {type(qty)}")
            print(f"Current Price: {current_price}, Type: {type(current_price)}")
            proposed_trade_value = current_price * qty
            print(f"Proposed trade value for {symbol}: {proposed_trade_value}")

            # Calculate the current equity in cryptocurrencies
            crypto_equity = sum([float(position.current_price) * float(position.qty) for position in portfolio if
                                 position.symbol.startswith('C:')])
            print(f"Current crypto equity: {crypto_equity}")

            open_orders = self.api.list_orders(status='open')
            open_order_symbols = [order.symbol for order in open_orders]
            print(f"Open order symbols: {open_order_symbols}")

            # Ensure no more than 20% of total cash is used for any one trade
            account_cash = float(self.api.get_account().cash)
            print(f"Account cash: {account_cash}")
            if proposed_trade_value > account_cash * 0.2:
                print(f"Proposed trade for {qty} shares of {symbol} exceeds 20% of available cash.")
                return False

            if order_type == 'buy':
                # Check if a new buy order would violate the risk parameters
                if not self.check_risk_before_order(symbol, qty):  # qty is your new_shares
                    print(
                        f"A position or open order already exists for {symbol} that would violate the risk parameters "
                        f"with the new order.")
                    return False

                if qty > self.risk_params['max_position_size']:
                    print(f"Buy order for {qty} shares of {symbol} exceeds maximum position size.")
                    return False

                if (portfolio_value + proposed_trade_value) > self.risk_params['max_portfolio_size']:
                    print(f"Buy order for {qty} shares of {symbol} exceeds maximum portfolio size.")
                    return False

                if symbol.startswith('C:'):
                    # Ensure the proposed trade doesn't violate the max_crypto_equity limit
                    if (crypto_equity + proposed_trade_value) > self.risk_params['max_crypto_equity']:
                        print(f"Proposed trade for {qty} shares of {symbol} exceeds maximum crypto equity.")
                        return False

                # Ensure the proposed trade doesn't violate the max_risk_per_trade limit
                equity = self.get_equity()
                if equity == 0 or (proposed_trade_value / equity) <= self.risk_params['max_risk_per_trade']:
                    pass
                else:
                    print(f"Proposed trade for {qty} shares of {symbol} exceeds maximum risk per trade.")
                    return False



            elif order_type == 'sell':

                position_list = [position for position in portfolio if position.symbol == symbol]

                if len(position_list) == 0:

                    print(f"Sell order for {symbol} can't be placed because the position doesn't exist.")

                    return False

                else:

                    position = position_list[0]

                    if float(position.qty) < qty:  # Here is the change, convert string to float

                        print(

                            f"Sell order for {qty} shares of {symbol} can't be placed because it exceeds the existing "

                            f"position size.")

                        return False

            return True

        # error handling
        except Exception as e:
            print(f"An exception occurred while validating trade for {symbol}: {str(e)}")
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
            self.risk_params['max_position_size'] *= 1.10  # increase by 10%
            self.risk_params['max_portfolio_size'] *= 1.1065  # increase by 10%
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
        # Use the current price
        current_price = self.get_current_price(symbol)

        if current_price == 0 or current_price is None:
            print(f"Current price for {symbol} is zero or None. Returning quantity 0.")
            return 0

        # Calculate a preliminary quantity based on the max_position_size parameter
        preliminary_quantity = self.risk_params['max_position_size'] / current_price

        # Tiered system for quantity adjustment
        if current_price > 10000:  # High priced assets like BTC
            quantity = preliminary_quantity * 0.1  # buy less of high priced assets
        elif 1000 < current_price <= 10000:  # Mid-priced assets
            quantity = preliminary_quantity * 0.5  # buy moderate quantity
        else:  # Low-priced assets
            quantity = preliminary_quantity  # buy more of low priced assets

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

    def get_current_price(self, symbol, market="USD"):
        try:
            if '/' in symbol:  # For cryptocurrencies
                source, quote = symbol.split('/')
                if quote == market:
                    url = f"https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={source}&market={market}&interval=5min&outputsize=full&apikey={ALPHA_VANTAGE_API}"
                    data_field = 'Time Series Crypto (5min)'
                else:
                    return None  # We can't fetch price for this market
            else:  # For non-crypto
                url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={ALPHA_VANTAGE_API}"
                data_field = 'Time Series (Daily)'

            response = requests.get(url)
            data = response.json()

            last_update = list(data[data_field].keys())[0]
            current_price = float(data[data_field][last_update]['4. close'])

            print(f"Current price for {symbol} is {current_price} at {last_update}.")

            time.sleep(0.5)  # delay to respect API limit, adjust as necessary

            return current_price
        except Exception as e:
            print(f"Failed to get current price for {symbol}. Error: {str(e)}")
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

        average_entry_price = round(average_entry_price, 2)
        current_price = round(current_price, 2)
        profitability = round((current_price - average_entry_price) / average_entry_price * 100, 2)

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

