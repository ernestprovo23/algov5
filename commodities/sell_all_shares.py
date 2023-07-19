import alpaca_trade_api as tradeapi
import credentials
import os
import glob

# Import API keys from credentials
ALPACA_API_KEY = credentials.ALPACA_API_KEY
ALPACA_SECRET_KEY = credentials.ALPACA_SECRET_KEY

# Initialize the Alpaca API
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

def close_position(position):
    symbol = position.symbol
    qty = position.qty
    try:
        api.submit_order(
            symbol=symbol,
            qty=qty,
            side='sell',
            type='market',
            time_in_force='gtc'
        )
        print(f"Market sell order submitted for {symbol} (Quantity: {qty})")
    except Exception as e:
        print(f"Error submitting market sell order for {symbol}: {e}")

# Get all open positions
positions = api.list_positions()

# Close positions with negative unrealized profit and loss
for position in positions:
    if float(position.unrealized_pl) < 0:
        close_position(position)

# List all CSV files in the same directory as the script
script_dir = os.path.dirname(os.path.realpath(__file__))
csv_files = glob.glob(os.path.join(script_dir, '*.csv'))

# Now you can do something with the CSV files...
for csv_file in csv_files:
    print(f"Found CSV file: {csv_file}")
