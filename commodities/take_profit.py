from alpaca_trade_api.rest import REST, TimeFrame
from credentials import ALPACA_API_KEY, ALPACA_SECRET_KEY

# Initialize API
api = REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

# Get a list of all of our positions
positions = api.list_positions()

# Loop through all of our positions
for position in positions:
    # If the symbol of the position ends with 'USD'
    if position.symbol.endswith('USD'):
        # Submit an order to sell all shares of the position
        api.submit_order(
            symbol=position.symbol,
            qty=position.qty,
            side='sell',
            type='market',
            time_in_force='gtc',
            order_class='simple'
        )
