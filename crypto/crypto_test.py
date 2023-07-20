import requests
import pandas as pd
from credentials import ALPHA_VANTAGE_API
import time
import concurrent.futures

# Define the pairs
btc_pairs = ['BCH/BTC', 'ETH/BTC', 'LINK/BTC', 'LTC/BTC', 'MATIC/BTC', 'SOL/BTC', 'UNI/BTC']
usdt_usd_pairs = ['AAVE/USDT', 'ALGO/USDT', 'AVAX/USDT', 'BCH/USDT', 'BTC/USDT', 'DAI/USDT', 'DOGE/USDT', 'ETH/USDT', 'LINK/USDT', 'LTC/USDT', 'NEAR/USDT', 'PAXG/USDT', 'SOL/USDT', 'SUSHI/USDT', 'TRX/USDT', 'UNI/USDT', 'YFI/USDT']

all_pairs = btc_pairs + usdt_usd_pairs

def fetch_intraday_stats(crypto):
    url = f"https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={crypto}&market=USD&interval=5min&outputsize=full&apikey={ALPHA_VANTAGE_API}"
    response = requests.get(url).json()

    print(f'Loading Intra Day Values: {crypto}...')
    try:
        latest_intraday_data = list(response.get('Time Series Crypto (5min)', {}).values())[0]
        return {
            'Crypto': crypto,
            'Open': latest_intraday_data['1. open'],
            'High': latest_intraday_data['2. high'],
            'Low': latest_intraday_data['3. low'],
            'Close': latest_intraday_data['4. close'],
            'Volume': latest_intraday_data['5. volume'],
        }
    except IndexError:
        print(f"No intraday data found for {crypto}")
        return None

# Prepare list to hold results
intraday_stats_data = []

# Use ThreadPoolExecutor for parallel processing
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Prepare a list of futures
    futures = [executor.submit(fetch_intraday_stats, pair.split('/')[0].replace('USDT', 'USD')) for pair in all_pairs]
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:  # If we got results, append them to our list
            intraday_stats_data.append(result)
        time.sleep(0.4)  # To respect API rate limit

# Construct the DataFrame from the data we collected
intraday_stats_df = pd.DataFrame(intraday_stats_data)

# Print the DataFrame
print(intraday_stats_df)

def fetch_daily_data(crypto):
    url = f"https://www.alphavantage.co/query?function=CRYPTO_DAILY&symbol={crypto}&market=USD&apikey={ALPHA_VANTAGE_API}"
    response = requests.get(url).json()
    data = response.get('Time Series (Digital Currency Daily)', {})

    if not data:
        print(f"No daily data found for {crypto}")
        return None

    df = pd.DataFrame.from_dict(data).T
    df = df.apply(pd.to_numeric)
    return df


# Get historical data for each crypto
dfs = []
for crypto in btc_pairs + usdt_usd_pairs:
    base_crypto = crypto.replace('USDT', 'USD').split('/')[0]
    df = fetch_daily_data(base_crypto)
    if df is not None:
        df['Crypto'] = base_crypto
        dfs.append(df)
    else:
        print(f"No daily data available for {base_crypto}. Skipping...")

# Concatenate dataframes
historical_data = pd.concat(dfs)

def backtest_strategy(strategy_func, df, **kwargs):
    # Apply the strategy to the historical data
    df['Signal'] = strategy_func(df, **kwargs)

    # Compute the strategy's returns
    df['Return'] = df['Close'].pct_change()
    df.loc[df['Signal'] == 'Sell', 'Return'] *= -1

    # Return the total return of the strategy
    return df['Return'].sum()

# Backtest each strategy on each crypto
results = {}
for crypto in btc_pairs + usdt_usd_pairs:
    base_crypto = crypto.replace('USDT', 'USD').split('/')[0]
    df = historical_data[historical_data['Crypto'] == base_crypto].copy()  # create a copy to avoid SettingWithCopyWarning
    results[base_crypto] = {
        'Mean Reversion': backtest_strategy(mean_reversion_strategy, df, std_dev_factor=1),
        'Momentum': backtest_strategy(momentum_strategy, df, period=14),
    }

# Print the results
for crypto, result in results.items():
    print(f'Crypto: {crypto}')
    for strategy, return_ in result.items():
        print(f'  {strategy}: {return_}')
