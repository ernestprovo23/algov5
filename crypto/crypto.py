import requests
import pandas as pd
from credentials import ALPHA_VANTAGE_API
import time
import concurrent.futures

# Define the pairs
usdt_usd_pairs = ['AAVE/USD', 'ALGO/USD', 'AVAX/USD', 'BCH/USD', 'BTC/USD', 'ETH/USD',
                  'LINK/USD', 'LTC/USD', 'TRX/USD', 'UNI/USD', 'USDT/USD', 'SHIB/USD']

all_pairs = usdt_usd_pairs

# Create a session object
s = requests.Session()


# Wrap the API call in a try/except block to handle exceptions
def fetch_exchange_rate(base_currency, quote_currency):
    url = f"https://www.alphavantage.co/query?function=CURRENCY_EXCHANGE_RATE&from_currency={base_currency}&to_currency={quote_currency}&apikey={ALPHA_VANTAGE_API}"
    try:
        response = s.get(url).json()
        exchange_rate = response["Realtime Currency Exchange Rate"]["5. Exchange Rate"]
        return float(exchange_rate)
    except KeyError:
        print(f"No exchange rate found from {base_currency} to {quote_currency}")
        return None
    except Exception as e:
        print(f"Error while fetching exchange rate: {e}")
        return None
    finally:
        time.sleep(4.5)  # Sleep for 12 seconds to respect API rate limit


# Wrap the API call in a try/except block to handle exceptions
def fetch_intraday_stats(crypto):
    base_crypto, quote = crypto.split('/')
    exchange_rate = fetch_exchange_rate(base_crypto, quote)

    if exchange_rate is None:
        print(f"No exchange rate found for {base_crypto} to {quote}")
        return None

    url = f"https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={base_crypto}&market=USD&interval=5min&outputsize=full&apikey={ALPHA_VANTAGE_API}"
    try:
        response = s.get(url).json()
        intraday_data = response.get('Time Series Crypto (5min)', {})
        if not intraday_data:
            print(f"No intraday data found for {crypto}")
            return None

        # Only take the latest 288 data points (approximately the last 24 hours)
        latest_intraday_data = list(intraday_data.items())[:288]
        df = pd.DataFrame({
            'Date': [date for date, data in latest_intraday_data],
            'Crypto': [base_crypto] * len(latest_intraday_data),
            'Quote': [quote] * len(latest_intraday_data),
            'Open': [round(float(data['1. open']) * exchange_rate, 2) for date, data in latest_intraday_data],
            'High': [round(float(data['2. high']) * exchange_rate, 2) for date, data in latest_intraday_data],
            'Low': [round(float(data['3. low']) * exchange_rate, 2) for date, data in latest_intraday_data],
            'Close': [round(float(data['4. close']) * exchange_rate, 2) for date, data in latest_intraday_data],
            'Volume': [round(float(data['5. volume']) * exchange_rate, 2) for date, data in latest_intraday_data],
        })

        # Sort the dataframe by 'Date' in ascending order
        df = df.sort_values('Date')

        # Apply rolling calculations
        window_size = 20  # Adjust the window size as per your requirements
        df['Mean'] = df['Close'].rolling(window=window_size).mean()
        df['Std Dev'] = df['Close'].rolling(window=window_size).std()

        return df
    except Exception as e:
        print(f"Error while fetching intraday stats: {e}")
        return None


# Limit ThreadPoolExecutor workers to avoid exceeding the API rate limit
with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(fetch_intraday_stats, pair) for pair in all_pairs]
    dfs = []
    for f in concurrent.futures.as_completed(futures):
        result = f.result()
        if result is not None and not result.empty:
            dfs.append(result)


def mean_reversion_strategy(df, std_dev_factor):
    df['Mean'] = df['Close'].rolling(window=20).mean()
    df['Std Dev'] = df['Close'].rolling(window=20).std()
    df['Buy Signal'] = df['Close'] < (df['Mean'] - std_dev_factor * df['Std Dev'])
    df['Sell Signal'] = df['Close'] > (df['Mean'] + std_dev_factor * df['Std Dev'])
    df['Signal'] = 'Hold'
    df.loc[df['Buy Signal'], 'Signal'] = 'Buy'
    df.loc[df['Sell Signal'], 'Signal'] = 'Sell'
    return df

def momentum_strategy(df, period):
    df['Momentum'] = df['Close'].diff(period)
    df['Signal'] = 'Hold'
    df.loc[df['Momentum'] > 0, 'Signal'] = 'Buy'
    df.loc[df['Momentum'] < 0, 'Signal'] = 'Sell'
    return df


# Try to save to CSV inside a try/except block
try:
    results = {}
    historical_data = pd.DataFrame()

    for crypto in all_pairs:
        base_crypto, quote = crypto.split('/')
        df_all_pairs = pd.concat(dfs)
        df = df_all_pairs[
            (df_all_pairs['Crypto'] == base_crypto) & (df_all_pairs['Quote'] == quote)].copy()

        df = mean_reversion_strategy(df, std_dev_factor=1)
        df = momentum_strategy(df, period=14)

        # The mean reversion and momentum signals are now being calculated on the entire DataFrame,
        # so the signals for the last row (most recent time period) will be calculated correctly.
        df['Mean Reversion Signal'] = df['Signal']
        df['Momentum Signal'] = df['Signal']

        historical_data = pd.concat([historical_data, df])

    print(historical_data)
    historical_data.to_csv("crypto_results.csv")

except Exception as e:
    print(f"Error while writing to CSV: {e}")
