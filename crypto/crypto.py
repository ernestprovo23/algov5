import requests
import pandas as pd
from credentials import ALPHA_VANTAGE_API
import concurrent.futures

# Define the pairs
usdt_usd_pairs = ['AAVE/USD', 'AVAX/USD', 'BCH/USD', 'BTC/USD', 'ETH/USD',
                  'LINK/USD', 'LTC/USD', 'TRX/USD', 'UNI/USD', 'SHIB/USD']

all_pairs = usdt_usd_pairs

# Create a session object
s = requests.Session()


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


def build_dataframe(latest_intraday_data, exchange_rate, base_crypto, quote):
    dates, opens, highs, lows, closes, volumes = [], [], [], [], [], []
    for date, data in latest_intraday_data:
        dates.append(date)
        opens.append(round(float(data['1. open']) * exchange_rate, 2))
        highs.append(round(float(data['2. high']) * exchange_rate, 2))
        lows.append(round(float(data['3. low']) * exchange_rate, 2))
        closes.append(round(float(data['4. close']) * exchange_rate, 2))
        volumes.append(round(float(data['5. volume']) * exchange_rate, 2))

    df = pd.DataFrame({
        'Date': dates,
        'Crypto': [base_crypto] * len(latest_intraday_data),
        'Quote': [quote] * len(latest_intraday_data),
        'Open': opens,
        'High': highs,
        'Low': lows,
        'Close': closes,
        'Volume': volumes,
    })

    return df.sort_values('Date')


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

        latest_intraday_data = list(intraday_data.items())[:288]
        df = build_dataframe(latest_intraday_data, exchange_rate, base_crypto, quote)
        return apply_strategies(df)
    except Exception as e:
        print(f"Error while fetching intraday stats: {e}")
        return None


def apply_strategies(df):
    window_size = 20
    std_dev_factor = 1
    period = 14

    df['Mean'] = df['Close'].rolling(window=window_size).mean()
    df['Std Dev'] = df['Close'].rolling(window=window_size).std()
    df['Buy Signal'] = df['Close'] < (df['Mean'] - std_dev_factor * df['Std Dev'])
    df['Sell Signal'] = df['Close'] > (df['Mean'] + std_dev_factor * df['Std Dev'])
    df['Mean Reversion Signal'] = 'Hold'
    df.loc[df['Buy Signal'], 'Mean Reversion Signal'] = 'Buy'
    df.loc[df['Sell Signal'], 'Mean Reversion Signal'] = 'Sell'

    df['Momentum'] = df['Close'].diff(period)
    df['Momentum Signal'] = 'Hold'
    df.loc[df['Momentum'] > 0, 'Momentum Signal'] = 'Buy'
    df.loc[df['Momentum'] < 0, 'Momentum Signal'] = 'Sell'

    return df


with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(fetch_intraday_stats, pair) for pair in all_pairs]
    historical_data = pd.DataFrame()
    for f in concurrent.futures.as_completed(futures):
        result = f.result()
        if result is not None and not result.empty:
            historical_data = pd.concat([historical_data, result])

# Try to save to CSV
try:
    print(historical_data)
    historical_data.to_csv("crypto_results.csv")
except Exception as e:
    print(f"Error while writing to CSV: {e}")
