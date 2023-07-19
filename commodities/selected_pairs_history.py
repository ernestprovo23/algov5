import pandas as pd
from azure.storage.blob import BlobServiceClient
from s3connector import azure_connection_string
from io import StringIO
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
import credentials
import time
import numpy as np
import os

# Additional Filters and Criteria

# Profit Margin:
# - Very low or Negative: A profit margin of 1% or lower is considered very low,
#   and if it's negative (e.g., -5%), the company is losing money.
#   Example: Amazon, in its early years, often reported negative profit margins.
# - High: A profit margin above 15% is typically considered high.
#   Example: Companies like Microsoft often have profit margins exceeding 30%.

# Price-Earnings Ratio (P/E):
# - Very low or Negative: A P/E ratio below 5 is considered low, suggesting
#   the market has low expectations for the company's future. Companies with negative earnings have a negative P/E ratio.
#   Example: In 2020, many airlines had negative P/E ratios due to substantial losses caused by the COVID-19 pandemic.
# - High: A P/E ratio above 20 is typically considered high, indicating that
#   the market expects high earnings growth.
#   Example: Amazon has had a high P/E ratio for many years, often exceeding 100.

# Return on Equity (ROE):
# - Very low or Negative: An ROE below 5% is considered low, suggesting the company
#   isn't generating much profit from its equity. Negative ROE (e.g., -10%) means the company is losing money.
#   Example: In 2008 during the financial crisis, many banks reported negative ROE.
# - High: An ROE above 20% is generally considered high.
#   Example: Companies like Apple have consistently reported ROE above 30%.

# EV to EBITDA:
# - Very low or Negative: An EV/EBITDA below 5 is generally considered low, suggesting
#   the company might be undervalued, assuming it's a profitable business. Negative values can occur if EBITDA is negative,
#   indicating operating losses. Example: In 2008, during the financial crisis, some banks had low EV/EBITDA ratios.
# - High: An EV/EBITDA above 15 is usually considered high, suggesting the company may be overvalued.
#   High-growth tech companies often have high EV/EBITDA ratios. Example: Zoom Video Communications had an EV/EBITDA ratio over 200 in 2020.

# Quarterly Earnings Growth YoY:
# - Very low or Negative: Negative quarterly earnings growth means the company's earnings have shrunk compared to the same quarter in the previous year.
#   Example: During the COVID-19 pandemic in 2020, many companies in the travel and hospitality industry faced negative quarterly earnings growth.
# - High: A high number (e.g., 50% or higher) would indicate a significant increase in earnings compared to the same quarter in the previous year.
#   Example: Many tech companies like Apple and Amazon reported high quarterly earnings growth in 2020 due to the increased demand for digital services amidst the pandemic.


ALPHA_VANTAGE_API_KEY = credentials.ALPHA_VANTAGE_API

ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')
ti = TechIndicators(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')


def get_current_price_and_sma(symbol, period=20):
    try:
        # Get daily stock price data
        daily_data, _ = ts.get_daily(symbol=symbol, outputsize='compact')
        print(f"Daily data for {symbol}: {daily_data}")

        # Get the SMA data
        sma_data, _ = ti.get_sma(symbol=symbol, interval='daily', time_period=period)
        print(f"SMA data for {symbol}: {sma_data}")

        # Get the current price (last row of the daily data close price)
        current_price = daily_data['4. close'].iloc[-1]

        # Get the latest SMA value (last row of the SMA data)
        sma_value = sma_data['SMA'].iloc[-1]

        return current_price, sma_value
    except Exception as e:
        print(f"An error occurred while fetching price and SMA for {symbol}: {e}")
        return np.nan, np.nan


def get_rsi(symbol, period=14):
    try:
        # Get RSI data
        rsi_data, _ = ti.get_rsi(symbol=symbol, interval='daily', time_period=period)
        print(f"RSI data for {symbol}: {rsi_data}")

        # Get the latest RSI value (last row of the RSI data)
        rsi_value = rsi_data['RSI'].iloc[-1]

        return rsi_value
    except Exception as e:
        print(f"An error occurred while fetching RSI for {symbol}: {e}")
        return np.nan


blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_name = "historic"
file_name = "company_overviews.csv"

container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(file_name)
blob_data = blob_client.download_blob().readall().decode("utf-8")
if blob_data:
    df = pd.read_csv(StringIO(blob_data))
else:
    print("The blob data is empty.")
    exit(1)

numeric_cols = ["MarketCapitalization", "PERatio", "DividendYield", "RevenuePerShareTTM", "ProfitMargin",
                "OperatingMarginTTM", "ReturnOnAssetsTTM", "ReturnOnEquityTTM", "QuarterlyEarningsGrowthYOY",
                "QuarterlyRevenueGrowthYOY", "AnalystTargetPrice", "TrailingPE", "ForwardPE", "PriceToSalesRatioTTM",
                "PriceToBookRatio", "EVToRevenue", "EVToEBITDA", "Beta"]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# Drop rows where 'Sector' is NaN
df = df.dropna(subset=['Sector'])

average_pe_by_sector = df.groupby('Sector')['PERatio'].mean()
df = df[df.apply(lambda row: row['PERatio'] < average_pe_by_sector[row['Sector']], axis=1)]
min_market_cap = 10_000_000
max_market_cap = 5_000_000_000_000
df = df[(df["MarketCapitalization"] >= min_market_cap) & (df["MarketCapitalization"] <= max_market_cap)]


# Print basic info about the DataFrame
print(df.info())

# Print the first 5 rows of the DataFrame
print(df.head())

# Print the last 5 rows of the DataFrame
print(df.tail())

# Print descriptive statistics of the DataFrame
print(df.describe())


# Apply additional filters and criteria
df = df[df["ProfitMargin"] > -5.5]
print(f"Symbols left after ProfitMargin filter: {df.shape[0]}")

df = df[df["PERatio"] >= 3.5]
print(f"Symbols left after PERatio filter: {df.shape[0]}")

df = df[df["ReturnOnEquityTTM"] >= 2.5]
print(f"Symbols left after ReturnOnEquityTTM filter: {df.shape[0]}")

df = df[df["EVToEBITDA"] >= 1.5]
print(f"Symbols left after EVToEBITDA filter: {df.shape[0]}")

df = df[df["QuarterlyEarningsGrowthYOY"] > 0.078]
print(f"Symbols left after QuarterlyEarningsGrowthYOY filter: {df.shape[0]}")


df = df.sort_values("MarketCapitalization", ascending=False)

df_grouped = df.groupby('Industry')

selected_pairs = pd.DataFrame()

# Select all stocks by Market Capitalization within the specified range
selected_pairs = df[(df["MarketCapitalization"] >= min_market_cap) & (df["MarketCapitalization"] <= max_market_cap)]


# Output the selected pairs, now keeping multiple columns
selected_pairs = selected_pairs[["Symbol", "Sector", "Industry", "MarketCapitalization", "PERatio",
                     "DividendYield", "RevenuePerShareTTM", "ProfitMargin", "OperatingMarginTTM",
                     "ReturnOnAssetsTTM", "ReturnOnEquityTTM", "QuarterlyEarningsGrowthYOY",
                     "QuarterlyRevenueGrowthYOY", "AnalystTargetPrice", "TrailingPE", "ForwardPE",
                     "PriceToSalesRatioTTM", "PriceToBookRatio", "EVToRevenue", "EVToEBITDA", "Beta"]]

# Save to CSV
script_dir = os.path.dirname(os.path.realpath(__file__))
output_filename = "selected_pairs.csv"
output_filepath = os.path.join(script_dir, output_filename)
selected_pairs.to_csv(output_filepath, index=False)

print("Selected pairs saved to 'selected_pairs.csv' locally")


# Get blob client
blob_client = blob_service_client.get_blob_client("historic", "selected_pairs.csv")

# Upload the csv file to Azure Storage
with open(output_filepath, "rb") as data:
    blob_client.upload_blob(data, overwrite=True)
print("Selected pairs saved to 'selected_pairs.csv' in Azure Storage")

delay = 60 / 150  # Delay between requests in seconds

for index, row in selected_pairs.iterrows():
    symbol = row['Symbol']
    current_price, sma_value = get_current_price_and_sma(symbol)
    time.sleep(delay)
    rsi_value = get_rsi(symbol)
    time.sleep(delay)