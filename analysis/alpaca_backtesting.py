from backtesting import Backtest, Strategy
import alpaca_trade_api as tradeapi
from alpha_vantage.timeseries import TimeSeries
from s3connector import connect_to_storage_account, azure_connection_string, download_blob
import credentials
import io
import pandas as pd

# Azure storage account
blob_service_client = connect_to_storage_account(azure_connection_string)

# Alpha Vantage API key
alpha_vantage_api_key = credentials.ALPHA_VANTAGE_API

# Alpha Vantage time series object
ts = TimeSeries(key=alpha_vantage_api_key, output_format="pandas")

# Function to download CSV from Azure and convert to dataframe
def download_blob_to_dataframe(blob_service_client, container_name, blob_name):
    try:
        print(f"Downloading blob {blob_name} from container {container_name}...")
        container_client = blob_service_client.get_container_client(container_name)
        blob_data = container_client.download_blob(blob_name).readall()
        df = pd.read_csv(io.BytesIO(blob_data))
        print("Download complete.")
        print("First few lines of the DataFrame:")
        print(df.head())  # Print the first few rows of the DataFrame
        df.to_csv('backtesting_test.csv')
        return df
    except Exception as e:
        print(f"Failed to download blob: {e}")
        return pd.DataFrame()


class LSTMStrategy(Strategy):
    def init(self):
        self.prediction = self.data["Predicted"]
        self.account_balance = api.get_account().cash

    def next(self):
        print(f"Current Close Price: {self.data.Close[-1]}, Predicted Close Price: {self.prediction[-1]}")
        if self.data.Close[-1] < self.prediction[-1] and self.data.Close[-1] * 1.01 <= self.prediction[-1]:
            if self.account_balance >= self.data.Close[-1]:
                # Go long
                print(f"Going long. Buying at price: {self.data.Close[-1]}")
                self.buy()
                self.account_balance -= self.data.Close[-1]
        elif self.data.Close[-1] > self.prediction[-1] and self.position:
            # Go short, sell what we bought earlier
            print(f"Going short. Selling at price: {self.data.Close[-1]}")
            self.sell()
            self.account_balance += self.data.Close[-1]

def backtest_model(df, commission=.0005):
    try:
        # Get the account balance from the Alpaca account
        account_balance = float(api.get_account().cash)
        print(f"Account balance: {account_balance}")
        df = df.dropna()  # Ensure there are no NaN values
        bt = Backtest(df, LSTMStrategy, cash=account_balance, commission=commission)
        stats = bt.run()
        return stats
    except Exception as e:
        print(f"Failed to backtest model: {e}")
        return None

# Load your Alpaca API keys
api = tradeapi.REST(credentials.ALPACA_API_KEY, credentials.ALPACA_SECRET_KEY, base_url='https://paper-api.alpaca.markets')

try:
    # Load the list of symbols from the file in Azure
    df_symbols = download_blob_to_dataframe(blob_service_client, "historic", "selected_pairs.csv")

    # Loop through each symbol and backtest
    for symbol in df_symbols['Symbol']:

        print(f"Processing predictions for symbol: {symbol}")

        # Get daily historical data from AlphaVantage
        print("Downloading historical data from AlphaVantage...")
        data, meta_data = ts.get_daily(symbol, outputsize='full')
        data = data[::-1]  # Reverse the data to be in ascending order
        print("Download complete.")

        # The data received from Alpha Vantage contains OHLC prices. You can check it by printing the data.
        print(data.head())

        # Get prediction data
        df_pred = download_blob_to_dataframe(blob_service_client, "historic", f"{symbol}_predictions.csv")
        if df_pred.empty:
            print(f"No prediction data available for symbol: {symbol}. Skipping...")
            continue
        data["Predicted"] = df_pred["Predicted"]

        # Perform backtesting
        stats = backtest_model(data)
        print(stats)
except Exception as e:
    print(f"Failed to process symbols: {e}")

