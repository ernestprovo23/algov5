import sys
import time
import credentials
import pandas as pd
import requests
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from io import StringIO
import logging
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from s3connector import azure_connection_string


# Ensuring output is line buffered
sys.stdout.reconfigure(line_buffering=True)

# Create a session to reuse connections
session = requests.Session()


start_time = datetime.now()
api_key = credentials.ALPHA_VANTAGE_API
max_requests_per_minute = 150
threads_per_minute = max_requests_per_minute


class RateLimiter:
    def __init__(self, max_requests_per_minute):
        self.max_requests_per_minute = max_requests_per_minute
        self.timestamps = deque(maxlen=max_requests_per_minute)

    def request(self):
        if len(self.timestamps) >= self.max_requests_per_minute:
            time_since_oldest = time.time() - self.timestamps[0]
            if time_since_oldest < 60:
                sleep_time = 60 - time_since_oldest
                print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)
        self.timestamps.append(time.time())

rate_limiter = RateLimiter(max_requests_per_minute)


def retrieve_company_overview(api_key, symbol):
    url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}'
    while True:
        rate_limiter.request()
        try:
            response = session.get(url)
            response.raise_for_status()
            data = response.json()
            flat_data = pd.json_normalize(data)
            if not flat_data.empty:
                print(f"Finished processing symbol '{symbol}'.")
                return flat_data.to_dict(orient='records')[0]
            else:
                print(f"No data returned for symbol '{symbol}'.")
                return None
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            return None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return None
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            raise


# Function to save a dataframe as a CSV file in Azure Blob Storage
def save_dataframe_to_csv(dataframe, container_name, filename):
    csv_data = dataframe.to_csv(index=False)
    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        container_client.upload_blob(name=filename, data=csv_data, overwrite=True)
        print(f"Dataframe saved to Azure Blob Storage as '{filename}' successfully.")
    except AzureError as e:
        print(f"Error uploading CSV to Azure Blob Storage: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_name = 'historic'
tickers_file = 'tickers.csv'

try:
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(tickers_file)
    tickers_data = blob_client.download_blob().readall().decode('utf-8')
    print("Retrieved tickers data from Azure Blob Storage successfully.")
    tickers_df = pd.read_csv(StringIO(tickers_data))
    tickers_df.rename(columns={'ticker': 'symbol'}, inplace=True)
    tickers_list = tickers_df['symbol'].tolist()
except AzureError as e:
    print(f"Error retrieving tickers from Azure Blob Storage: {e}")
    exit(1)
except Exception as e:
    print(f"An error occurred: {e}")
    exit(1)


# Function to chunk the ticker list
def chunks(data, SIZE=150):
    it = iter(data)
    for i in range(0, len(data), SIZE):
        if (i + SIZE + 1) > len(data):
            yield data[i:]
        else:
            yield data[i:i + SIZE]


# Function to throttle requests
def throttle_requests(api_key, tickers_chunk):
    futures = []
    for ticker in tickers_chunk:
        futures.append(executor.submit(retrieve_company_overview, api_key, ticker))
    return [future.result() for future in futures]


# Throttling Mechanism
with ThreadPoolExecutor(max_workers=threads_per_minute) as executor:
    start_time = datetime.now()
    company_overviews = []
    for tickers_chunk in chunks(tickers_list):
        company_overviews.extend(throttle_requests(api_key, tickers_chunk))
    print("All threads completed")


# Convert the list to a DataFrame
company_overviews_df = pd.DataFrame(filter(None, company_overviews))

# Save the DataFrame to CSV
save_dataframe_to_csv(company_overviews_df, container_name, 'company_overviews.csv')

# Calculate the total elapsed time
end_time = datetime.now()
total_time = (end_time - start_time).total_seconds()
print(f"Total elapsed time: {total_time:.2f} seconds")

# Create a custom logger
logger = logging.getLogger(__name__)

# Set the level of logger to DEBUG. This means the logger will handle all levels from DEBUG and above.
logger.setLevel(logging.DEBUG)

# Create file handler which logs even debug messages
fh = logging.FileHandler('script.log')
fh.setLevel(logging.DEBUG)

# Create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

# Create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

# Log completion
logger.info("Data retrieval and storage process completed.")


