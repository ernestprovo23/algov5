import time
import requests
import pandas as pd
import logging
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from io import StringIO
import credentials
from s3connector import azure_connection_string

# Set up logging
logging.basicConfig(filename='script.log', level=logging.INFO)

api_key = credentials.ALPHA_VANTAGE_API
max_requests_per_minute = 150
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_name = 'historic'
tickers_file = 'tickers.csv'

def send_teams_message(teams_url, message):
    data = {'text': message}
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(teams_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        logging.info(f"Message sent to Teams successfully: {message}")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred when sending message to Teams: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred when sending message to Teams: {req_err}")
    except Exception as e:
        logging.error(f"Unexpected error occurred when sending message to Teams: {e}")

teams_url = 'https://data874.webhook.office.com/webhookb2/9cb96ee7-c2ce-44bc-b4fe-fe2f6f308909@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/7e8bd751e7b4457aba27a1fddc7e8d9f/6d2e1385-bdb7-4890-8bc5-f148052c9ef5'

send_teams_message(teams_url, "Script started.")

def retrieve_company_overview(api_key, symbol):
    url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if data:
            print(f"Finished processing symbol '{symbol}'.")
            logging.info(f"Finished processing symbol '{symbol}'.")
            return data
        else:
            logging.warning(f"No data returned for symbol '{symbol}'.")
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error occurred: {req_err}")
    except Exception as e:
        logging.error(f"Unexpected error occurred: {e}")

def save_dataframe_to_csv(dataframe, container_name, filename):
    csv_data = dataframe.to_csv(index=False)
    try:
        container_client = blob_service_client.get_container_client(container_name)
        container_client.upload_blob(name=filename, data=csv_data, overwrite=True)
        logging.info(f"Dataframe saved to Azure Blob Storage as '{filename}' successfully.")
        send_teams_message(teams_url, f"Dataframe saved to Azure Blob Storage as '{filename}' successfully.")
    except AzureError as e:
        logging.error(f"Error uploading CSV to Azure Blob Storage: {e}")

try:
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(tickers_file)
    tickers_data = blob_client.download_blob().readall().decode('utf-8')
    logging.info("Retrieved tickers data from Azure Blob Storage successfully.")
    send_teams_message(teams_url, "Retrieved tickers data from Azure Blob Storage successfully.")
    tickers_df = pd.read_csv(StringIO(tickers_data))
    tickers_df.rename(columns={'ticker': 'symbol'}, inplace=True)
    tickers_list = tickers_df['symbol'].tolist()
except AzureError as e:
    logging.error(f"Error retrieving tickers from Azure Blob Storage: {e}")

company_overviews = []

for symbol in tickers_list:
    company_overviews.append(retrieve_company_overview(api_key, symbol))
    # Wait 0.4 seconds after each request to respect the rate limit of 150 requests per minute
    time.sleep(0.4)

# Convert the list to a DataFrame
company_overviews_df = pd.DataFrame(company_overviews)

# Save the DataFrame to CSV
save_dataframe_to_csv(company_overviews_df, container_name, 'company_overviews.csv')

send_teams_message(teams_url, "Script finished successfully.")
