import credentials
import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from s3connector import azure_connection_string
from azure.core.exceptions import AzureError
import logging

def update_tickers():
    # Connect to Alpha Vantage API
    apikey = credentials.ALPHA_VANTAGE_API
    url = f'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={apikey}'

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception if the request was not successful
        data = response.text
        print("Connected to Alpha Vantage API successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Alpha Vantage API: {e}")
        return

    # Parse the data and create DataFrame
    header = data.split("\n")[0].split(",")
    data_list = []
    for line in data.split("\n")[1:]:
        if line == "":
            continue
        values = line.split(",")
        data_dict = {}
        for i, header_value in enumerate(header):
            data_dict[header_value] = values[i]
        data_list.append(data_dict)

    df = pd.DataFrame(data_list)

    # Convert DataFrame to CSV data in memory
    csv_data = df.to_csv(index=False)

    # Upload the CSV data to Azure Blob Storage
    container_name = "historic"  # Replace with the actual container name

    try:
        blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        container_client.upload_blob(name="tickers.csv", data=csv_data, overwrite=True)
        print("Tickers CSV uploaded to Azure Blob Storage successfully.")
    except AzureError as e:
        print(f"Error uploading CSV to Azure Blob Storage: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")



# Call the function to update the tickers and upload the CSV data
update_tickers()
