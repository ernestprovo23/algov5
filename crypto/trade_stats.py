from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import csv
import os
from s3connector import azure_connection_string, upload_blob, download_blob



# BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_name = 'historic'  # replace with your container name
blob_name = 'trades.csv'


from datetime import datetime

def record_trade(symbol, qty, price, date=None):
    """
    Record a trade in Azure Blob Storage.
    """
    # Load existing trades
    trades = download_trades()

    # Check if trade already exists
    for trade in trades:
        # If the trade has only three elements (symbol, qty, price), append a default date value
        if len(trade) < 4:
            trade.append('No date')  # Or any default date value you prefer

        if trade[0] == symbol and trade[1] == qty and trade[2] == price and trade[3] == date:
            return  # Trade already exists, so don't record it

    # Set the default date as current datetime if not provided
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Add the new trade
    trades.append([symbol, qty, price, date])

    # Save to CSV file
    file_path = 'trades.csv'
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Symbol", "Quantity", "Price", "Date Traded"])  # Writing headers
        writer.writerows(trades)

    # Upload CSV file to Azure Blob Storage
    upload_blob(blob_service_client, container_name, blob_name, file_path)

    # Delete the local CSV file
    os.remove(file_path)


def download_trades():
    """
    Download the trades CSV file from Azure Blob Storage and return the trades as a list.
    If the trades file does not exist, return an empty list.
    """
    try:
        file_path = 'trades.csv'
        download_blob(blob_service_client, container_name, blob_name, file_path)

        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            next(reader)  # skip headers
            trades = list(reader)

        # Delete the local CSV file
        os.remove(file_path)

        return trades
    except Exception:
        # The trades file does not exist yet
        return []
