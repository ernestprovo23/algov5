azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=dataexperts0101;AccountKey=nuvNVlxFcJu6oyvlZmPG+PVgXfJAXcVF3xhCdv0kPocwfvxMH7M7n4UKAmh8Cj06rnLu48wf4YUf+ASt1ld2ug==;EndpointSuffix=core.windows.net"

from azure.storage.blob import BlobServiceClient

def connect_to_storage_account(connection_string):
    """
    Connects to the Azure Storage Account using the provided connection string.
    Returns a BlobServiceClient object.
    """
    return BlobServiceClient.from_connection_string(connection_string)

def list_containers(blob_service_client):
    """
    Lists all containers in the Azure Storage Account.
    """
    containers = blob_service_client.list_containers()
    for container in containers:
        print(container.name)

def upload_blob(blob_service_client, container_name, blob_name, file_path):
    """
    Uploads a file as a blob to the specified container in the Azure Storage Account.
    """
    container_client = blob_service_client.get_container_client(container_name)
    with open(file_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

def download_blob(blob_service_client, container_name, blob_name, file_path):
    """
    Downloads a blob from the specified container in the Azure Storage Account and saves it to a local file.
    """
    container_client = blob_service_client.get_container_client(container_name)
    with open(file_path, "wb") as file:
        blob_data = container_client.download_blob(blob_name)
        blob_data.readinto(file)
