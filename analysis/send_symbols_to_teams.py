import pandas as pd
import requests
import json
from requests.exceptions import RequestException
from s3connector import download_blob, connect_to_storage_account, azure_connection_string


def send_teams_message(webhook_url, message):
    """
    Sends a message to a Microsoft Teams channel.
    """
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "text": message
    }
    response = requests.post(webhook_url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        raise Exception(f"Message to Teams could not be sent. Status code: {response.status_code}\nText: {response.text}")

def main():
    try:
        # Connect to Azure storage
        blob_service_client = connect_to_storage_account(azure_connection_string)

        # Download CSV file from blob
        csv_file = 'selected_pairs.csv'
        download_blob(blob_service_client, 'historic', 'selected_pairs.csv', csv_file)

        # Read CSV into DataFrame
        df = pd.read_csv(csv_file)

        # Get unique list of symbols
        symbols = df['Symbol'].unique().tolist()

        # Drop duplicates to get unique companies
        unique_df = df.drop_duplicates(subset=['Symbol'])

        def format_market_cap(value):
            """
            Formats the market cap number into a readable string with a dollar sign and the appropriate suffix.
            """
            value = float(value)
            if value >= 1e9:
                return f"${value / 1e9:.2f}B"
            elif value >= 1e6:
                return f"${value / 1e6:.2f}M"
            else:
                return f"${value:.2f}"

        # Create message with symbols list
        message = "Here is the updated company watchlist:  \n\n| Symbol | Sector | Industry | Market Cap |\n|---|---|---|---|\n"
        for _, row in unique_df.iterrows():
            market_cap = format_market_cap(row['MarketCapitalization'])
            message += f"| {row['Symbol']} | {row['Sector']} | {row['Industry']} | {market_cap} |\n"

        # Send message to Teams
        teams_url = "https://data874.webhook.office.com/webhookb2/6b06c5f8-e95d-4924-ac0a-60c08373f6ae@4f84582a-9476-452e-a8e6-0b57779f244f/IncomingWebhook/c30547ea4b684738a1c7cf8930fd742b/6d2e1385-bdb7-4890-8bc5-f148052c9ef5"
        send_teams_message(teams_url, message)

        # Get unique list of symbols
        symbols = df['Symbol'].unique().tolist()

        # Create a string with comma-separated symbols
        symbols_str = ', '.join(symbols)

        message_string = f"{symbols_str}"

        send_teams_message(teams_url, message_string)

    except FileNotFoundError:
        print(f"File {csv_file} not found.")
    except RequestException as e:
        print(f"An error occurred when trying to send a Teams message: {str(e)}")

if __name__ == "__main__":
    main()
