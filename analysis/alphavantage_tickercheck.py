import requests
import time
from concurrent.futures import ThreadPoolExecutor
import credentials

def check_api_limit(api_key):
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": "IBM",
        "apikey": api_key
    }

    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(requests.get, url, params=params) for _ in range(150)}
        error_count = 0

        for future in futures:
            response = future.result()
            if "Error Message" in response.text:
                error_count += 1

    return error_count

api_key = credentials.ALPHA_VANTAGE_API
errors = check_api_limit(api_key)

if errors > 0:
    print(f"Your API key is not valid for 150 requests per minute, encountered {errors} errors.")
else:
    print("Your API key is valid for 150 requests per minute.")
