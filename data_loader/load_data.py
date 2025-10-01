import os
import time
import requests
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def get_financial_data():
    """
    Fetches sample financial data from an API.
    Replace this with your actual API endpoint and logic.
    """
    # Using a free sample API for this example
    api_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    print("Fetching data from API...")
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

def load_data_to_mongo(data):
    """
    Connects to MongoDB and loads the given data.
    """
    # Get MongoDB connection details from environment variables
    mongo_user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    mongo_pass = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    mongo_host = "mongodb" # This is the service name in docker-compose.yml

    # Connection string
    mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/"

    print(f"Attempting to connect to MongoDB at {mongo_host}...")
    
    # Retry connecting to MongoDB, as it might take a moment to start up
    max_retries = 10
    retry_delay = 5 # seconds
    for attempt in range(max_retries):
        try:
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
            print("MongoDB connection successful.")
            
            db = client.financial_data
            collection = db.crypto_prices
            
            # Insert data with a timestamp
            for asset, prices in data.items():
                document = {
                    "asset": asset,
                    "price_usd": prices['usd'],
                    "timestamp": time.time()
                }
                collection.insert_one(document)
            
            print(f"Successfully inserted {len(data)} documents into the 'crypto_prices' collection.")
            client.close()
            return
        except ConnectionFailure as e:
            print(f"Attempt {attempt + 1}/{max_retries}: MongoDB connection failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    print("Could not connect to MongoDB after several retries. Exiting.")

if __name__ == "__main__":
    financial_data = get_financial_data()
    if financial_data:
        load_data_to_mongo(financial_data)
