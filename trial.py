import os
from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configurations
MONGO_URI = os.getenv("MONGO_URI", "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017")
MONGO_DB = "ethereum_blockchain_etl"
MONGO_COLLECTION = "dex_events"
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://target_user:target_pass@localhost:5432/target_db")
POSTGRES_TABLE = "dex_events"
POSTGRES_TABLE_DAILY_STATS = "dex_events_daily_stats"

# Functions
def get_mongo_collection(uri, db_name, collection_name):
    client = MongoClient(uri)
    db = client[db_name]
    return db[collection_name]

def fetch_dex_events(collection, start_date, end_date, batch_size=500):
    query = {
        "block_timestamp": {"$gte": int(start_date.timestamp()), "$lte": int(end_date.timestamp())},
        "event_type": "SWAP"  # Filter by event_type after narrowing by block_timestamp
    }
    projection = {
        "_id": 0,
        "event_type": 1,
        "block_timestamp": 1,
        "contract_address": 1,
        "amount0": 1,
        "amount1": 1,
        "amount0_in": 1,
        "amount0_out": 1,
        "amount1_in": 1,
        "amount1_out": 1,
        "wallet": 1
    }

    cursor = collection.find(query, projection).batch_size(batch_size)
    total_fetched = 0
    results = []

    print("Fetching data in batches...")
    for document in cursor:
        results.append(document)
        total_fetched += 1

        # Print progress after every batch
        if total_fetched % batch_size == 0:
            print(f"Fetched {total_fetched} records so far...")

    print(f"Finished fetching data. Total records fetched: {total_fetched}")
    return results

def pre_aggregate_data(data):
    df = pd.DataFrame(data)
    df["block_timestamp"] = pd.to_datetime(df["block_timestamp"], unit="s")
    df["date"] = df["block_timestamp"].dt.date

    if "amount0" in df.columns and "amount1" in df.columns:
        daily_stats = df.groupby(["date", "contract_address"]).agg(
            total_volume0=("amount0", "sum"),
            total_volume1=("amount1", "sum"),
            transaction_count=("event_type", "count")
        ).reset_index()
    else:
        daily_stats = df.groupby(["date", "contract_address"]).agg(
            total_volume0=("amount0_in", "sum"),
            total_volume1=("amount1_out", "sum"),
            transaction_count=("event_type", "count")
        ).reset_index()

    return daily_stats

def create_postgres_table(engine, table_name, schema):
    metadata = MetaData()
    table = Table(table_name, metadata, *schema)
    metadata.create_all(engine)
    print(f"Table '{table_name}' created successfully.")

def load_to_postgres(dataframe, postgres_uri, table_name, schema):
    engine = create_engine(postgres_uri)

    # Create the table if it doesn't exist
    create_postgres_table(engine, table_name, schema)

    # Load the data into PostgreSQL
    dataframe.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"Data successfully loaded to {table_name} in PostgreSQL.")

# Main script
if __name__ == "__main__":
    # Define schemas
    dex_events_schema = [
        Column("_id", String, primary_key=True),
        Column("event_type", String, nullable=False),
        Column("block_timestamp", Integer, nullable=False),
        Column("contract_address", String, nullable=False),
        Column("amount0", Float, nullable=True),
        Column("amount1", Float, nullable=True),
        Column("amount0_in", Float, nullable=True),
        Column("amount0_out", Float, nullable=True),
        Column("amount1_in", Float, nullable=True),
        Column("amount1_out", Float, nullable=True),
        Column("wallet", String, nullable=True)
    ]

    dex_events_daily_stats_schema = [
        Column("date", String, nullable=False),
        Column("contract_address", String, nullable=False),
        Column("total_volume0", Float, nullable=False),
        Column("total_volume1", Float, nullable=False),
        Column("transaction_count", Integer, nullable=False)
    ]

    # Define date range
    end_date = datetime.now()
    start_date = end_date - timedelta(minutes=1)

    # Fetch data from MongoDB
    collection = get_mongo_collection(MONGO_URI, MONGO_DB, MONGO_COLLECTION)
    print("MongoDB connected")
    data = fetch_dex_events(collection, start_date, end_date)
    print("Data fetched")

    # Pre-aggregate data
    daily_stats = pre_aggregate_data(data)
    print("Finished aggregating data for daily stats")
    # Load data to PostgreSQL
    load_to_postgres(pd.DataFrame(data), POSTGRES_URI, POSTGRES_TABLE, dex_events_schema)
    load_to_postgres(daily_stats, POSTGRES_URI, POSTGRES_TABLE_DAILY_STATS, dex_events_daily_stats_schema)
