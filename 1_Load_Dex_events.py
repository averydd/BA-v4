import os
import csv
import pymongo
import psycopg2
from psycopg2.extras import execute_values

MONGO_URI = os.getenv("MONGO_URI", "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017")
MONGO_DB = 'ethereum_blockchain_etl'
MONGO_COLLECTION = "dex_events"
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://target_user:target_pass@localhost:5432/target_db")
POSTGRES_TABLE = "dex_events"
CSV_FILE = "dex_events.csv"

# MongoDB Connection
def connect_mongo(uri, db_name, collection_name):
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

# PostgreSQL Connection
def connect_postgres(uri):
    conn = psycopg2.connect(uri)
    return conn

# Extract data from MongoDB and write to CSV
def export_to_csv(collection, csv_file, limit=1000000):
    query = {}
    projection = {
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
    
    events = collection.find(query, projection).limit(limit)
    
    with open(csv_file, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["id"] + list(projection.keys()))
        writer.writeheader()
        for idx, event in enumerate(events, start=1):
            # Add a new identifying column
            sanitized_event = {k: (v if v not in [None, ""] else None) for k, v in event.items() if k in projection}
            sanitized_event["id"] = idx  # Assign a unique identifier
            writer.writerow(sanitized_event)

    print(f"Data exported to {csv_file}")

# Load CSV data to PostgreSQL
def load_csv_to_postgres(csv_file, conn, table_name):
    with open(csv_file, "r") as file:
        reader = csv.DictReader(file)
        rows = []
        for row in reader:
            # Convert empty strings to None for numeric fields
            sanitized_row = tuple(None if v == "" else v for v in row.values())
            rows.append(sanitized_row)
        
    columns = list(reader.fieldnames)
    insert_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES %s
    """

    with conn.cursor() as cursor:
        execute_values(cursor, insert_query, rows)
        conn.commit()

    print(f"Data loaded to PostgreSQL table {table_name}")

# Main Function
if __name__ == "__main__":
    events_collection = connect_mongo(MONGO_URI, MONGO_DB, MONGO_COLLECTION)
    export_to_csv(events_collection, CSV_FILE, limit=1000000)

    postgres_conn = connect_postgres(POSTGRES_URI)
    try:
        load_csv_to_postgres(CSV_FILE, postgres_conn, POSTGRES_TABLE)
    finally:
        postgres_conn.close()
