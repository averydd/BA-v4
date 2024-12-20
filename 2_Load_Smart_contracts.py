import psycopg2
import pymongo
import json
from psycopg2.extras import execute_values

MONGO_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"
DATABASE = "knowledge_graph"  # Replace with your database name
COLLECTION = "smart_contracts"  # Replace with your collection name
POSTGRES_URI = "postgresql://target_user:target_pass@localhost:5432/target_db"

# MongoDB Connection
def connect_mongo(uri, db_name, collection_name):
    """Connect to MongoDB and return the specified collection."""
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    return collection

# Postgres Connection
def connect_postgres(uri):
    """Connect to Postgres using a URI."""
    return psycopg2.connect(uri)

# Fetch Data from MongoDB
def fetch_smart_contracts(collection, limit=1000000):
    """Fetch smart contract data from MongoDB."""
    contracts = collection.find().limit(limit)
    relevant_data = []
    for contract in contracts:
        relevant_data.append({
            "address": contract.get("address"),
            "name": contract.get("name"),
            "project": contract.get("project"),
            "projectDapp": contract.get("projectDapp"),
            "tags": contract.get("tags"),  # JSON-serializable field
            "categories": contract.get("categories"),  # JSON-serializable field
            "symbol": contract.get("symbol"),
            "decimals": contract.get("decimals"),
            "chainId": contract.get("chainId"),
        })
    return relevant_data

# Insert Data into Postgres
def load_to_postgres(conn, data):
    """Insert smart contract data into Postgres."""
    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO smart_contracts (
                address, name, project, projectDapp, tags, categories, symbol, decimals, chainId
            ) VALUES %s
            ON CONFLICT (address) DO NOTHING;
        """
        # Prepare data for bulk insert
        values = [
            (
                item["address"], item["name"], item["project"], item["projectDapp"],
                json.dumps(item["tags"]) if item["tags"] else None,
                json.dumps(item["categories"]) if item["categories"] else None,
                item["symbol"], item["decimals"], item["chainId"]
            )
            for item in data
        ]
        execute_values(cur, insert_query, values)
        conn.commit()

# Main Execution
if __name__ == "__main__":
    try:
        # Connect to MongoDB and Postgres
        mongo_collection = connect_mongo(MONGO_URI, DATABASE, COLLECTION)
        postgres_conn = connect_postgres(POSTGRES_URI)

        # Fetch data and load into Postgres
        contracts_data = fetch_smart_contracts(mongo_collection)
        load_to_postgres(postgres_conn, contracts_data)

        print(f"Loaded {len(contracts_data)} smart contracts into Postgres.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'postgres_conn' in locals() and postgres_conn:
            postgres_conn.close()
