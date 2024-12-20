import psycopg2
import pandas as pd

# Database connection details
db_url = "postgresql://target_user:target_pass@localhost:5432/target_db"

# File to upload
file_path = "token_pairs_with_names.csv"

def upload_csv_to_postgres(db_url, file_path, table_name):
    try:
        # Read CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Connect to PostgreSQL
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            contract_address VARCHAR,
            token0 VARCHAR,
            token1 VARCHAR,
            name_token0 VARCHAR,
            name_token1 VARCHAR
        );
        """
        cursor.execute(create_table_query)
        conn.commit()

        # Insert data from DataFrame into table
        for index, row in df.iterrows():
            placeholders = ", ".join(["%s" for _ in row])
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))

        conn.commit()
        print("Data uploaded successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Specify the target table name in PostgreSQL
table_name = "contract_info"

# Call the function
upload_csv_to_postgres(db_url, file_path, table_name)
