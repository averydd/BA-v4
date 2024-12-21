import requests
import csv
import json
import time

# Etherscan API settings
ETHERSCAN_API_KEY = "UXJ4E8VQAW7Z4QSXNKVCA6YDP3B13NXZ6Q"  # Replace with your Etherscan API key
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"

BATCH_SIZE = 20  # Number of contract addresses to process in a batch
RATE_LIMIT_DELAY = 0.1  # Seconds to wait between batches to comply with API limits

def fetch_token_pair(contract_address):
    """
    Fetch token pairs (token0 and token1) using the contract's ABI from Etherscan.
    """
    abi_params = {
        "module": "contract",
        "action": "getabi",
        "address": contract_address,
        "apikey": ETHERSCAN_API_KEY,
    }
    abi_response = requests.get(ETHERSCAN_BASE_URL, params=abi_params)
    abi_data = abi_response.json()

    if abi_data["status"] != "1":
        print(f"Failed to fetch ABI for {contract_address}")
        return {"token0": "N/A", "token1": "N/A"}

    try:
        abi = json.loads(abi_data["result"])
    except json.JSONDecodeError:
        print(f"Failed to decode ABI for {contract_address}")
        return {"token0": "N/A", "token1": "N/A"}

    token0_found = any(item for item in abi if item.get("name") == "token0")
    token1_found = any(item for item in abi if item.get("name") == "token1")

    if not token0_found or not token1_found:
        print(f"No token0 or token1 functions found in ABI for {contract_address}")
        return {"token0": "N/A", "token1": "N/A"}

    contract_params = {
        "module": "proxy",
        "action": "eth_call",
        "to": contract_address,
        "data": "0x0dfe1681",  # token0() function signature
        "apikey": ETHERSCAN_API_KEY,
    }
    token0_response = requests.get(ETHERSCAN_BASE_URL, params=contract_params)
    token0_address = token0_response.json().get("result", "N/A")

    contract_params["data"] = "0xd21220a7"  # token1() function signature
    token1_response = requests.get(ETHERSCAN_BASE_URL, params=contract_params)
    token1_address = token1_response.json().get("result", "N/A")

    return {
        "token0": "0x" + token0_address[-40:] if token0_address != "N/A" else "N/A",
        "token1": "0x" + token1_address[-40:] if token1_address != "N/A" else "N/A",
    }

def process_csv_in_batches(input_csv, output_csv):
    """
    Process the input CSV in batches and fetch token pairs. Write batch results to the output CSV immediately.
    """
    with open(input_csv, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    # Open the output CSV file once and append results batch by batch
    with open(output_csv, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["contract_address", "token0", "token1"])
        writer.writeheader()  # Write the header only once at the start

        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            print(f"Processing batch {i // BATCH_SIZE + 1}...")

            batch_results = []
            for row in batch:
                contract_address = row["contract_address"].strip()
                print(f"Fetching token pair for contract address: {contract_address}")
                token_pair = fetch_token_pair(contract_address)
                batch_results.append({
                    "contract_address": contract_address,
                    "token0": token_pair["token0"],
                    "token1": token_pair["token1"]
                })

            # Write the current batch results to the CSV
            writer.writerows(batch_results)

            # Optional: Print to console for confirmation
            print(f"Batch {i // BATCH_SIZE + 1} results written to {output_csv}")

            time.sleep(RATE_LIMIT_DELAY)  # Delay between batches

if __name__ == "__main__":
    input_csv = "batch12.csv"  # Input CSV file
    output_csv = "batch1_tokenpair12.csv"  # Output CSV file

    process_csv_in_batches(input_csv, output_csv)
    print(f"Data saved to {output_csv}")
