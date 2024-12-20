import requests
import csv
import json

# Etherscan API settings
ETHERSCAN_API_KEY = "8NKCB1RPTRCMF2MYD1GDYXYEPPSJFCAC86"  # Replace with your Etherscan API key
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"

def fetch_token_pair(contract_address):
    """
    Fetch token pairs (token0 and token1) using the contract's ABI from Etherscan.
    """
    # Step 1: Get the ABI
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

    # Step 2: Decode the ABI
    try:
        abi = json.loads(abi_data["result"])
    except json.JSONDecodeError:
        print(f"Failed to decode ABI for {contract_address}")
        return {"token0": "N/A", "token1": "N/A"}

    # Step 3: Check for token0 and token1 functions
    token0_found = any(item for item in abi if item.get("name") == "token0")
    token1_found = any(item for item in abi if item.get("name") == "token1")

    if not token0_found or not token1_found:
        print(f"No token0 or token1 functions found in ABI for {contract_address}")
        return {"token0": "N/A", "token1": "N/A"}

    # Step 4: Query the contract for token0 and token1
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

def process_csv(input_csv, output_csv):
    """
    Read contract addresses from the input CSV, fetch token pairs using Etherscan,
    and save the results to a new CSV.
    """
    results = []

    # Read contract addresses from the input CSV
    with open(input_csv, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            contract_address = row["contract_address"].strip()
            print(f"Fetching token pair for contract address: {contract_address}")
            token_pair = fetch_token_pair(contract_address)
            results.append({
                "contract_address": contract_address,
                "token0": token_pair["token0"],
                "token1": token_pair["token1"]
            })

    # Save results to the output CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["contract_address", "token0", "token1"])
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    input_csv = "2.unique_contract_addresses_from_dex_events.csv"  # Input CSV file
    output_csv = "3.1.contract_address_to_tokens"  # Output CSV file

    process_csv(input_csv, output_csv)
    print(f"Data saved to {output_csv}")
