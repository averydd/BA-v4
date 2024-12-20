import requests
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

    #print(f"ABI Response: {json.dumps(abi_data, indent=2)}")  # Debug: Print the ABI response

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

if __name__ == "__main__":
    contract_address = "0x0149ebe930260ccfdaaa8e3081b4c39446b6f491"  # Example contract address
    print(f"Fetching token pair for contract address: {contract_address}")
    token_pair = fetch_token_pair(contract_address)
    print(f"Token Pair: {token_pair}")
