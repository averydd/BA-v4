import pandas as pd
import requests

# Etherscan API settings
ETHERSCAN_API_KEY = "8NKCB1RPTRCMF2MYD1GDYXYEPPSJFCAC86"  # Replace with your Etherscan API key
ETHERSCAN_BASE_URL = "https://api.etherscan.io/api"

def fetch_token_details(token_address):
    """
    Fetch the name and symbol of a token from Etherscan API.
    
    Parameters:
        token_address (str): Token contract address.
    
    Returns:
        dict: A dictionary containing the token name and symbol.
    """
    params = {
        "module": "token",
        "action": "tokeninfo",
        "contractaddress": token_address,
        "apikey": ETHERSCAN_API_KEY,
    }
    response = requests.get(ETHERSCAN_BASE_URL, params=params)
    data = response.json()

    if data["status"] == "1":
        return {
            "name": data["result"][0].get("name", "N/A"),
            "symbol": data["result"][0].get("symbol", "N/A"),
        }
    else:
        print(f"Failed to fetch details for token: {token_address}")
        return {"name": "N/A", "symbol": "N/A"}

def decode_tokens(input_csv, output_csv):
    """
    Decode token addresses into their name and symbol.
    
    Parameters:
        input_csv (str): Path to the CSV file containing unique token addresses.
        output_csv (str): Path to save the decoded tokens with their name and symbol.
    """
    # Load unique tokens
    tokens = pd.read_csv(input_csv)

    # Create a list to store results
    decoded_tokens = []

    # Fetch name and symbol for each token
    for token_address in tokens["unique_tokens"]:
        print(f"Fetching details for token: {token_address}")
        details = fetch_token_details(token_address)
        decoded_tokens.append({
            "token_address": token_address,
            "name": details["name"],
            "symbol": details["symbol"]
        })

    # Save the results to a new CSV
    decoded_df = pd.DataFrame(decoded_tokens)
    decoded_df.to_csv(output_csv, index=False)

    print(f"Decoded token details have been saved to {output_csv}")

if __name__ == "__main__":
    # Parameters
    input_csv = "3.2.2.1unique_tokens.csv"  # Input CSV with unique token addresses
    output_csv = "decoded_tokens.csv"  # Output CSV with token details

    # Decode tokens and save to CSV
    decode_tokens(input_csv, output_csv)
