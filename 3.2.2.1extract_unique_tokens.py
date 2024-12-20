import pandas as pd

def extract_unique_tokens(input_csv, output_csv):
    """
    Extract unique values from token0 and token1 columns, group them, and save to a new CSV.

    Parameters:
        input_csv (str): Path to the input CSV file.
        output_csv (str): Path to save the output CSV file with unique tokens.
    """
    # Load the input CSV
    data = pd.read_csv(input_csv)

    # Combine token0 and token1 into a single Series and get unique values
    unique_tokens = pd.concat([data['token0'], data['token1']]).dropna().unique()

    # Create a DataFrame for the unique tokens
    unique_tokens_df = pd.DataFrame({'unique_tokens': unique_tokens})

    # Save the unique tokens to the output CSV
    unique_tokens_df.to_csv(output_csv, index=False)

    print(f"Unique tokens have been saved to {output_csv}")

if __name__ == "__main__":
    # Parameters
    input_csv = "3.2.1token_pairs_output.csv"  # Input CSV with token0 and token1 columns
    output_csv = "3.2.2.1unique_tokens.csv"      # Output CSV for unique tokens

    # Extract unique tokens and save
    extract_unique_tokens(input_csv, output_csv)
