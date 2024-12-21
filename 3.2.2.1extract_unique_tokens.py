import pandas as pd

def extract_unique_tokens(input_csv, output_csv):
    data = pd.read_csv(input_csv)
    unique_tokens = pd.concat([data['token0'], data['token1']]).dropna().unique()
    unique_tokens_df = pd.DataFrame({'unique_tokens': unique_tokens})
    unique_tokens_df.to_csv(output_csv, index=False)

    print(f"Unique tokens have been saved to {output_csv}")

if __name__ == "__main__":
    # Parameters
    input_csv = "batch1_tokenpair1.csv"  # Input CSV with token0 and token1 columns
    output_csv = "unique_tokens1.csv"      # Output CSV for unique tokens

    # Extract unique tokens and save
    extract_unique_tokens(input_csv, output_csv)
