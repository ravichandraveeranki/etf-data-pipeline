import pandas as pd

def extract_data(file_path):
    return pd.read_csv(file_path)

def transform_data(df):
    df = df.dropna()
    df["daily_return"] = df["close"].pct_change()
    return df

def load_data(df, output_path):
    df.to_csv(output_path, index=False)

def main():
    input_file = "sample_data.csv"
    output_file = "processed_data.csv"

    df = extract_data(input_file)
    df = transform_data(df)
    load_data(df, output_file)

    print("ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()