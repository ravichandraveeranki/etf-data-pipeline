import pandas as pd
import yfinance as yf
import logging
import sys
import matplotlib.pyplot as plt

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def extract_data(ticker):
    df = yf.download(ticker, period="3mo", auto_adjust=False)
    return df


def transform_data(df):
    # flatten columns if yfinance returns multi-index columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] for col in df.columns]

    df = df.reset_index(drop=False)
    df = df.dropna().reset_index(drop=True)

    df["daily_return"] = df["Close"].pct_change()
    df["ma_5"] = df["Close"].rolling(window=5).mean()
    df["volatility_5"] = df["daily_return"].rolling(window=5).std()

    # trading signal
    df["signal"] = 0
    df.loc[df["Close"] > df["ma_5"], "signal"] = 1
    df.loc[df["Close"] < df["ma_5"], "signal"] = -1
    df["strategy_return"] = df["signal"].shift(1) * df["daily_return"]
    df["cumulative_return"] = (1 + df["strategy_return"]).cumprod()

    return df


def plot_data(df, ticker):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10))

    ax1.plot(df["Close"], label="Close Price", linewidth=2)
    ax1.plot(df["ma_5"], label="5-Day MA", linewidth=2)

    buy = df[df["signal"] == 1]
    ax1.scatter(buy.index, buy["Close"], color="green", label="Buy", marker="^")

    sell = df[df["signal"] == -1]
    ax1.scatter(sell.index, sell["Close"], color="red", label="Sell", marker="v")

    ax1.set_title(f"{ticker} Price & Signals")
    ax1.legend()
    ax1.grid(True)

    ax2.plot(df.index, df["cumulative_return"], label="Strategy Return", linewidth=2)
    ax2.set_title(f"{ticker} Strategy Performance")
    ax2.legend()
    ax2.grid(True)

    plt.tight_layout()
    plt.show()

def load_data(df, output_path):
    df.to_csv(output_path, index=False)


def main():
    try:
        if len(sys.argv) > 1:
            ticker = sys.argv[1].upper()
        else:
            ticker = "SPY"

        output_file = "processed_data.csv"
        logging.info(f"Starting ETL for ticker: {ticker}")

        df = extract_data(ticker)

        if df.empty:
            logging.warning(f"No data found for ticker: {ticker}")
            print("Invalid ticker or no data found.")
            return

        df = transform_data(df)
        plot_data(df, ticker)
        load_data(df, output_file)

        logging.info("ETL pipeline completed successfully.")
        print("ETL pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        print("Error occurred:", e)


if __name__ == "__main__":
    main()