#  End-to-End ETF Data Pipeline with Financial Analytics using Python & Pandas

This project is a simple ETL (Extract, Transform, Load) pipeline built using Python and Pandas.
This pipeline processes financial time-series data and computes daily returns for analysis.

## 🔹 Features
- Extracts data from a CSV file
- Transforms data by calculating daily returns
- Loads processed data into a new CSV file

## 🔹 Tech Stack
- Python
- Pandas

## 🔹 Project Structure
- main.py → ETL pipeline script
- sample_data.csv → Input data
- processed_data.csv → Output data

## 🔹 How to Run

Run the pipeline using:

```bash
python main.py
```

Then enter ticker when prompted (e.g., SPY, AAPL)


##  Features Added

- CLI-based execution using `sys.argv`
- Logging using Python `logging` module
- Real-time financial data extraction using `yfinance`
- Error handling and validation for invalid tickers
