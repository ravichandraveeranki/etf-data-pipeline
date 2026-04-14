# 📈 ETF Trading Signal Pipeline (Python)

## 🚀 Overview
This project is an end-to-end ETL pipeline that extracts financial data, transforms it into meaningful features, generates trading signals, and visualizes strategy performance.

It uses real market data to simulate a simple trading strategy based on moving averages.

---

## 🛠️ Tech Stack
- Python
- Pandas
- yfinance
- Matplotlib

---

## ⚙️ Features
- 📊 Real-time data extraction using yfinance
- 🔄 Data transformation & feature engineering
- 📉 5-day moving average calculation
- 📈 Volatility calculation (rolling standard deviation)
- 💡 Trading signal generation (Buy/Sell)
- 📉 Strategy backtesting with cumulative returns
- 📊 Visualization of price, signals, and performance
- 🧾 Logging using Python logging module
## ▶️ How to Run

```bash
pip install -r requirements.txt
python main.py AAPL

---

## 📂 Project Structure
## 💡 Key Insight

This project demonstrates how simple moving average-based strategies can generate trading signals, and how these signals can be evaluated using cumulative returns to assess performance.

While simplistic, it provides a foundation for more advanced quantitative trading models.
