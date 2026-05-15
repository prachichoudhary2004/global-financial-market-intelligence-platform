import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate_stock_data(path):
    """Simulates S&P 500 data (Kaggle Dataset 1)"""
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    dates = pd.date_range(start='2020-01-01', end='2023-12-31', freq='D')
    data = []
    for ticker in tickers:
        price = np.random.uniform(100, 500)
        for date in dates:
            change = np.random.normal(0, 0.02)
            price *= (1 + change)
            data.append([date, ticker, price * 0.98, price * 1.02, price * 0.97, price, np.random.randint(1000000, 5000000), ticker])
    
    df = pd.DataFrame(data, columns=['Date', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'Name'])
    df.to_csv(os.path.join(path, 'all_stocks_5yr.csv'), index=False)
    print(f"Generated Stock Data: {len(df)} rows")

def generate_crypto_data(path):
    """Simulates Crypto Price History (Kaggle Dataset 2)"""
    coins = ['Bitcoin', 'Ethereum', 'Solana']
    dates = pd.date_range(start='2020-01-01', end='2023-12-31', freq='D')
    data = []
    for coin in coins:
        price = 40000 if coin == 'Bitcoin' else 2000
        for date in dates:
            change = np.random.normal(0, 0.05)
            price *= (1 + change)
            data.append([date, coin, coin[:3].upper(), price, price * 1.05, price * 0.95, price, np.random.randint(100000, 1000000), np.random.uniform(1e9, 1e10)])
            
    df = pd.DataFrame(data, columns=['Date', 'Coin', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'MarketCap'])
    df.to_csv(os.path.join(path, 'crypto_prices.csv'), index=False)
    print(f"Generated Crypto Data: {len(df)} rows")

def generate_sentiment_data(path):
    """Simulates Financial News Sentiment (Kaggle Dataset 3)"""
    sentiments = ['neutral', 'positive', 'negative']
    headlines = [
        "Stock markets rally on positive earnings",
        "Fed signals potential rate hike",
        "Tech sector sees major correction",
        "Crypto adoption grows in emerging markets",
        "Oil prices stabilize after supply concerns"
    ]
    data = []
    for i in range(1000):
        sentiment = np.random.choice(sentiments)
        headline = np.random.choice(headlines)
        data.append([sentiment, headline])
        
    df = pd.DataFrame(data, columns=['sentiment', 'headline'])
    df.to_csv(os.path.join(path, 'all-data.csv'), index=False, encoding='ISO-8859-1')
    print(f"Generated Sentiment Data: {len(df)} rows")

def generate_fundamentals_data(path):
    """Simulates Company Fundamentals (Kaggle Dataset 4)"""
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
    data = []
    for ticker in tickers:
        data.append([
            ticker,
            np.random.uniform(20, 40), # PE Ratio
            np.random.uniform(10, 30), # Profit Margin
            np.random.uniform(100, 1000), # Market Cap (B)
            np.random.uniform(5, 15) # Dividend Yield
        ])
    df = pd.DataFrame(data, columns=['Symbol', 'PE_Ratio', 'Profit_Margin', 'Market_Cap_B', 'Dividend_Yield'])
    df.to_csv(os.path.join(path, 'fundamentals.csv'), index=False)
    print(f"Generated Fundamentals Data: {len(df)} rows")

if __name__ == "__main__":
    bronze_path = "global-financial-market-intelligence-platform/data/bronze"
    os.makedirs(bronze_path, exist_ok=True)
    generate_stock_data(bronze_path)
    generate_crypto_data(bronze_path)
    generate_sentiment_data(bronze_path)
    generate_fundamentals_data(bronze_path)
