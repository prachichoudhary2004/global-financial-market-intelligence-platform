import pandas as pd
from prophet import Prophet
from xgboost import XGBRegressor
import os
import joblib
import numpy as np

def train_ensemble_models(symbol, gold_path, models_path):
    print(f"Training Institutional Ensemble for {symbol}...")
    df = pd.read_parquet(os.path.join(gold_path, "market_kpis_gold.parquet"))
    symbol_df = df[df['Symbol'] == symbol].sort_values('Date')
    
    # 1. Prophet
    prophet_df = symbol_df[['Date', 'Close']].rename(columns={'Date': 'ds', 'Close': 'y'})
    m_prophet = Prophet(daily_seasonality=True)
    m_prophet.fit(prophet_df)
    joblib.dump(m_prophet, os.path.join(models_path, f"{symbol}_prophet.joblib"))
    
    # 2. XGBoost (Supervised)
    symbol_df['Target'] = symbol_df['Close'].shift(-1)
    train_df = symbol_df.dropna()
    # Ensure all required features exist
    features = ['Close', 'MA20', 'MA50', 'RSI', 'Volatility_20d']
    if all(col in train_df.columns for col in features):
        X = train_df[features]
        y = train_df['Target']
        m_xgb = XGBRegressor(n_estimators=100, learning_rate=0.05)
        m_xgb.fit(X, y)
        joblib.dump(m_xgb, os.path.join(models_path, f"{symbol}_xgb.joblib"))
        print(f"Ensemble for {symbol} saved.")
    else:
        print(f"Skipping XGBoost for {symbol} due to missing features.")

if __name__ == "__main__":
    gold_dir = "global-financial-market-intelligence-platform/data/gold"
    models_dir = "global-financial-market-intelligence-platform/models/forecasting"
    os.makedirs(models_dir, exist_ok=True)
    
    df = pd.read_parquet(os.path.join(gold_dir, "market_kpis_gold.parquet"))
    top_tickers = df['Symbol'].value_counts().head(5).index.tolist()
    
    for t in top_tickers:
        try:
            train_ensemble_models(t, gold_dir, models_dir)
        except Exception as e:
            print(f"Error training {t}: {e}")
