from fastapi import FastAPI, Query
import pandas as pd
import os
from typing import Optional

app = FastAPI(title="Global Financial Market Intelligence API")

DATA_PATH = "global-financial-market-intelligence-platform/data/gold"
GOLD_PATH = "global-financial-market-intelligence-platform/data/gold"

@app.get("/")
def read_root():
    return {"status": "online", "platform": "Global Financial Market Intelligence Platform"}

@app.get("/market-overview")
async def market_overview():
    df = pd.read_parquet(os.path.join(GOLD_PATH, "market_kpis_gold.parquet"))
    latest = df.sort_values('Date').groupby('Symbol').tail(1)
    return latest.to_dict(orient="records")

@app.get("/risk-metrics")
async def risk_metrics():
    df = pd.read_parquet(os.path.join(GOLD_PATH, "portfolio_gold.parquet"))
    return df[['Symbol', 'Beta', 'Sharpe_Ratio', 'Volatility_20d']].to_dict(orient="records")

@app.get("/sentiment-momentum")
async def sentiment_momentum():
    df = pd.read_parquet(os.path.join(GOLD_PATH, "sentiment_gold.parquet"))
    return df.to_dict(orient="records")[0]

@app.get("/top-movers")
async def top_movers(limit: int = 10):
    df = pd.read_parquet(os.path.join(GOLD_PATH, "market_kpis_gold.parquet"))
    latest = df.sort_values('Date').groupby('Symbol').tail(1)
    return latest.sort_values('Daily_Return', ascending=False).head(limit).to_dict(orient="records")

@app.get("/anomalies")
def get_anomalies():
    path = "global-financial-market-intelligence-platform/models/anomaly_detection/anomalies_gold.parquet"
    if os.path.exists(path):
        df = pd.read_parquet(path)
        return df.sort_values('Date', ascending=False).head(50).to_dict(orient="records")
    return []

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
