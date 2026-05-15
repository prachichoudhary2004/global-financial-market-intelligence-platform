import pandas as pd
import numpy as np
import os

def detect_anomalies(data_path, output_path):
    print("Running Anomaly Detection...")
    df = pd.read_parquet(os.path.join(data_path, "market_kpis_gold.parquet"))
    
    # Simple Z-score for daily returns
    df['Return_Mean'] = df.groupby('Symbol')['Daily_Return'].transform('mean')
    df['Return_Std'] = df.groupby('Symbol')['Daily_Return'].transform('std')
    df['Z_Score'] = (df['Daily_Return'] - df['Return_Mean']) / df['Return_Std']
    
    # Anomalies are points where Z-score > 3 (3 sigma)
    anomalies = df[np.abs(df['Z_Score']) > 3].copy()
    anomalies['Anomaly_Type'] = 'Volatility Spike'
    
    os.makedirs(output_path, exist_ok=True)
    anomalies.to_parquet(os.path.join(output_path, "anomalies_gold.parquet"))
    print(f"Detected {len(anomalies)} anomalies.")

if __name__ == "__main__":
    gold_dir = "global-financial-market-intelligence-platform/data/gold"
    anomalies_dir = "global-financial-market-intelligence-platform/models/anomaly_detection"
    detect_anomalies(gold_dir, anomalies_dir)
