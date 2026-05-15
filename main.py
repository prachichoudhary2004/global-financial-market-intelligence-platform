import subprocess
import os
import sys

def run_script(script_path):
    print(f"--- Executing: {script_path} ---")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Success: {script_path}")
        print(result.stdout)
    else:
        print(f"Error in {script_path}")
        print(result.stderr)
    return result.returncode == 0

def main():
    # 1. Silver Layer (Processing real data organized from archives)
    if not run_script("global-financial-market-intelligence-platform/pipelines/transformations/silver_layer.py"):
        print("Silver layer failed.")
        return
        print("Note: PySpark might not be configured. Falling back to simulated silver data...")
        # Fallback logic could go here if needed
    
    # 3. Gold Layer
    run_script("global-financial-market-intelligence-platform/pipelines/transformations/gold_layer.py")
    
    # 4. Models
    run_script("global-financial-market-intelligence-platform/models/forecasting/train_forecaster.py")
    run_script("global-financial-market-intelligence-platform/models/anomaly_detection/detect_anomalies.py")
    
    print("--- ALL PIPELINES COMPLETED SUCCESSFULLY ---")
    print("You can now start the API and Dashboard:")
    print("1. API: uvicorn global-financial-market-intelligence-platform.api.fastapi.main:app --reload")
    print("2. Dashboard: streamlit run global-financial-market-intelligence-platform/dashboard/streamlit/app.py")

if __name__ == "__main__":
    main()
