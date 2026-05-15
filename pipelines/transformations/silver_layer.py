from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lower, trim, when
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("GlobalFinancialIntelligence-Silver") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def process_stocks_to_silver(spark, bronze_path, silver_path):
    print("Processing Stocks to Silver...")
    df = spark.read.csv(os.path.join(bronze_path, "all_stocks_5yr.csv"), header=True, inferSchema=True)
    # Map 'date' to 'Date', 'Name' to 'Symbol'
    df_silver = df.withColumnRenamed("date", "Date") \
                  .withColumnRenamed("Name", "Symbol") \
                  .withColumn("Date", to_date(col("Date")))
    df_silver.write.mode("overwrite").parquet(os.path.join(silver_path, "stocks_silver.parquet"))

def process_crypto_to_silver(spark, bronze_path, silver_path):
    print("Processing Crypto to Silver...")
    df = spark.read.csv(os.path.join(bronze_path, "crypto_prices.csv"), header=True, inferSchema=True)
    # The consolidated crypto file has 'Date' as timestamp string
    df_silver = df.withColumn("Date", to_date(col("Date")))
    df_silver.write.mode("overwrite").parquet(os.path.join(silver_path, "crypto_silver.parquet"))

def process_sentiment_to_silver(spark, bronze_path, silver_path):
    print("Processing Sentiment to Silver...")
    import pandas as pd
    # all-data.csv is headerless: [sentiment, headline]
    pdf = pd.read_csv(os.path.join(bronze_path, "all-data.csv"), encoding='ISO-8859-1', header=None, names=['sentiment', 'headline'])
    df = spark.createDataFrame(pdf)
    df_silver = df.withColumn("sentiment", lower(trim(col("sentiment")))) \
                  .withColumn("headline", trim(col("headline")))
    df_silver.write.mode("overwrite").parquet(os.path.join(silver_path, "sentiment_silver.parquet"))

def process_fundamentals_to_silver(spark, bronze_path, silver_path):
    print("Processing Fundamentals to Silver...")
    df = spark.read.csv(os.path.join(bronze_path, "fundamentals.csv"), header=True, inferSchema=True)
    # Already has Date, Open, Close, Name
    df_silver = df.withColumnRenamed("Name", "Symbol").withColumn("Date", to_date(col("Date")))
    df_silver.write.mode("overwrite").parquet(os.path.join(silver_path, "fundamentals_silver.parquet"))

def process_to_silver_pandas(bronze_path, silver_path):
    print("Spark failed. Falling back to Pandas for Silver Layer...")
    import pandas as pd
    import shutil
    for f in ["stocks_silver.parquet", "crypto_silver.parquet", "sentiment_silver.parquet", "fundamentals_silver.parquet"]:
        p = os.path.join(silver_path, f); 
        if os.path.exists(p): 
            if os.path.isdir(p): shutil.rmtree(p)
            else: os.remove(p)

    # Stocks
    df_stocks = pd.read_csv(os.path.join(bronze_path, "all_stocks_5yr.csv"))
    df_stocks.rename(columns={'date': 'Date', 'Name': 'Symbol', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
    df_stocks['Date'] = pd.to_datetime(df_stocks['Date'])
    df_stocks.to_parquet(os.path.join(silver_path, "stocks_silver.parquet"), index=False)
    
    # Crypto
    df_crypto = pd.read_csv(os.path.join(bronze_path, "crypto_prices.csv"))
    # Crypto already has Title Case from the original Kaggle files
    df_crypto['Date'] = pd.to_datetime(df_crypto['Date']).dt.date
    df_crypto.to_parquet(os.path.join(silver_path, "crypto_silver.parquet"), index=False)
    
    # ... (Rest of the logic)
    
    # Sentiment
    df_sent = pd.read_csv(os.path.join(bronze_path, "all-data.csv"), encoding='ISO-8859-1', header=None, names=['sentiment', 'headline'])
    df_sent.to_parquet(os.path.join(silver_path, "sentiment_silver.parquet"), index=False)
    
    # Fundamentals
    df_fund = pd.read_csv(os.path.join(bronze_path, "fundamentals.csv"))
    df_fund.rename(columns={'Name': 'Symbol'}, inplace=True)
    df_fund['Date'] = pd.to_datetime(df_fund['Date'])
    df_fund.to_parquet(os.path.join(silver_path, "fundamentals_silver.parquet"), index=False)

if __name__ == "__main__":
    bronze_dir = "global-financial-market-intelligence-platform/data/bronze"
    silver_dir = "global-financial-market-intelligence-platform/data/silver"
    os.makedirs(silver_dir, exist_ok=True)
    
    try:
        spark = create_spark_session()
        process_stocks_to_silver(spark, bronze_dir, silver_dir)
        process_crypto_to_silver(spark, bronze_dir, silver_dir)
        process_sentiment_to_silver(spark, bronze_dir, silver_dir)
        process_fundamentals_to_silver(spark, bronze_dir, silver_dir)
        spark.stop()
    except Exception as e:
        print(f"Spark Error: {e}")
        process_to_silver_pandas(bronze_dir, silver_dir)
