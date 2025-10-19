# /// script
# requires-python = ">=3.12"
# dependencies = ["pyspark"]
# ///
import time
from typing import TypedDict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from urllib.parse import urlparse

# Define the expected output structure
class Result(TypedDict):
    logs_per_second: float
    rate_2xx: float
    rate_4xx: float
    rate_5xx: float

# Define the schema for better performance when reading JSON
# Spark expects the JSON array structure [{"service": "...", "timestamp": ...}, ...]
schema = StructType([
    StructField("service", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("message", StringType(), True)
])

def parse_s3_url(url: str) -> tuple[str, str]:
    """Extracts bucket name and prefix from an s3:// URL."""
    parsed = urlparse(url)
    if parsed.scheme != 's3':
        raise ValueError("Input must be an s3:// URL")
    # path is usually '/prefix/sub_prefix/'
    prefix = parsed.path.lstrip('/').rstrip('/')
    return parsed.netloc, prefix

def get_status_code_group(code: int) -> str:
    """Groups status codes into 2xx, 4xx, or 5xx."""
    if 200 <= code <= 299:
        return '2xx'
    elif 400 <= code <= 499:
        return '4xx'
    elif 500 <= code <= 599:
        return '5xx'
    return 'other'

# Define a UDF to extract the status code group
def status_group_udf(message):
    if not message:
        return 'other'
    
    # Simple extraction logic: find the first 3-digit number after "HTTP Status Code: "
    try:
        code_str = message.split("HTTP Status Code: ")[1][:3]
        code = int(code_str)
        
        if 200 <= code <= 299:
            return '2xx'
        elif 400 <= code <= 499:
            return '4xx'
        elif 500 <= code <= 599:
            return '5xx'
    except:
        pass
    return 'other'

# Register the UDF
spark_status_group_udf = F.udf(status_group_udf, StringType())


def main(input_s3_url: str) -> Result:
    # Set up Spark Session
    # For S3 access, you often need to configure Hadoop settings for AWS (like access keys or IAM roles)
    # The simplest way is to ensure your environment/IAM role grants Spark access.
    spark = (
        SparkSession.builder
        .appName("S3LogRateAnalysis")
        .getOrCreate()
    )
    
    # 1. Read the JSON Files from S3
    # PySpark will read all JSON files matching the pattern in parallel.
    s3_path = input_s3_url + "/*"
    print(f"Reading data from {s3_path}...")
    
    # The logs are arrays of JSON objects, so we use 'json' format and multiline mode
    # If the files were newline-delimited, we'd use 'json' without multiline.
    df = (
        spark.read
        .schema(schema)
        .option("multiLine", "true") 
        .json(s3_path)
    )

    # Cache the DataFrame for multiple passes/calculations (optional but good practice)
    df.cache()

    # Get the total count of logs
    total_logs = df.count()
    
    if total_logs < 2:
        spark.stop()
        return {"logs_per_second": 0.0, "rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}

    # 2. Logs Per Second Calculation (Duration)
    # Spark is excellent for distributed aggregation
    duration_stats = df.agg(
        F.max("timestamp").alias("max_ts"),
        F.min("timestamp").alias("min_ts")
    ).collect()[0]

    max_ts = duration_stats["max_ts"]
    min_ts = duration_stats["min_ts"]
    
    total_duration_seconds = max_ts - min_ts
    
    # Calculate logs per second
    logs_per_second = total_logs / total_duration_seconds if total_duration_seconds > 0 else 0.0


    # 3. Status Code Percentage Rate Calculation
    
    # Add a column for the status code group (2xx, 4xx, 5xx)
    df_with_group = df.withColumn(
        "status_group",
        spark_status_group_udf(F.col("message"))
    )
    
    # Count occurrences of each group
    status_counts = (
        df_with_group.groupBy("status_group")
        .count()
        .collect()
    )
    
    # Convert results to a dictionary for easy access
    counts_dict = {row["status_group"]: row["count"] for row in status_counts}
    
    total_counted_status = counts_dict.get('2xx', 0) + counts_dict.get('4xx', 0) + counts_dict.get('5xx', 0)
    
    if total_counted_status == 0:
        rate_2xx, rate_4xx, rate_5xx = 0.0, 0.0, 0.0
    else:
        rate_2xx = counts_dict.get('2xx', 0) / total_counted_status
        rate_4xx = counts_dict.get('4xx', 0) / total_counted_status
        rate_5xx = counts_dict.get('5xx', 0) / total_counted_status
        
    # Stop Spark Session
    spark.stop()

    return {
        "logs_per_second": round(logs_per_second, 4),
        "rate_2xx": round(rate_2xx, 4),
        "rate_4xx": round(rate_4xx, 4),
        "rate_5xx": round(rate_5xx, 4),
    }


if __name__ == "__main__":
    import argparse
    
    # Spark often requires a few seconds to start up
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="Path to S3 folder with log files (e.g., s3://my-bucket/5gb)")
    args = parser.parse_args()
    
    result = main(args.input)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("\n--- RESULTS ---")
    print(f"Overall Log Rate: {result['logs_per_second']:.4f} logs/second")
    print(f"2xx Success Rate: {result['rate_2xx'] * 100:.2f}%")
    print(f"4xx Client Error Rate: {result['rate_4xx'] * 100:.2f}%")
    print(f"5xx Server Error Rate: {result['rate_5xx'] * 100:.2f}%")
    print(f"Execution Time (Wall Clock): {elapsed_time:.4f} seconds")
    print("---------------")