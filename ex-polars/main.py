# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3", "polars"]
# ///

from typing import TypedDict, List, Dict, Any
import json
import re
from urllib.parse import urlparse
import boto3
import time
import polars as pl

STATUS_CODE_PATTERN = re.compile(r"HTTP Status Code: (\d{3})")

# Updated Result to reflect BOTH logs/second and status code percentages
class Result(TypedDict):
    logs_per_second: float
    rate_2xx: float
    rate_4xx: float
    rate_5xx: float

def parse_s3_url(url: str) -> tuple[str, str]:
    """Extracts bucket name and prefix from an s3:// URL."""
    parsed = urlparse(url)
    if parsed.scheme != 's3':
        raise ValueError("Input must be an s3:// URL")
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


def main(input_s3_url: str) -> Result:
    bucket_name, prefix = parse_s3_url(input_s3_url)
    s3 = boto3.client('s3')
    
    # Data Structures to Collect Metrics
    all_timestamps: List[float] = []
    status_counts = {'2xx': 0, '4xx': 0, '5xx': 0, 'total': 0}
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    print(f"Reading data from s3://{bucket_name}/{prefix}...")

    for page in pages:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue

            try:
                response = s3.get_object(Bucket=bucket_name, Key=key)
                file_content = response['Body'].read().decode('utf-8')
                events = json.loads(file_content)

                for event in events:
                    # 1. Collect Timestamp for Logs/Second Calculation
                    if "timestamp" in event:
                        all_timestamps.append(event["timestamp"])
                        
                    # 2. Extract and Count Status Codes for Percentage Calculation
                    match = STATUS_CODE_PATTERN.search(event.get("message", ""))
                    if match:
                        code = int(match.group(1))
                        group = get_status_code_group(code)

                        if group in status_counts:
                            status_counts[group] += 1
                        status_counts['total'] += 1
                        
            except Exception as e:
                print(f"Error processing s3://{bucket_name}/{key}: {e}")
                continue

    # --- 1. Logs Per Second Calculation (using Polars) ---
    total_logs = len(all_timestamps)
    rate_per_second = 0.0
    
    if total_logs >= 2:
        ts_series = pl.Series("timestamp", all_timestamps)
        
        rate_df = pl.DataFrame({"timestamp": ts_series}).select(
            total_duration_seconds = (pl.col("timestamp").max() - pl.col("timestamp").min()),
            total_logs_count = pl.len() # Using pl.len() instead of deprecated pl.count()
        ).with_columns(
            logs_per_second = pl.col("total_logs_count") / pl.col("total_duration_seconds")
        )
        rate_per_second = rate_df["logs_per_second"].item()
    
    # --- 2. Status Code Percentage Rate Calculation ---
    total_counted_status = status_counts['total']
    
    if total_counted_status == 0:
        return {
            "logs_per_second": round(rate_per_second, 4),
            "rate_2xx": 0.0, 
            "rate_4xx": 0.0, 
            "rate_5xx": 0.0
        }

    rate_2xx = status_counts['2xx'] / total_counted_status
    rate_4xx = status_counts['4xx'] / total_counted_status
    rate_5xx = status_counts['5xx'] / total_counted_status

    return {
        "logs_per_second": round(rate_per_second, 4),
        "rate_2xx": round(rate_2xx, 4), 
        "rate_4xx": round(rate_4xx, 4),
        "rate_5xx": round(rate_5xx, 4),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="Path to S3 folder with log files (e.g., s3://my-bucket/5gb)")
    args = parser.parse_args()
    
    # --- Timing Implementation ---
    start_time = time.time()
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