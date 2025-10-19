# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb", "boto3"]
# ///

from typing import TypedDict
from urllib.parse import urlparse
import duckdb
import boto3
import time
import json
import re # Necesario si usas el patrón regex en Python, aunque el código final no lo usa.

class Result(TypedDict):
    rate_2xx: float
    rate_4xx: float
    rate_5xx: float

# Patrón regex (aunque lo usaremos en SQL, lo definimos por consistencia)
STATUS_CODE_PATTERN = 'HTTP Status Code: (\\d{3})'

def parse_s3_url(url: str) -> str:
    """Standardizes the S3 path for DuckDB's globbing."""
    parsed = urlparse(url)
    if parsed.scheme != 's3':
        raise ValueError("Input must be an s3:// URL")
    
    return url.rstrip('/') + '/*.json'

def main(input_s3_url: str) -> Result:
    s3_glob_path = parse_s3_url(input_s3_url)
    
    print(f"Reading and processing data from {s3_glob_path} using DuckDB (Forced IAM auth)...")

    # 1. Obtener Credenciales Temporales del IAM Role (CRUCIAL para evitar 403)
    try:
        session = boto3.Session()
        # Obtiene las credenciales temporales del rol IAM de la instancia EC2
        creds = session.get_credentials().get_frozen_credentials()
        if not creds.access_key or not creds.secret_key:
             raise Exception("boto3 could not find temporary IAM credentials.")
    except Exception as e:
        print(f"Error fetching IAM credentials via boto3: {e}. Check IAM Role on EC2.")
        return {"rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}

    # 2. Conectar y Configurar DuckDB
    con = duckdb.connect(database=":memory:")
    
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception as e:
        print(f"Error loading httpfs extension: {e}")
        return {"rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}

    # Configuración Mínima
    # ¡REEMPLAZA 'us-east-1' con la región REAL de tu bucket!
    con.execute("SET s3_region='us-east-1';") 
    con.execute("SET s3_use_ssl=true;")
    
    # 3. Inyectar Credenciales Temporales (SOLUCIÓN AL 403)
    # Esto sobreescribe cualquier intento de DuckDB de buscar credenciales por sí mismo.
    con.execute(f"SET s3_access_key_id='{creds.access_key}';")
    con.execute(f"SET s3_secret_access_key='{creds.secret_key}';")
    
    # El token de sesión es necesario para credenciales temporales (roles IAM)
    if creds.token:
        con.execute(f"SET s3_session_token='{creds.token}';")

    # 4. Definir y Ejecutar la Consulta SQL
    sql_query = f"""
    WITH extracted_codes AS (
        SELECT 
            CAST(regexp_extract(
                message, 
                '{STATUS_CODE_PATTERN}', 
                1
            ) AS INTEGER) AS status_code
        FROM 
            read_json_auto('{s3_glob_path}', format='array')
    )
    , grouped_counts AS (
        SELECT
            CASE
                WHEN status_code BETWEEN 200 AND 299 THEN '2xx'
                WHEN status_code BETWEEN 400 AND 499 THEN '4xx'
                WHEN status_code BETWEEN 500 AND 599 THEN '5xx'
                ELSE 'other'
            END AS status_group,
            count(*) AS count
        FROM extracted_codes
        WHERE status_code IS NOT NULL
        GROUP BY 1
    )
    SELECT
        status_group,
        count
    FROM grouped_counts
    WHERE status_group IN ('2xx', '4xx', '5xx');
    """

    start_time = time.time()
    
    # 5. Ejecuta la consulta
    try:
        results = con.execute(sql_query).fetchall()
    except Exception as e:
        print(f"DuckDB Query Execution Error: {e}")
        return {"rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}

    end_time = time.time()
    elapsed_time = end_time - start_time
    
    # 6. Calcular las Tasas
    counts_dict = {row[0]: row[1] for row in results}
    total_events = sum(counts_dict.values())
    
    if total_events == 0:
        return {"rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}

    rate_2xx = counts_dict.get('2xx', 0) / total_events
    rate_4xx = counts_dict.get('4xx', 0) / total_events
    rate_5xx = counts_dict.get('5xx', 0) / total_events

    final_result = {
        "rate_2xx": round(rate_2xx, 4),
        "rate_4xx": round(rate_4xx, 4),
        "rate_5xx": round(rate_5xx, 4),
    }

    print("\n--- RESULTS ---")
    print(f"Rates: {final_result}")
    print(f"DuckDB Execution Time (Wall Clock): {elapsed_time:.4f} seconds")
    print("---------------")
    
    return final_result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="Path to s3 bucket with all data (e.g., s3://my-bucket/5gb/)")
    args = parser.parse_args()
    
    main(args.input)