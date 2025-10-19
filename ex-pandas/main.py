# /// script
# requires-python = ">=3.12"
# dependencies = ["pandas", "boto3"] 
# s3fs ya NO es necesario, pero lo mantenemos en dependencies si lo quieres tener
# ///

from typing import TypedDict
import re
from urllib.parse import urlparse
import pandas as pd
import boto3
# s3fs ya NO es necesario, pero lo mantenemos en el bloque de dependencias si es preferido

# Patrón para extraer el código de estado HTTP
STATUS_CODE_PATTERN = re.compile(r"HTTP Status Code: (\d{3})")

class Result(TypedDict):
    rate_2xx: float
    rate_4xx: float
    rate_5xx: float

def parse_s3_url(url: str) -> tuple[str, str]:
    """Extrae nombre del bucket y prefijo de un URL s3://."""
    parsed = urlparse(url)
    if parsed.scheme != 's3':
        raise ValueError("Input must be an s3:// URL")
    prefix = parsed.path.lstrip('/').rstrip('/')
    return parsed.netloc, prefix

# Se mantiene la lógica vectorial de Pandas
def get_status_code_group(code: pd.Series) -> pd.Series:
    """Grupa códigos de estado en 2xx, 4xx, o 5xx usando vectorización."""
    group = pd.Series('other', index=code.index)
    group[(code >= 200) & (code <= 299)] = '2xx'
    group[(code >= 400) & (code <= 499)] = '4xx'
    group[(code >= 500) & (code <= 599)] = '5xx'
    return group

def extract_codes_and_group(df: pd.DataFrame) -> pd.DataFrame:
    """Extrae el código de estado y lo agrupa vectorialmente."""
    df['status_code_str'] = df['message'].str.extract(STATUS_CODE_PATTERN, expand=False)
    df['status_code'] = pd.to_numeric(df['status_code_str'], errors='coerce')
    df['group'] = get_status_code_group(df['status_code'])
    return df[df['group'] != 'other']

def main(input_s3_url: str) -> Result:
    bucket_name, prefix = parse_s3_url(input_s3_url)
    s3 = boto3.client('s3')
    
    # Lista para almacenar los DataFrames de cada archivo
    all_dfs = [] 

    # Usamos paginator de Boto3 para listar todos los objetos
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    print(f"Reading data file by file from s3://{bucket_name}/{prefix}...")

    # Iteración manual de archivos (como en Python Puro)
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            # Solo procesamos archivos JSON, ignoramos directorios
            if key.endswith('/'): 
                continue

            try:
                # 1. Descargar el archivo usando Boto3 (Python Puro)
                response = s3.get_object(Bucket=bucket_name, Key=key)
                file_content = response['Body'].read().decode('utf-8')
                
                # 2. Cargar el JSON descargado en un DataFrame (Pandas)
                # Esta llamada es local y NO usa s3fs, por lo tanto, evita el error.
                df_file = pd.read_json(file_content, lines=False) 
                
                all_dfs.append(df_file)
                
            except Exception as e:
                # print(f"Error processing s3://{bucket_name}/{key}: {e}")
                continue
    
    # 3. Concatenar todos los DataFrames
    if not all_dfs:
        print("No files were successfully read.")
        return {"rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}
        
    df = pd.concat(all_dfs, ignore_index=True)
    
    # 4. Procesamiento y Conteo (vectorizado de Pandas)
    df_processed = extract_codes_and_group(df)
    
    # ... (El resto del cálculo de tasas) ...
    counts = df_processed['group'].value_counts()
    total_events = counts.sum()
    
    if total_events == 0:
        return {"rate_2xx": 0.0, "rate_4xx": 0.0, "rate_5xx": 0.0}

    rate_2xx = counts.get('2xx', 0) / total_events
    rate_4xx = counts.get('4xx', 0) / total_events
    rate_5xx = counts.get('5xx', 0) / total_events

    return {
        "rate_2xx": round(rate_2xx, 4),
        "rate_4xx": round(rate_4xx, 4),
        "rate_5xx": round(rate_5xx, 4),
    }


if __name__ == "__main__":
    import argparse
    import time
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help="Path to s3 bucket with all data (e.g., s3://my-bucket/5gb/)")
    args = parser.parse_args()
    # --- Timing Implementation ---
    start_time = time.time()
    result = main(args.input) 
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("\n--- RESULTS ---")
    print(f"Rates: {result}")
    # Aunque /usr/bin/time -v es el cronómetro oficial, incluimos este para depuración
    print(f"Pandas Execution Time (Wall Clock): {elapsed_time:.4f} seconds")
    print("---------------")