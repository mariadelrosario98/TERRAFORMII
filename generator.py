# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3",
# ]
# ///

import concurrent.futures
import datetime
import json
import pathlib
import random
import uuid
from typing import Any, Callable, Iterator, Dict, Tuple, List
import boto3

# File Count = Total GB * 1024 * 1024 / 1 KB  (ESTO ES EL CONTEO TOTAL DE EVENTOS)
TOTAL_EVENTS_PER_GB = 1024 * 1024 # ~1 millón de eventos por GB

# Archivos de 1MB significan 1024 archivos por GB (1024 archivos * 1MB/archivo = 1GB)
WORKLOADS: Dict[str, Tuple[int, int, int]] = {
    # Prefijo : (Size en GB, Eventos por Archivo, Total Archivos GRANDES)
    "5gb": (5, 1024, 5 * 1024),          # 5 GB = 5,120 archivos de 1MB
    "10gb": (10, 1024, 10 * 1024),       # 10 GB = 10,240 archivos de 1MB
    "15gb": (15, 1024, 15 * 1024),       # 15 GB = 15,360 archivos de 1MB
    "20gb": (20, 1024, 20 * 1024),       # 20 GB = 20,480 archivos de 1MB
    "25gb": (25, 1024, 25 * 1024),       # 25 GB = 25,600 archivos de 1MB
}


def main(num_files: int, events_per_file: int, writer: Callable[[dict[str, Any]], None]):
    event_generator = _generate_random_event()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for _ in range(num_files):
            # Recopilar el bloque de eventos (1 MB de data)
            event_batch = [next(event_generator) for _ in range(events_per_file)]
            
            # Enviar el bloque completo al escritor
            futures.append(executor.submit(writer, event_batch))
        
        # Esperar a que todos los archivos grandes se suban
        concurrent.futures.wait(futures)


class _LocalWriter:
    def __init__(self, dir: pathlib.Path):
        self._dir = dir
        self._dir.mkdir(parents=True, exist_ok=True)

    # El escritor ahora acepta una LISTA de eventos (un lote)
    def __call__(self, events: List[dict[str, Any]]) -> None:
        # Serializa cada evento a JSON y los une con saltos de línea (\n)
        payload = "\n".join(json.dumps(event) for event in events)
        name = f"{uuid.uuid4()}.jsonl" # Usamos .jsonl para archivos con JSON por línea
        print(f"writing {name}")
        with open(self._dir / name, "w") as file:
            file.write(payload)


class _S3Writer:
    def __init__(self, bucket: str, prefix: str):
        self._bucket = bucket
        self._prefix = prefix
        self._s3 = boto3.client("s3")
        self._s3.create_bucket(Bucket=self._bucket)

    def __call__(self, event: dict[str, Any]) -> None:
        payload = json.dumps(event)
        name = f"{uuid.uuid4()}.json"
        print(f"writing {name}")
        self._s3.put_object(Bucket=self._bucket, Key=f"{self._prefix}/{name}", Body=payload)


def _generate_random_event() -> Iterator[dict[str, Any]]:
    random.seed(a=42)
    statuses = [200, 201, 202, 203, 400, 401, 402, 403, 404, 500]
    services = ["training", "evaluation", "inference", "monitoring"]
    now = datetime.datetime.now()
    while True:
        status_code = random.choice(statuses)
        now += datetime.timedelta(seconds=random.randint(1, 300))
        yield {
            "service": random.choice(services),
            "timestamp": now.timestamp(),
            "message": f"HTTP Status Code: {status_code}",
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate benchmark data for S3 or local directory.")
    parser.add_argument("bucket_or_dir", help="S3 bucket name or local directory path.")
    parser.add_argument("--is-bucket", action="store_true", help="Set to write to S3. Omit to write to local directory.")
    parser.add_argument("--only-size", type=str, choices=list(WORKLOADS.keys()), help="Optional: Run only a single specific workload (e.g., 5gb).")
    
    args = parser.parse_args()

    if args.is_bucket:
        bucket_name = args.bucket_or_dir
        
        if args.only_size:
            # Run a single specified workload
            size, events_per_file, files_to_create = WORKLOADS[args.only_size]  # <-- FIX 1 (Unpack 3 values)
            print(f"\n--- Generating {size}GB dataset ({files_to_create} files de 1MB) into s3://{bucket_name}/{args.only_size}/ ---")
            writer = _S3Writer(bucket_name, args.only_size)
            main(files_to_create, events_per_file, writer)  # <-- FIX 2 (Pass correct arguments to main)
        else:
            # Run ALL workloads sequentially
            for prefix, (size, events_per_file, files_to_create) in WORKLOADS.items():  # <-- FIX 3 (Unpack 3 values)
                print(f"\n--- Generating {size}GB dataset ({files_to_create} files de 1MB) into s3://{bucket_name}/{prefix}/ ---")
                writer = _S3Writer(bucket_name, prefix)
                main(files_to_create, events_per_file, writer)  # <-- FIX 4 (Pass correct arguments to main)

    else:
        # Local generation (runs all defined workloads into subdirectories)
        base_dir = pathlib.Path(args.bucket_or_dir)
        
        if args.only_size:
            size, events_per_file, files_to_create = WORKLOADS[args.only_size]  # <-- FIX 5 (Unpack 3 values)
            prefix = args.only_size
            print(f"\n--- Generating {size}GB dataset ({files_to_create} files de 1MB) into local path {base_dir / prefix}/ ---")
            writer = _LocalWriter(base_dir / prefix)
            main(files_to_create, events_per_file, writer)  # <-- FIX 6 (Pass correct arguments to main)
        else:
            for prefix, (size, events_per_file, files_to_create) in WORKLOADS.items():  # <-- FIX 7 (Unpack 3 values)
                print(f"\n--- Generating {size}GB dataset ({files_to_create} files de 1MB) into local path {base_dir / prefix}/ ---")
                writer = _LocalWriter(base_dir / prefix)
                main(files_to_create, events_per_file, writer)  # <-- FIX 8 (Pass correct arguments to main)