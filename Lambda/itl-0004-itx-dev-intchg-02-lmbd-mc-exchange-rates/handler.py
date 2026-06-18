import io
import os
import json
import time
import random
import logging
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from curl_cffi import requests

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIGURATION
# =============================================================================

S3_BUCKET = os.environ.get("S3_BUCKET", "itl-0004-itx-dev-intchg-02-s3-reference")
S3_PREFIX = os.environ.get("S3_PREFIX", "exchange-rates/brand=Mastercard")
FUNCTION_NAME = os.environ.get(
    "FUNCTION_NAME", "itl-0004-itx-dev-intchg-02-lmbd-mc-exchange-rates"
)

CURRENT_UTC_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
BEGIN_DATE = os.environ.get("BEGIN_DATE", CURRENT_UTC_DATE)
END_DATE = os.environ.get("END_DATE", CURRENT_UTC_DATE)

NUM_CHUNKS = 10
MAX_WORKERS = 9
REQUEST_TIMEOUT = 15
PAUSE_MIN = 1.0
PAUSE_MAX = 1.3
PROXY_BAN_AFTER = 1  # consecutive failures before banning a proxy

REQUEST_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
    "Referer": "https://www.mastercard.us/en-us/personal/get-support/convert-currency.html",
    "Origin": "https://www.mastercard.us",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

MASTERCARD_RATES_URL = (
    "https://www.mastercard.com/marketingservices/public/mccom-services/"
    "currency-conversions/conversion-rates"
)

DATE_FORMAT_INPUT = "%Y-%m-%d"
DATE_FORMAT_OUTPUT = "%m/%d/%Y"
DATE_FORMAT_FILE = "%Y%m%d"

# =============================================================================
# PROXY MANAGER
# Centralizes proxy state so failures from ANY thread count toward the ban limit.
# =============================================================================


class ProxyManager:
    """
    Thread-safe proxy pool manager.
    Tracks failure counts globally across all threads so that a proxy
    getting errors from different threads still accumulates toward the ban threshold.
    """

    def __init__(self, proxies: list[dict]):
        self._lock = threading.Lock()
        self._pool = [
            {"proxy": p["proxy"], "status": "active", "fails": 0}
            for p in proxies
            if p.get("status") == "active"
        ]

    def get_active(self) -> list[dict]:
        with self._lock:
            return [p for p in self._pool if p["status"] == "active"]

    def pick(self, index: int) -> dict | None:
        """Round-robin selection over active proxies only."""
        with self._lock:
            active = [p for p in self._pool if p["status"] == "active"]
            if not active:
                return None
            return active[index % len(active)]

    def report_failure(self, proxy: dict) -> None:
        """
        Increments the failure counter for a proxy.
        Bans it globally once PROXY_BAN_AFTER consecutive failures are reached.
        """
        with self._lock:
            proxy["fails"] += 1
            if proxy["fails"] >= PROXY_BAN_AFTER and proxy["status"] == "active":
                proxy["status"] = "inactive"
                safe_url = self._mask_proxy_url(proxy["proxy"])
                logger.warning(
                    f"[ProxyManager] Proxy banned after {proxy['fails']} failures: {safe_url} | "
                    f"Active proxies remaining: {sum(1 for p in self._pool if p['status'] == 'active')}"
                )

    def report_success(self, proxy: dict) -> None:
        """Resets the failure counter on a successful request."""
        with self._lock:
            proxy["fails"] = 0

    @staticmethod
    def _mask_proxy_url(url: str) -> str:
        """Masks credentials in proxy URL for safe logging."""
        try:
            if "@" in url:
                protocol = url.split("://")[0]
                host_part = url.split("@")[-1]
                return f"{protocol}://***:***@{host_part}"
        except Exception:
            pass
        return "***"

    @property
    def total(self) -> int:
        return len(self._pool)

    @property
    def active_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._pool if p["status"] == "active")


# =============================================================================
# PROXY LOADING & VALIDATION
# =============================================================================


def load_proxy_settings() -> list[dict]:
    """Loads active proxy list from the deployment package proxy_settings.json."""
    try:
        with open("resources/proxy_settings.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        raw_proxies = data.get("proxy_settings", {}).get(
            "proxy_list_mastercard", []
        ) or data.get("proxy_settings", {}).get("proxy_list", [])
        active = [p for p in raw_proxies if p.get("status") == "active"]
        logger.info(f"[load_proxy_settings] {len(active)} active proxies loaded")
        return active

    except Exception as e:
        logger.error(f"[load_proxy_settings] Failed to load proxy file: {e}")
        return []


def validate_proxies(proxies: list[dict]) -> list[dict]:
    """
    Validates ALL proxies concurrently against Mastercard before starting the worker.
    Discards any proxy that fails, times out, or returns non-200, logging the exact error.
    """
    if not proxies:
        return []

    logger.info(
        f"[validate_proxies] Launching concurrent validation for ALL {len(proxies)} proxies..."
    )
    valid_proxies = []

    test_params = {
        "exchange_date": datetime.now(timezone.utc).strftime(DATE_FORMAT_INPUT),
        "transaction_currency": "USD",
        "cardholder_billing_currency": "EUR",
        "bank_fee": "0",
        "transaction_amount": "1",
    }

    def check_proxy(proxy_dict: dict) -> dict | None:
        proxy_url = proxy_dict["proxy"]
        # Enmascaramos la credencial para un log seguro
        safe_url = ProxyManager._mask_proxy_url(proxy_url)
        try:
            with requests.Session(impersonate="chrome120") as s:
                s.proxies = {"http": proxy_url, "https": proxy_url}
                resp = s.get(
                    MASTERCARD_RATES_URL,
                    params=test_params,
                    headers=REQUEST_HEADERS,
                    timeout=5,
                )

                if resp.status_code == 200 and resp.text.strip():
                    return proxy_dict
                else:
                    # Captura casos donde el proxy responde pero con códigos de error (ej. 403, 502)
                    logger.warning(
                        f"[validate_proxies] Proxy {safe_url} rejected connection | HTTP Status: {resp.status_code}"
                    )
        except Exception as e:
            # Captura caídas de red a nivel de socket (ej. Timeouts, Connection resets, Aborted)
            logger.warning(
                f"[validate_proxies] Proxy {safe_url} failed validation test | Details: {e}"
            )
        return None

    # Test de ping masivo e hilos concurrentes para aislar nodos funcionales
    with ThreadPoolExecutor(max_workers=15) as executor:
        results = executor.map(check_proxy, proxies)
        for res in results:
            if res:
                valid_proxies.append(res)

    discarded = len(proxies) - len(valid_proxies)
    logger.info(
        f"[validate_proxies] Pre-flight complete | "
        f"Passed: {len(valid_proxies)} | Discarded: {discarded}"
    )

    return valid_proxies


# =============================================================================
# CURRENCY LIST
# =============================================================================


def fetch_currency_list() -> list[list[str]] | str:
    """Loads the currency cross matrix from the local currencies.json file."""
    try:
        with open("resources/currencies.json", "r", encoding="utf-8") as f:
            data = json.load(f)

        currencies = [c["alphaCd"] for c in data["currencies"]]
        pairs = [[src, dst] for src in currencies for dst in currencies if src != dst]
        logger.info(
            f"[fetch_currency_list] {len(currencies)} currencies -> {len(pairs)} pairs"
        )
        return pairs

    except Exception as e:
        logger.error(f"[fetch_currency_list] Failed to load currencies: {e}")
        return "error"


# =============================================================================
# HELPERS
# =============================================================================


def generate_date_range(begin_date_str: str, end_date_str: str) -> list[str]:
    """Returns a list of dates in MM/DD/YYYY format between two YYYY-MM-DD dates."""
    try:
        begin = datetime.strptime(begin_date_str, DATE_FORMAT_INPUT)
        end = datetime.strptime(end_date_str, DATE_FORMAT_INPUT)
        dates = [
            (begin + timedelta(days=i)).strftime(DATE_FORMAT_OUTPUT)
            for i in range((end - begin).days + 1)
        ]
        logger.info(
            f"[generate_date_range] {len(dates)} date(s) generated: {dates[0]} -> {dates[-1]}"
        )
        return dates
    except ValueError as e:
        logger.error(f"[generate_date_range] Invalid date format: {e}")
        raise


def split_into_chunks(items: list, num_chunks: int) -> list[list]:
    """
    Splits a list into N evenly distributed chunks.
    Remainder items are spread one-by-one across the first chunks.
    """
    try:
        chunk_size, remainder = divmod(len(items), num_chunks)
        chunks = []
        start = 0

        for i in range(num_chunks):
            end = start + chunk_size + (1 if i < remainder else 0)
            chunks.append(items[start:end])
            start = end

        sizes = [len(c) for c in chunks]
        logger.info(
            f"[split_into_chunks] {len(items)} items -> {num_chunks} chunks | "
            f"min={min(sizes)} | max={max(sizes)}"
        )
        return chunks
    except Exception as e:
        logger.error(f"[split_into_chunks] Failed to split list: {e}")
        raise


def delete_existing_parquets(date_str: str) -> int:
    """
    Deletes all parquet files under the S3 prefix for a given date.
    Used to clean up stale files before reprocessing.
    Returns the number of deleted objects.
    """
    prefix = f"{S3_PREFIX}/exchange_date={date_str}/"
    try:
        s3 = boto3.client("s3")
        objects = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            objects.extend(page.get("Contents", []))

        if not objects:
            logger.info(
                f"[delete_existing_parquets] No existing files at s3://{S3_BUCKET}/{prefix}"
            )
            return 0

        s3.delete_objects(
            Bucket=S3_BUCKET, Delete={"Objects": [{"Key": o["Key"]} for o in objects]}
        )
        logger.info(
            f"[delete_existing_parquets] Deleted {len(objects)} file(s) from s3://{S3_BUCKET}/{prefix}"
        )
        return len(objects)

    except Exception as e:
        logger.error(
            f"[delete_existing_parquets] Failed to delete files at {prefix}: {e}"
        )
        raise


def save_chunk_to_s3(records: list[dict], date_str: str, chunk_id: int) -> str:
    """
    Serializes exchange rate records into a parquet file and uploads it to S3.
    Skips records with missing fx_rate values.
    Returns the S3 key of the saved file.
    """
    file_date = datetime.strptime(date_str, DATE_FORMAT_INPUT).strftime(
        DATE_FORMAT_FILE
    )
    s3_key = (
        f"{S3_PREFIX}/exchange_date={date_str}/temp_chunks/{file_date}_chunk_{chunk_id}.parquet"
    )
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    valid_records = [r for r in records if r["fx_rate"] != ""]
    skipped_count = len(records) - len(valid_records)
    num_records = len(valid_records)
    
    if not valid_records:
        logger.warning(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | No valid records, skipping upload"
        )
        return s3_key

    try:
        table = pa.table(
            {
                "from_currency": [r["from_currency"] for r in valid_records],
                "to_currency": [r["to_currency"] for r in valid_records],
                "fx_rate": [r["fx_rate"] for r in valid_records],
                "creation_timestamp": [current_timestamp] * num_records,
            }
        )

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        boto3.client("s3").put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | "
            f"written={len(valid_records)} | skipped={skipped_count} | "
            f"s3://{S3_BUCKET}/{s3_key}"
        )
        return s3_key

    except Exception as e:
        logger.error(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | Failed to upload parquet: {e}"
        )
        raise


def invoke_next_worker(date: str, chunks: list, chunk_index: int) -> None:
    """
    Invokes the next worker in the chain asynchronously.
    Does nothing if chunk_index is out of range (chain is complete).
    """
    if chunk_index >= len(chunks):
        logger.info("[invoke_next_worker] All chunks processed. Invoking consolidator...")
        try:
            boto3.client("lambda").invoke(
                FunctionName=FUNCTION_NAME,
                InvocationType="Event",
                Payload=json.dumps({
                    "mode": "consolidator",
                    "date": date
                }),
            )
        except Exception as e:
            logger.error(f"[invoke_next_worker] Failed to invoke consolidator: {e}")
            raise
        return

    try:
        payload = {
            "mode": "worker",
            "date": date,
            "chunks": chunks,
            "chunk_index": chunk_index,
        }
        boto3.client("lambda").invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        logger.info(
            f"[invoke_next_worker] Worker invoked | "
            f"chunk_index={chunk_index} | chunk_id={chunk_index + 1}/{len(chunks)} | "
            f"pairs={len(chunks[chunk_index])}"
        )
    except Exception as e:
        logger.error(
            f"[invoke_next_worker] Failed to invoke worker at chunk_index={chunk_index}: {e}"
        )
        raise


# =============================================================================
# STEP 2: Process a sub-chunk of pairs inside a single thread
# =============================================================================


def process_sub_chunk(
    date: str,
    sub_chunk: list[list[str]],
    worker_id: int,
    proxy_manager: ProxyManager,
) -> list[dict]:
    """
    Processes a batch of currency pairs inside a single thread.
    Uses ProxyManager for thread-safe proxy rotation and global failure tracking.
    """
    date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
    thread_records = []
    sub_chunk_total = len(sub_chunk)

    with requests.Session(impersonate="chrome120") as session:
        for idx, pair in enumerate(sub_chunk):
            from_curr, to_curr = pair

            params = {
                "exchange_date": date_str,
                "transaction_currency": from_curr,
                "cardholder_billing_currency": to_curr,
                "bank_fee": "0",
                "transaction_amount": "1",
            }

            empty_record = {
                "from_currency": from_curr,
                "to_currency": to_curr,
                "fx_rate": "",
                "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            proxy = proxy_manager.pick(idx)
            if proxy:
                session.proxies = {"http": proxy["proxy"], "https": proxy["proxy"]}
            else:
                session.proxies = {}
                logger.warning(
                    f"[Thread {worker_id}] No active proxies available — running without proxy"
                )

            try:
                response = session.get(
                    MASTERCARD_RATES_URL,
                    params=params,
                    headers=REQUEST_HEADERS,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code in (403, 429):
                    logger.warning(
                        f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                        f"HTTP {response.status_code} (blocked) | {from_curr}->{to_curr} | {date_str}"
                    )
                    if proxy:
                        proxy_manager.report_failure(proxy)
                    thread_records.append(empty_record)
                    time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
                    continue

                if response.status_code != 200 or not response.text.strip():
                    logger.warning(
                        f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                        f"HTTP {response.status_code} empty/unexpected | {from_curr}->{to_curr}"
                    )
                    thread_records.append(empty_record)
                    time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
                    continue

                fx_rate = float(
                    str(response.json()["data"]["conversionRate"]).replace(",", "")
                )

                if proxy:
                    proxy_manager.report_success(proxy)

                thread_records.append(
                    {
                        "from_currency": from_curr,
                        "to_currency": to_curr,
                        "fx_rate": fx_rate,
                        "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                logger.info(
                    f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                    f"OK {from_curr}->{to_curr} | {date_str} | fx={fx_rate}"
                )

            except Exception as e:
                error_msg = str(e).lower()
                logger.error(
                    f"[Thread {worker_id}][{idx + 1}/{sub_chunk_total}] "
                    f"Error | {from_curr}->{to_curr} | {date_str} | {type(e).__name__}: {e}"
                )

                # Report proxy failure on network-level errors
                if proxy and any(
                    keyword in error_msg
                    for keyword in ("timeout", "reset", "aborted", "connect")
                ):
                    proxy_manager.report_failure(proxy)

                thread_records.append(empty_record)

            time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

    return thread_records


# =============================================================================
# ORCHESTRATOR
# =============================================================================


def run_orchestrator(begin_date: str, end_date: str) -> dict:
    """
    Orchestrator role:
    - Loads and validates proxies
    - Fetches the full currency pair list
    - Deletes existing parquet files for each date before reprocessing
    - Kicks off the worker chain (chunk_index=0)
    """
    logger.info(f"[ORCHESTRATOR] Starting | begin={begin_date} | end={end_date}")

    try:
        dates = generate_date_range(begin_date, end_date)
        pairs = fetch_currency_list()

        if isinstance(pairs, str):
            raise RuntimeError("Failed to retrieve currency list")

        chunks = split_into_chunks(pairs, NUM_CHUNKS)

        for date in dates:
            date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(
                DATE_FORMAT_INPUT
            )
            delete_existing_parquets(date_str)

            logger.info(
                f"[ORCHESTRATOR] Starting chain for {date} | {NUM_CHUNKS} chunks | {len(pairs)} pairs"
            )
            invoke_next_worker(date, chunks, chunk_index=0)

        logger.info(f"[ORCHESTRATOR] Done | {len(dates)} chain(s) started")
        return {
            "statusCode": 200,
            "mode": "orchestrator",
            "chains": len(dates),
            "total_pairs": len(pairs),
            "dates": dates,
        }

    except Exception as e:
        logger.error(f"[ORCHESTRATOR] Fatal error: {e}")
        raise


# =============================================================================
# WORKER
# =============================================================================


def run_worker(date: str, chunks: list, chunk_index: int) -> dict:
    """
    Worker role:
    - Loads and initializes the ProxyManager for this execution
    - Splits its chunk into sub-chunks, one per thread
    - Processes all pairs and saves results to S3
    - Invokes the next worker in the chain
    """
    chunk_id = chunk_index + 1
    pairs = chunks[chunk_index]
    total_pairs = len(pairs)

    logger.info(
        f"[WORKER {chunk_id}/{len(chunks)}] Starting | "
        f"date={date} | pairs={total_pairs}"
    )

    try:
        date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(
            DATE_FORMAT_INPUT
        )
        proxy_manager = ProxyManager(validate_proxies(load_proxy_settings()))

        logger.info(
            f"[WORKER {chunk_id}/{len(chunks)}] Proxy pool initialized | "
            f"total={proxy_manager.total} | active={proxy_manager.active_count}"
        )

        sub_chunks = split_into_chunks(pairs, MAX_WORKERS)
        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(
                    process_sub_chunk, date, sub_chunk, i + 1, proxy_manager
                )
                for i, sub_chunk in enumerate(sub_chunks)
                if sub_chunk
            ]
            for future in as_completed(futures):
                try:
                    results.extend(future.result())
                except Exception as e:
                    logger.error(f"[WORKER {chunk_id}/{len(chunks)}] Thread error: {e}")

        s3_key = save_chunk_to_s3(results, date_str, chunk_id)
        written_count = len([r for r in results if r["fx_rate"] != ""])
        skipped_count = len(results) - written_count

        logger.info(
            f"[WORKER {chunk_id}/{len(chunks)}] Done | date={date_str} | "
            f"written={written_count} | skipped={skipped_count} | "
            f"active_proxies={proxy_manager.active_count} | file={s3_key}"
        )

        invoke_next_worker(date, chunks, chunk_index=chunk_index + 1)

        return {
            "statusCode": 200,
            "mode": "worker",
            "chunk_id": chunk_id,
            "records_ok": written_count,
            "records_skip": skipped_count,
            "s3_key": s3_key,
        }

    except Exception as e:
        logger.error(
            f"[WORKER {chunk_id}/{len(chunks)}] Fatal error | date={date} | {e}"
        )
        raise

# =============================================================================
# CONSOLIDATOR
# =============================================================================

def run_consolidator(date: str) -> dict:
    """
    Consolidator role:
    - Reads all parquet chunks from the temp_chunks directory for a given date.
    - Concatenates them into a single PyArrow table.
    - Saves the unified parquet file to the final S3 path.
    - Deletes the temporary chunks.
    """
    date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_INPUT)
    file_date = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(DATE_FORMAT_FILE)
    
    temp_prefix = f"{S3_PREFIX}/exchange_date={date_str}/temp_chunks/"
    final_s3_key = f"{S3_PREFIX}/exchange_date={date_str}/Mastercard_{file_date}.parquet"
    
    logger.info(f"[CONSOLIDATOR] Starting consolidation for {date_str}...")
    
    s3 = boto3.client("s3")
    tables = []
    objects_to_delete = []

    try:
        # 1. Listar y leer todos los chunks temporales
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=temp_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                objects_to_delete.append({"Key": key})
                
                # Descargar a memoria
                response = s3.get_object(Bucket=S3_BUCKET, Key=key)
                buffer = io.BytesIO(response['Body'].read())
                
                # Leer parquet y agregarlo a la lista
                table = pq.read_table(buffer)
                tables.append(table)

        if not tables:
            logger.warning(f"[CONSOLIDATOR] No temporary chunks found at {temp_prefix}. Skipping.")
            return {"statusCode": 200, "message": "No data to consolidate"}

        # 2. Unir todas las tablas en una sola
        consolidated_table = pa.concat_tables(tables)
        total_records = consolidated_table.num_rows

        # 3. Guardar el archivo consolidado final
        out_buffer = io.BytesIO()
        pq.write_table(consolidated_table, out_buffer)
        out_buffer.seek(0)
        
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=final_s3_key,
            Body=out_buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        logger.info(f"[CONSOLIDATOR] Successfully saved {total_records} records to s3://{S3_BUCKET}/{final_s3_key}")

        # 4. Limpiar los archivos temporales
        s3.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects_to_delete})
        logger.info(f"[CONSOLIDATOR] Cleaned up {len(objects_to_delete)} temporary chunk(s).")

        return {
            "statusCode": 200,
            "mode": "consolidator",
            "date": date_str,
            "total_records": total_records,
            "final_file": final_s3_key
        }

    except Exception as e:
        logger.error(f"[CONSOLIDATOR] Fatal error during consolidation: {e}")
        raise


# =============================================================================
# MAIN HANDLER
# =============================================================================


def lambda_handler(event: dict, context) -> dict:
    # logger.info(f"[lambda_handler] RAW EVENT: {json.dumps(event)}")
    mode = event.get("mode", "orchestrator")
    logger.info(f"[lambda_handler] Event received | mode={mode}")

    try:
        if mode == "orchestrator":
            begin_date = event.get("begin_date", BEGIN_DATE)
            end_date = event.get("end_date", END_DATE)
            return run_orchestrator(begin_date, end_date)

        if mode == "worker":
            date = event["date"]
            chunks = event["chunks"]
            chunk_index = event.get("chunk_index", 0)
            return run_worker(date, chunks, chunk_index)
     
        if mode == "consolidator":
            date = event["date"]
            return run_consolidator(date)

        raise ValueError(f"Unknown mode: '{mode}'. Use 'orchestrator', 'worker' or 'consolidator'.")

    except KeyError as e:
        logger.error(f"[lambda_handler] Missing required field in event: {e}")
        raise
    except ValueError as e:
        logger.error(f"[lambda_handler] Invalid event value: {e}")
        raise
    except Exception as e:
        logger.error(f"[lambda_handler] Fatal error: {e}")
        raise