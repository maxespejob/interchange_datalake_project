import asyncio
import io
import json
import logging
import os
import random
import urllib.request
import urllib.error
from datetime import datetime, timedelta

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup

# =============================================================================
# LOGGING
# =============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =============================================================================
# CONFIGURATION
# =============================================================================

S3_BUCKET = os.environ.get("S3_BUCKET", "itl-0004-itx-dev-intchg-02-s3-reference")
S3_PREFIX = os.environ.get("S3_PREFIX", "exchange-rates/brand=Visa")
FUNCTION_NAME = os.environ.get(
    "FUNCTION_NAME", "itl-0004-itx-dev-intchg-02-lmbd-vi-exchange-rates"
)
BEGIN_DATE = os.environ.get("BEGIN_DATE", datetime.now().strftime("%Y-%m-%d"))
END_DATE = os.environ.get("END_DATE", datetime.now().strftime("%Y-%m-%d"))

MIN_WAIT = 0.6
MAX_WAIT = 1
NUM_CHUNKS = 10
CONCURRENCY = 6
TIMEOUT_MS = 15000
COOLDOWN_EVERY = 1200  # pause after every N completed pairs
COOLDOWN_DURATION = 10  # seconds to pause

DATE_FORMAT_INPUT = "%Y-%m-%d"
DATE_FORMAT_OUTPUT = "%m/%d/%Y"
DATE_FORMAT_FILE = "%Y%m%d"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.91 Safari/537.36",
]

REQUEST_HEADERS = {
    "Referer": "https://www.visa.com.pe/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
}

VISA_CALCULATOR_URL = (
    "https://www.visa.com.pe/soporte/consumidores/viajes/exchange-rate-calculator.html"
)
VISA_RATES_URL = (
    "https://www.visa.com.pe/cmsapi/fx/rates?"
    "amount=1&fee=0"
    "&utcConvertedDate={date}"
    "&exchangedate={date}"
    "&fromCurr={to_currency}"
    "&toCurr={from_currency}"
)

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
    Remainder pairs are spread one-by-one across the first chunks
    instead of being dumped entirely into the last one.
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
            f"[split_into_chunks] {len(items)} items split into {num_chunks} chunks | "
            f"min={min(sizes)} | max={max(sizes)} | sizes={sizes}"
        )
        return chunks
    except Exception as e:
        logger.error(f"[split_into_chunks] Failed to split list: {e}")
        raise


def fetch_currency_list() -> list[list[str]]:
    """
    Fetches the supported currency list from the VISA calculator page
    and returns all valid currency pairs.
    Uses a browser-like User-Agent to avoid Cloudflare 520 blocks.
    """
    logger.info("[fetch_currency_list] Fetching supported currencies from VISA...")

    try:
        req = urllib.request.Request(
            VISA_CALCULATOR_URL,
            headers={"User-Agent": random.choice(USER_AGENTS)},
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            html = response.read().decode("utf-8")

        body = BeautifulSoup(html, "html.parser")
        calculator = body.find("dm-calculator")
        
        # 1. Validate the tag exists
        if calculator is None:
            raise ValueError("Missing 'dm-calculator' tag on the VISA page")
            
        # 2. Get the attribute and validate it is a string
        content_attr = calculator.get("content")
        if not isinstance(content_attr, str):
            raise ValueError("The 'content' attribute is missing or not a valid string")

        # 3. Now the linter knows 100% that content_attr is a str
        data = json.loads(content_attr)

        currencies = [c["key"] for c in data["currencyList"] if c["key"] != "None"]
        currencies.append("SLE")

        pairs = [[src, dst] for src in currencies for dst in currencies if src != dst]
        logger.info(
            f"[fetch_currency_list] {len(currencies)} currencies -> {len(pairs)} pairs"
        )
        return pairs

    except urllib.error.URLError as e:
        logger.error(f"[fetch_currency_list] Failed to reach VISA calculator page: {e}")
        raise
    except (AttributeError, KeyError, json.JSONDecodeError) as e:
        logger.error(f"[fetch_currency_list] Failed to parse currency list: {e}")
        raise
    except Exception as e:
        logger.error(f"[fetch_currency_list] Unexpected error: {e}")
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
                f"[delete_existing_parquets] No existing files found at s3://{S3_BUCKET}/{prefix}"
            )
            return 0

        delete_payload = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        s3.delete_objects(Bucket=S3_BUCKET, Delete=delete_payload)

        logger.info(
            f"[delete_existing_parquets] Deleted {len(objects)} file(s) from s3://{S3_BUCKET}/{prefix}"
        )
        return len(objects)

    except Exception as e:
        logger.error(
            f"[delete_existing_parquets] Failed to delete files at {prefix}: {e}"
        )
        raise


def build_s3_key(date_str: str, chunk_id: int) -> str:
    """
    Builds the S3 key for a parquet chunk inside a temporary folder.
    """
    file_date = datetime.strptime(date_str, DATE_FORMAT_INPUT).strftime(
        DATE_FORMAT_FILE
    )
    # Cambio principal: Agregamos "/temp_chunks/" a la ruta
    return f"{S3_PREFIX}/exchange_date={date_str}/temp_chunks/{file_date}_chunk_{chunk_id}.parquet"


def save_chunk_to_s3(records: list[dict], date_str: str, chunk_id: int) -> str:
    """
    Serializes a list of exchange rate records into a parquet file and uploads it to S3.
    Skips records with missing fx_rate values.
    Returns the S3 key of the saved file.
    """
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    s3_key = build_s3_key(date_str, chunk_id)
    valid_records = [r for r in records if r["fx_rate"] is not None]
    skipped_count = len(records) - len(valid_records)
    
    if not valid_records:
        logger.warning(
            f"[save_chunk_to_s3] chunk_id={chunk_id} | No valid records to save, skipping upload"
        )
        return s3_key
    
    num_records = len(valid_records)
    
    try:
        table = pa.table(
            {
                "from_currency": [r["from_currency"] for r in valid_records],
                "to_currency": [r["to_currency"] for r in valid_records],
                "fx_rate": [r["fx_rate"] for r in valid_records],
                "creation_timestamp": [current_timestamp]*num_records
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
    chunk_index is 0-based — the next worker processes chunks[chunk_index].
    Does nothing if chunk_index is out of range (chain is complete).
    """
    if chunk_index >= len(chunks):
        logger.info("[invoke_next_worker] All chunks processed. Chain complete.")
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
        response = boto3.client("lambda").invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )
        logger.info(
            f"[invoke_next_worker] Invoked worker for chunk_index={chunk_index} "
            f"(chunk_id={chunk_index + 1}/{len(chunks)}) | "
            f"pairs={len(chunks[chunk_index])} | status={response['StatusCode']}"
        )
    except Exception as e:
        logger.error(
            f"[invoke_next_worker] Failed to invoke worker at chunk_index={chunk_index}: {e}"
        )
        raise


# =============================================================================
# SCRAPER — Playwright async worker
# =============================================================================


async def scrape_chunk(date: str, pairs: list, chunk_id: int) -> list[dict]:
    """
    Scrapes exchange rates using Playwright.
    CONCURRENCY pages share a single browser context (required for --single-process).
    """
    from playwright.async_api import async_playwright

    results = []
    queue = asyncio.Queue()
    total = len(pairs)
    completed_count = 0
    cooldown_lock = asyncio.Lock()

    for index, pair in enumerate(pairs, 1):
        await queue.put((index, tuple(pair)))

    logger.info(
        f"[scrape_chunk] chunk_id={chunk_id} | pairs={total} | concurrency={CONCURRENCY}"
    )

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
                "--no-zygote",
            ],
        )

        user_agent = random.choice(USER_AGENTS)
        logger.info(f"[scrape_chunk] chunk_id={chunk_id} | user_agent={user_agent}")

        context = await browser.new_context(
            user_agent=user_agent,
            extra_http_headers=REQUEST_HEADERS,
        )

        async def browser_worker(worker_id: int):
            page = await context.new_page()

            while True:
                try:
                    index, pair = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                from_currency, to_currency = pair

                url = VISA_RATES_URL.format(
                    date=date,
                    from_currency=from_currency,
                    to_currency=to_currency,
                )
                record = {
                    "from_currency": from_currency,
                    "to_currency": to_currency,
                    "fx_rate": None,
                    "creation_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                try:
                    response = await page.goto(
                        url, wait_until="load", timeout=TIMEOUT_MS
                    )
                    status = response.status if response else None

                    if status == 429:
                        logger.warning(
                            f"[scrape_chunk] chunk={chunk_id} | worker={worker_id} | "
                            f"[{index}/{total}] 429 rate limited {from_currency}->{to_currency}"
                        )
                    else:
                        raw = await page.inner_text("pre", timeout=TIMEOUT_MS)
                        data = json.loads(raw)
                        fx_rate = float(data["originalValues"]["fxRateVisa"])
                        record["fx_rate"] = fx_rate
                        logger.info(
                            f"[scrape_chunk] chunk={chunk_id} | worker={worker_id} | "
                            f"[{index}/{total}] OK {from_currency}->{to_currency} | fx={fx_rate}"
                        )

                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(
                        f"[scrape_chunk] chunk={chunk_id} | worker={worker_id} | "
                        f"[{index}/{total}] Parse error {from_currency}->{to_currency} | {e}"
                    )
                except Exception as e:
                    logger.error(
                        f"[scrape_chunk] chunk={chunk_id} | worker={worker_id} | "
                        f"[{index}/{total}] Error {from_currency}->{to_currency} | {type(e).__name__}: {e}"
                    )
                finally:
                    results.append(record)
                    await asyncio.sleep(random.uniform(MIN_WAIT, MAX_WAIT))

                    # Cooldown every COOLDOWN_EVERY pairs to avoid sustained 429s
                    nonlocal completed_count
                    completed_count += 1
                    if completed_count % COOLDOWN_EVERY == 0:
                        async with cooldown_lock:
                            logger.info(
                                f"[scrape_chunk] chunk={chunk_id} | worker={worker_id} | "
                                f"Cooldown at {completed_count}/{total} pairs | "
                                f"pausing {COOLDOWN_DURATION}s..."
                            )
                            await asyncio.sleep(COOLDOWN_DURATION)

            await page.close()

        workers = [asyncio.create_task(browser_worker(i)) for i in range(CONCURRENCY)]
        await asyncio.gather(*workers)
        await browser.close()

    written_count = len([r for r in results if r["fx_rate"] is not None])
    skipped_count = len(results) - written_count
    logger.info(
        f"[scrape_chunk] chunk_id={chunk_id} | SUMMARY | "
        f"total={total} | scraped={written_count} | failed={skipped_count} | "
        f"success_rate={round(written_count / total * 100, 2) if total else 0}%"
    )
    return results


# =============================================================================
# ORCHESTRATOR
# =============================================================================


def run_orchestrator(begin_date: str, end_date: str) -> dict:
    """
    Orchestrator role:
    - Fetches the full currency pair list from VISA
    - Deletes existing parquet files for each date before reprocessing
    - Splits pairs into chunks and kicks off the chain by invoking worker 1
    - Each worker invokes the next one upon completion (chain pattern)
    """
    logger.info(f"[ORCHESTRATOR] Starting | begin={begin_date} | end={end_date}")

    try:
        dates = generate_date_range(begin_date, end_date)
        pairs = fetch_currency_list()
        chunks = split_into_chunks(pairs, NUM_CHUNKS)

        for date in dates:
            date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(
                DATE_FORMAT_INPUT
            )

            delete_existing_parquets(date_str)

            # Kick off the chain — worker 0 will invoke worker 1, and so on
            logger.info(
                f"[ORCHESTRATOR] Starting chain for {date} | {NUM_CHUNKS} chunks | {len(pairs)} pairs..."
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
    - Processes chunks[chunk_index]
    - Saves results as a parquet file in S3
    - Invokes the next worker in the chain (chunk_index + 1) before finishing
    """
    chunk_id = chunk_index + 1
    pairs = chunks[chunk_index]

    logger.info(
        f"[WORKER {chunk_id}/{len(chunks)}] Starting | "
        f"date={date} | pairs={len(pairs)} | chunk_index={chunk_index}"
    )

    try:
        date_str = datetime.strptime(date, DATE_FORMAT_OUTPUT).strftime(
            DATE_FORMAT_INPUT
        )
        records = asyncio.run(scrape_chunk(date, pairs, chunk_id))
        s3_key = save_chunk_to_s3(records, date_str, chunk_id)

        written_count = len([r for r in records if r["fx_rate"] is not None])
        skipped_count = len(records) - written_count

        logger.info(
            f"[WORKER {chunk_id}/{len(chunks)}] Done | date={date_str} | "
            f"written={written_count} | skipped={skipped_count} | file={s3_key}"
        )

        # Invoke next worker in chain before returning
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
    final_s3_key = f"{S3_PREFIX}/exchange_date={date_str}/Visa_{file_date}.parquet"
    
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
        
        raise ValueError(f"Unknown mode: '{mode}'. Use 'orchestrator' or 'worker'.")

    except KeyError as e:
        logger.error(f"[lambda_handler] Missing required field in event: {e}")
        raise
    except ValueError as e:
        logger.error(f"[lambda_handler] Invalid event value: {e}")
        raise
    except Exception as e:
        logger.error(f"[lambda_handler] Fatal error: {e}")
        raise
