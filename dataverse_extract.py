"""
Dataverse to SQL Database Extract Script

This script extracts data from Microsoft Dataverse and loads it into a SQL database.
It uses connection pooling, circuit breakers, rate limiting, and parallel processing.

Configuration via environment variables:
    - TENANT_ID: Azure AD tenant ID
    - CLIENT_ID: Azure AD application client ID  
    - CLIENT_SECRET: Azure AD application client secret
    - DATAVERSE_URL: Dataverse API URL (e.g., https://yourorg.crm.dynamics.com)
    - DB_CONNECTION_STRING: SQL database connection string
    - MAX_WORKERS: Number of parallel workers (default: 3)
    - PAGE_SIZE: Dataverse page size (default: 5000)
    - RATE_LIMIT_INTERVAL: Seconds between API calls (default: 0.3)
"""

import gc
import logging
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from functools import wraps
from threading import Lock

import pandas as pd
import psutil
import pyodbc
import requests
from requests.adapters import HTTPAdapter
from sqlalchemy import Column, MetaData, String, Table, inspect, text
from sqlalchemy.types import NVARCHAR, DateTime
from urllib3.util.retry import Retry

from Module.Connection_pool import Engine
from Module.Functions import timeout

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dataverse_extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Environment variables
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
DATAVERSE_URL = os.getenv("DATAVERSE_URL")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))
RATE_LIMIT_INTERVAL = float(os.getenv("RATE_LIMIT_INTERVAL", "0.3"))

# Timeout configuration
TOTAL_GROUP_TIMEOUT = int(os.getenv("GROUP_TIMEOUT_SECONDS", "10800"))  # 3 hours
ENTITY_TIMEOUT = int(os.getenv("ENTITY_TIMEOUT_SECONDS", "7200"))  # 2 hours

# Database schema for tables
DB_SCHEMA = os.getenv("DB_SCHEMA", "dbo")

# Script info
script_name = os.path.basename(__file__)
started = datetime.now()


class CircuitBreaker:
    """Circuit breaker pattern for handling API call failures per entity"""

    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = Lock()

    def call(self, func, *args, **kwargs):
        with self._lock:
            if self.state == "OPEN":
                if time.time() - self.last_failure_time > self.timeout:
                    self.state = "HALF_OPEN"
                    logger.info("Circuit breaker moved to HALF_OPEN state")
                else:
                    raise Exception("Circuit breaker is OPEN - too many failures")

        try:
            result = func(*args, **kwargs)
            with self._lock:
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failure_count = 0
                    logger.info("Circuit breaker moved to CLOSED state")
            return result
        except Exception as e:
            with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                if self.failure_count >= self.failure_threshold:
                    self.state = "OPEN"
                    logger.error(
                        f"Circuit breaker opened after {self.failure_count} failures"
                    )
            raise e


def rate_limit_decorator(min_interval=RATE_LIMIT_INTERVAL):
    """Rate limiting decorator to limit API call frequency"""
    last_call = {"time": 0}
    lock = Lock()

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                current_time = time.time()
                time_since_last = current_time - last_call["time"]
                if time_since_last < min_interval:
                    sleep_time = min_interval - time_since_last
                    time.sleep(sleep_time)
                last_call["time"] = time.time()
            return func(*args, **kwargs)

        return wrapper

    return decorator


def log_system_resources(entity_name):
    """Log system resources"""
    try:
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        total_threads = threading.active_count()

        if not hasattr(log_system_resources, "call_count"):
            log_system_resources.call_count = 0

        log_system_resources.call_count += 1

        if (
            log_system_resources.call_count == 1
            or log_system_resources.call_count % 10 == 0
        ):
            logger.info(
                f"{entity_name}: Memory: {memory_mb:.1f}MB, Active threads: {total_threads}"
            )

            if total_threads > 30:
                logger.warning(
                    f"High thread count: {total_threads}"
                )

    except Exception:
        pass


class TokenCache:
    """Cache for Azure AD access tokens"""
    
    def __init__(self):
        self._token = None
        self._expiry = None
        self._lock = Lock()

    def get_token(self, tenant_id, client_id, client_secret):
        with self._lock:
            current_time = datetime.now()
            if self._token is None or current_time >= self._expiry - timedelta(
                minutes=5
            ):
                self._refresh_token(tenant_id, client_id, client_secret)
            return self._token

    def _refresh_token(self, tenant_id, client_id, client_secret):
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        # Extract Dataverse URL for scope
        dataverse_scope = f"{DATAVERSE_URL}/.default"
        
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": dataverse_scope,
        }
        try:
            logger.info(f"Requesting new token with client_id: {client_id[:5]}...")

            session = requests.Session()
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)

            response = session.post(url, headers=headers, data=data, timeout=30)

            if response.status_code != 200:
                logger.error(
                    f"Token request failed with status: {response.status_code}"
                )
                logger.error(f"Response text: {response.text}")

            response.raise_for_status()
            token_data = response.json()
            self._token = token_data["access_token"]
            self._expiry = datetime.now() + timedelta(seconds=token_data["expires_in"])
            logger.info(f"Token refreshed. New expiry: {self._expiry}")
            session.close()

        except requests.exceptions.RequestException as e:
            logger.error(f"Token refresh error: {e}")
            if hasattr(e, "response") and e.response:
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text}")
            raise


token_cache = TokenCache()


class DataverseExtractor:
    """Main class for extracting data from Dataverse to SQL"""
    
    def __init__(self):
        self.engine = Engine
        self.script_name = os.path.basename(__file__)
        self.started = datetime.now()
        self.circuit_breakers = {}

    def get_circuit_breaker(self, entity_name):
        """Get or create circuit breaker for entity"""
        if entity_name not in self.circuit_breakers:
            self.circuit_breakers[entity_name] = CircuitBreaker()
        return self.circuit_breakers[entity_name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                logger.info("Script completed successfully")
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception as cleanup_err:
            print(f"Cleanup warning: {cleanup_err}")

        if exc_type is not None:
            print(f"Script finished with error: {exc_type.__name__}: {exc_val}")

    def get_source_properties(self):
        """
        Query SourceProperties table to get list of entities to extract.
        Returns None if table doesn't exist (use hardcoded entities instead).
        """
        try:
            query = text(
                f"""
                SELECT [SourceName], [Active], [Order], [Last_FullLoad], [Columns], [Where]
                FROM [{DB_SCHEMA}].[SourceProperties]
                WHERE Active = 1
                AND NOT CAST(ISNULL(Last_FullLoad, GETDATE() - 1) AS DATE) = CAST(GETDATE() AS DATE)
                ORDER BY [Order], [SourceName]
                """
            )
            df = pd.read_sql(query, con=self.engine.connect())
            
            if df.empty:
                logger.info("No active entities found in SourceProperties table")
                return None
            
            # Convert to list of dicts
            entities = []
            for _, row in df.iterrows():
                entity = {
                    "entity_name": row["SourceName"],
                    "columns": row["Columns"].split(", ") if pd.notna(row["Columns"]) else None,
                    "where_clause": row["Where"] if pd.notna(row["Where"]) else None
                }
                entities.append(entity)
            
            logger.info(f"Loaded {len(entities)} entities from SourceProperties table")
            return entities
            
        except Exception as e:
            logger.info(f"SourceProperties table not found or error: {e}")
            return None

    @timeout(10800)  # 3 hour timeout
    def main(self, entities_to_extract=None):
        """
        Main extraction process
        
        Args:
            entities_to_extract: Optional list of dicts with 'entity_name', 'columns', 'where_clause'.
                                If None, will try to load from SourceProperties table.
        """
        try:
            logger.info(f"{sys.argv[0]} started on: {socket.getfqdn()}")
            logger.info(f"Max workers: {MAX_WORKERS}")
            logger.info(f"Page size: {PAGE_SIZE}")
            logger.info(f"Rate limit interval: {RATE_LIMIT_INTERVAL}s")
            logger.info(
                f"Group timeout: {TOTAL_GROUP_TIMEOUT}s ({TOTAL_GROUP_TIMEOUT//3600}h {(TOTAL_GROUP_TIMEOUT%3600)//60}m)"
            )
            logger.info(
                f"Entity timeout: {ENTITY_TIMEOUT}s ({ENTITY_TIMEOUT//3600}h {(ENTITY_TIMEOUT%3600)//60}m)"
            )

            # Validate configuration
            if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, DATAVERSE_URL]):
                raise ValueError("Missing required environment variables. Please set TENANT_ID, CLIENT_ID, CLIENT_SECRET, and DATAVERSE_URL")

            # If no entities provided, try to load from SourceProperties table
            if entities_to_extract is None:
                logger.info("No entities provided, attempting to load from SourceProperties table...")
                entities_to_extract = self.get_source_properties()
                
                if entities_to_extract is None:
                    raise ValueError(
                        "No entities to extract. Either provide entities parameter or create SourceProperties table. "
                        "Run setup_config_table.sql to create the configuration table."
                    )

            self.process_entities(entities_to_extract)

            logger.info("Script completed successfully")
            return 0
        except Exception as e:
            error_msg = f"Unhandled error: {str(e)}"
            logger.error(error_msg)
            return 1

    def process_entities(self, entities):
        """Process entities in parallel"""
        try:
            logger.info(f"Processing {len(entities)} entities...")
            self.process_entity_group(entities, max_workers=MAX_WORKERS)
            return
        except Exception as e:
            logger.error(f"Exception in process_entities: {e}")
            raise

    def process_entity_group(self, entities, max_workers):
        """Process a group of entities with parallel workers"""
        if not entities:
            logger.info("No entities to process")
            return

        actual_workers = min(max_workers, len(entities), 5)

        logger.info(
            f"Processing {len(entities)} entities with {actual_workers} workers"
        )
        logger.info(
            f"Group timeout: {TOTAL_GROUP_TIMEOUT}s, Entity timeout: {ENTITY_TIMEOUT}s"
        )

        for entity in entities:
            logger.info(f"Will process entity: {entity['entity_name']}")

        group_start_time = time.time()

        with ThreadPoolExecutor(
            max_workers=actual_workers, thread_name_prefix="DataverseWorker"
        ) as executor:
            futures = [
                executor.submit(
                    self.fetch_and_upload,
                    entity["entity_name"],
                    entity.get("columns"),
                    entity.get("where_clause"),
                )
                for entity in entities
            ]

            errors = []
            completed_count = 0

            try:
                for future in as_completed(futures, timeout=TOTAL_GROUP_TIMEOUT):
                    try:
                        elapsed_time = time.time() - group_start_time
                        remaining_time = TOTAL_GROUP_TIMEOUT - elapsed_time

                        if remaining_time <= TOTAL_GROUP_TIMEOUT * 0.1:
                            logger.warning(
                                f"Group timeout approaching: {remaining_time:.1f}s remaining"
                            )

                        entity_timeout = min(
                            ENTITY_TIMEOUT, max(60, remaining_time - 30)
                        )

                        future.result(timeout=entity_timeout)
                        completed_count += 1
                        logger.info(
                            f"Completed {completed_count}/{len(entities)} entities "
                            f"(elapsed: {elapsed_time:.1f}s)"
                        )

                        if completed_count % 3 == 0:
                            gc.collect()

                    except TimeoutError as te:
                        logger.error(
                            f"Entity timeout after {entity_timeout}s: {str(te)}"
                        )
                        errors.append(f"Entity timeout: {str(te)}")
                        completed_count += 1

                    except Exception as e:
                        logger.error(f"Exception occurred: {e}")
                        errors.append(str(e))
                        completed_count += 1

            except TimeoutError:
                elapsed_time = time.time() - group_start_time
                logger.error(
                    f"Group timeout exceeded after {elapsed_time:.1f}s. "
                    f"Completed {completed_count}/{len(entities)} entities"
                )

                remaining_count = 0
                for future in futures:
                    if not future.done():
                        future.cancel()
                        remaining_count += 1

                if remaining_count > 0:
                    logger.warning(
                        f"Cancelled {remaining_count} remaining entity tasks"
                    )

                errors.append(f"Group timeout exceeded after {elapsed_time:.1f}s")

        total_elapsed = time.time() - group_start_time
        logger.info(
            f"All tasks completed in {total_elapsed:.1f}s. "
            f"Success: {completed_count - len(errors)}, Errors: {len(errors)}"
        )

        if errors:
            error_summary = f"Failed to process {len(errors)} entities"
            if len(errors) <= 3:
                error_summary += f": {', '.join(errors)}"
            else:
                error_summary += (
                    f": {', '.join(errors[:3])}... and {len(errors)-3} more"
                )

            if len(errors) == len(entities):
                raise Exception(error_summary)
            else:
                logger.warning(error_summary)
        return

    def adjust_datatype(self, value):
        """Trim all columns to max 100 chars, return None for empty values"""
        if (
            pd.isna(value)
            or value is None
            or value == ""
            or value == "nan"
            or value == "None"
            or value == "NULL"
        ):
            return None
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)[:100]

    def convert_to_string(self, obj):
        """Recursively convert all values in dict or list to string"""
        if isinstance(obj, dict):
            return {k: self.convert_to_string(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_string(elem) for elem in obj]
        elif isinstance(obj, bool):
            return "1" if obj else "0"
        else:
            return self.adjust_datatype(obj)

    @rate_limit_decorator(min_interval=RATE_LIMIT_INTERVAL)
    def fetch_data_from_dataverse(
        self,
        entity_name,
        columns=None,
        where_clause=None,
    ):
        """Fetch data from Dataverse API with pagination"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(
            pool_connections=1, pool_maxsize=1, max_retries=retry_strategy
        )
        session.mount("https://", adapter)

        url = f"{DATAVERSE_URL}/api/data/v9.2/{entity_name}"

        query_params = []

        if columns and any(columns):
            valid_columns = [col for col in columns if col and pd.notna(col)]
            if valid_columns:
                query_params.append(f"$select={','.join(valid_columns)}")
                logger.info(
                    f"Fetching specific columns for {entity_name}: {valid_columns}"
                )
        else:
            logger.info(f"Fetching all columns for {entity_name}")

        if where_clause and pd.notna(where_clause) and where_clause.strip():
            query_params.append(f"$filter={where_clause}")
            logger.info(f"Applying filter for {entity_name}: {where_clause}")

        if query_params:
            url += "?" + "&".join(query_params)

        page_count = 0
        total_records = 0

        try:
            while url:
                try:
                    page_count += 1

                    access_token = token_cache.get_token(
                        TENANT_ID, CLIENT_ID, CLIENT_SECRET
                    )

                    headers = {
                        "Authorization": f"Bearer {access_token}",
                        "OData-MaxVersion": "4.0",
                        "OData-Version": "4.0",
                        "Accept": "application/json",
                        "Prefer": f'odata.include-annotations="OData.Community.Display.V1.FormattedValue",odata.maxpagesize={PAGE_SIZE}',
                        "Connection": "close",
                    }

                    response = session.get(
                        url, headers=headers, timeout=(30, 120)
                    )
                    response.raise_for_status()
                    data = response.json()

                    records_count = len(data.get("value", []))
                    total_records += records_count

                    for item in data.get("value", []):
                        yield self.convert_to_string(item)

                    next_link = data.get("@odata.nextLink")
                    if next_link:
                        url = next_link
                        time.sleep(0.1)
                    else:
                        logger.info(
                            f"Pagination complete for {entity_name}. Total: {total_records} records"
                        )
                        url = None

                except requests.exceptions.RequestException as e:
                    logger.error(f"Request error for {entity_name}: {e}")

                    response_status = getattr(e, "response", None)
                    if response_status is not None:
                        logger.error(f"Status code: {response_status.status_code}")
                        logger.error(f"Response body: {response_status.text}")

                        if response_status.status_code == 401:
                            logger.warning("Access token expired. Refreshing token...")
                            token_cache._token = None
                            continue
                        elif response_status.status_code == 429:
                            retry_after = int(
                                response_status.headers.get("Retry-After", 60)
                            )
                            logger.warning(
                                f"Rate limit reached. Waiting for {retry_after} seconds..."
                            )
                            time.sleep(retry_after)
                            continue

                    if isinstance(
                        e,
                        (requests.exceptions.Timeout, requests.exceptions.ReadTimeout),
                    ):
                        logger.error(
                            f"Timeout after 2 minutes for {entity_name} page {page_count}"
                        )
                        logger.info(f"Will retry this page for {entity_name}")
                        time.sleep(5)
                        continue

                    logger.error(f"Error fetching data: {e}")
                    raise

        finally:
            session.close()
            time.sleep(0.1)

    def clean_column_names(self, df):
        """Clean column names in dataframe"""
        df_result = df.copy()
        df_result.columns = (
            df_result.columns.str.replace(r"^_+", "", regex=True)
            .str.replace(r"\$", "", regex=True)
            .str.replace(
                r"_value@Microsoft.Dynamics.CRM.lookuplogicalname", "lookupname"
            )
            .str.replace(r"@OData.Community.Display.V1.FormattedValue", "name")
            .str.replace(r"_value", "")
        )
        return df_result.drop(columns=["@odata.etag"], errors="ignore")

    def get_column_info(self, engine, schema, table_name):
        """Get column info from database table"""
        query = f"""
        SELECT 
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable
        FROM 
            sys.columns c
        INNER JOIN 
            sys.types t ON c.user_type_id = t.user_type_id
        INNER JOIN 
            sys.tables s ON s.object_id = c.object_id
        WHERE 
            s.name = '{table_name}' AND s.schema_id = SCHEMA_ID('{schema}')
        """
        with engine.connect() as connection:
            result = connection.execute(text(query))
            return {row.column_name: row for row in result}

    def process_and_upload_data(self, entity_name, data_generator, columns):
        """Process and upload data to SQL database"""
        chunksize = 20000
        chunk = []
        chunk_counter = 0
        total_records_processed = 0

        inspector = inspect(self.engine)
        table_exists = inspector.has_table(entity_name, schema=DB_SCHEMA)

        try:
            first_item = next(data_generator)
            logger.info(f"Successfully got first item for {entity_name}")
        except StopIteration:
            logger.warning(f"No data found for entity {entity_name}")
            return

        sample_df = pd.DataFrame([first_item])
        sample_df = self.clean_column_names(sample_df)

        if not table_exists:
            self.create_table_if_not_exists(
                self.engine, entity_name, sample_df.columns.tolist()
            )

        column_info = self.get_column_info(self.engine, DB_SCHEMA, entity_name)

        dropped_columns = set(sample_df.columns) - set(column_info.keys())
        for col in dropped_columns:
            logger.info(
                f"Table {entity_name}: Column {col} not found in SQL table. Dropping."
            )

        sql_types = {
            col: String(100) for col in column_info.keys() if col != "AuditDate"
        }
        sql_types["AuditDate"] = DateTime()

        chunk = [first_item]

        with self.engine.begin() as connection:
            logger.info(f"Starting transaction for {entity_name}")

            if table_exists:
                logger.info(f"Truncating table {DB_SCHEMA}.{entity_name}")
                connection.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{entity_name}"))

            for item in data_generator:
                chunk.append(item)
                if len(chunk) == chunksize:
                    chunk_counter += 1
                    df_chunk = self.process_chunk(chunk, column_info, entity_name)
                    if not df_chunk.empty:
                        self.upload_chunk(connection, entity_name, df_chunk, sql_types)
                        total_records_processed += len(df_chunk)
                        logger.info(
                            f"{entity_name}: Uploaded chunk {chunk_counter} ({len(df_chunk)} records, total: {total_records_processed})"
                        )

                    del df_chunk
                    chunk.clear()

                    if chunk_counter % 5 == 0:
                        gc.collect()

            if chunk:
                chunk_counter += 1
                df_chunk = self.process_chunk(chunk, column_info, entity_name)
                if not df_chunk.empty:
                    self.upload_chunk(connection, entity_name, df_chunk, sql_types)
                    total_records_processed += len(df_chunk)
                    logger.info(
                        f"{entity_name}: Uploaded final chunk {chunk_counter} ({len(df_chunk)} records, total: {total_records_processed})"
                    )

            # Update Last_FullLoad timestamp if using config table
            # Only update if SourceProperties table exists
            try:
                connection.execute(
                    text(
                        f"""UPDATE [{DB_SCHEMA}].[SourceProperties]
                        SET Last_FullLoad = GETDATE()
                        WHERE [SourceName] = :entity_name"""
                    ),
                    {"entity_name": entity_name},
                )
                logger.info(f"Updated Last_FullLoad timestamp for {entity_name}")
            except Exception:
                # Table might not exist if not using config-based approach
                pass

            logger.info(
                f"Transaction completed for {entity_name}. Total records: {total_records_processed}"
            )

    def create_table_if_not_exists(self, engine, table_name, columns):
        """Create table if it doesn't exist"""
        metadata = MetaData()

        table_columns = []
        for col in columns:
            if col != "AuditDate":
                table_columns.append(Column(col, NVARCHAR(100)))

        table_columns.append(Column("AuditDate", DateTime()))

        table = Table(table_name, metadata, *table_columns, schema=DB_SCHEMA)

        metadata.create_all(engine)
        logger.info(
            f"Created table {table_name} in schema {DB_SCHEMA} with {len(table_columns)} columns"
        )

    def process_chunk(self, chunk, column_info, entity_name):
        """Process a chunk of data into DataFrame"""
        df = pd.DataFrame(chunk)

        if any("," in col for col in df.columns):
            new_cols = {}
            for col in df.columns:
                if "," in col:
                    split_cols = [c.strip() for c in col.split(",")]
                    for i, split_col in enumerate(split_cols):
                        new_cols[col + f"_{i}"] = df[col]
                else:
                    new_cols[col] = df[col]
            df = pd.DataFrame(new_cols)

        df = self.clean_column_names(df)

        columns_to_keep = [col for col in df.columns if col in column_info]
        df = df[columns_to_keep]

        df["AuditDate"] = pd.Timestamp.now()

        return df

    def upload_chunk(self, connection, entity_name, df, sql_types):
        """Upload a chunk of data to SQL"""
        try:
            df.to_sql(
                name=entity_name,
                con=connection,
                schema=DB_SCHEMA,
                if_exists="append",
                index=False,
                dtype=sql_types,
            )
        except pyodbc.Error as e:
            logger.error(f"Error uploading chunk for {entity_name}: {str(e)}")
            for col in df.columns:
                logger.error(f"Column {col} info:")
                logger.error(f"  Data type: {sql_types.get(col, 'Unknown')}")
                logger.error(f"  Sample values: {df[col].head().tolist()}")
            raise

    def fetch_and_upload(
        self,
        entity_name,
        columns,
        where_clause,
        retries=3,
        delay=60,
    ):
        """Fetch data from Dataverse and upload to SQL"""
        log_system_resources(entity_name)

        for attempt in range(retries):
            try:
                logger.info(
                    f"Starting data retrieval for: {entity_name} (attempt {attempt + 1}/{retries})"
                )

                entity_circuit_breaker = self.get_circuit_breaker(entity_name)
                data_generator = entity_circuit_breaker.call(
                    self.fetch_data_from_dataverse,
                    entity_name,
                    columns,
                    where_clause,
                )

                self.process_and_upload_data(
                    entity_name, data_generator, columns
                )
                logger.info(f"Data upload completed for table: {entity_name}")

                log_system_resources(entity_name)
                return

            except requests.exceptions.Timeout as e:
                logger.error(f"Timeout error for {entity_name}: {str(e)}")
                if attempt < retries - 1:
                    retry_delay = delay * (attempt + 1)
                    logger.info(
                        f"Retrying in {retry_delay} seconds for {entity_name}..."
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to process entity {entity_name} after {retries} attempts"
                    )
                    raise

            except Exception as e:
                logger.error(
                    f"Failed to fetch or upload data for entity {entity_name}: {str(e)}"
                )
                if attempt < retries - 1:
                    retry_delay = delay * (attempt + 1)
                    logger.info(
                        f"Retrying in {retry_delay} seconds for {entity_name}..."
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        f"Failed to process entity {entity_name} after {retries} attempts"
                    )
                    raise


if __name__ == "__main__":
    # Two ways to use this script:
    
    # Option 1: Load entities from SourceProperties SQL table (recommended)
    # Run setup_config_table.sql first to create and populate the table
    with DataverseExtractor() as extractor:
        exit_code = extractor.main()  # Will load from SourceProperties table
    
    # Option 2: Hardcode entities directly in script
    # entities = [
    #     {
    #         "entity_name": "accounts",
    #         "columns": ["accountid", "name", "emailaddress1"],
    #         "where_clause": None
    #     },
    #     {
    #         "entity_name": "contacts",
    #         "columns": None,  # All columns
    #         "where_clause": "statecode eq 0"  # Only active contacts
    #     }
    # ]
    # with DataverseExtractor() as extractor:
    #     exit_code = extractor.main(entities)

    os._exit(exit_code)
