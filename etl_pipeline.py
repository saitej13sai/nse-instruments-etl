import pandas as pd
import sqlite3
from pymongo import MongoClient, UpdateOne
import os
from datetime import datetime
import logging
import requests
import gzip
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
OUTPUT_DIR = "C:/Users/saite/Downloads/output"
UPSTOX_FILE = os.path.join(OUTPUT_DIR, "NSE.csv.gz")
UPSTOX_EXTRACTED_FILE = os.path.join(OUTPUT_DIR, "NSE_clean.csv")
DHAN_FILE = os.path.join(OUTPUT_DIR, "api-scrip-master.csv")
MONGO_URI = "mongodb+srv://myuser:mypassword123@cluster0.kxzttwo.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "market_data"
COLLECTION_NAME = "upstox_nse"
SQLITE_DB = "C:/Users/saite/Downloads/nse_data.db"

# SQLite schema
SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS dhan_nse (
    exchange TEXT,
    security_id TEXT PRIMARY KEY,
    symbol_name TEXT,
    trading_symbol TEXT UNIQUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_trading_symbol ON dhan_nse (trading_symbol);
"""

os.makedirs(OUTPUT_DIR, exist_ok=True)

upstox_columns = [
    'instrument_key', 'exchange_token', 'tradingsymbol', 'name', 'last_price',
    'expiry', 'strike', 'tick_size', 'lot_size', 'instrument_type',
    'option_type', 'exchange'
]

column_mapping = {
    'SEM_SMST_SECURITY_ID': 'instrument_key',
    'SEM_TRADING_SYMBOL': 'tradingsymbol',
    'SEM_INSTRUMENT_NAME': 'name',
    'SEM_EXPIRY_DATE': 'expiry',
    'SEM_STRIKE_PRICE': 'strike',
    'SEM_TICK_SIZE': 'tick_size',
    'SEM_LOT_UNITS': 'lot_size',
    'SEM_EXCH_INSTRUMENT_TYPE': 'instrument_type',
    'SEM_OPTION_TYPE': 'option_type',
    'SEM_EXM_EXCH_ID': 'exchange'
}

def download_file(url, destination):
    try:
        logger.info(f"Downloading file from {url}")
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        with open(destination, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded file to {destination}")
    except Exception as e:
        logger.error(f"Download error for {url}: {str(e)}")
        raise

def decompress_gz(gz_file, extracted_file):
    try:
        logger.info(f"Decompressing {gz_file} to {extracted_file}")
        with gzip.open(gz_file, 'rb') as f_in:
            with open(extracted_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        logger.info(f"Decompressed file to {extracted_file}")
    except Exception as e:
        logger.error(f"Decompression error: {str(e)}")
        raise

def apply_mongo_schema_and_index():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.create_index([("instrument_key", 1)], unique=True)
        logger.info(f"MongoDB index applied on {COLLECTION_NAME}")
        client.close()
    except Exception as e:
        logger.error(f"MongoDB index error: {str(e)}")
        raise

def apply_sqlite_schema():
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()
        cursor.executescript(SQLITE_SCHEMA)
        conn.commit()
        logger.info("SQLite schema applied")
        conn.close()
    except Exception as e:
        logger.error(f"SQLite schema error: {str(e)}")
        raise

def extract_upstox_data():
    try:
        logger.info(f"Reading Upstox file: {UPSTOX_EXTRACTED_FILE}")
        if not os.path.exists(UPSTOX_EXTRACTED_FILE):
            raise FileNotFoundError(f"Upstox file not found at {UPSTOX_EXTRACTED_FILE}")

        df = pd.read_csv(UPSTOX_EXTRACTED_FILE)
        df = df[(df['exchange'] == 'NSE_EQ') & (df['instrument_type'] == 'EQUITY')]

        df = df[['instrument_key', 'tradingsymbol', 'name']].copy()
        df.rename(columns={'tradingsymbol': 'trading_symbol'}, inplace=True)
        df['exchange'] = 'NSE'
        df['short_name'] = df['name']
        df['isin'] = None
        df['trading_symbol'] = df['trading_symbol'].str.strip().str.upper()

        logger.info(f"Extracted {len(df)} Upstox NSE Equity records")
        return df
    except Exception as e:
        logger.error(f"Upstox extraction error: {str(e)}")
        raise

def extract_dhan_data():
    try:
        logger.info(f"Reading Dhan file: {DHAN_FILE}")
        if not os.path.exists(DHAN_FILE):
            raise FileNotFoundError(f"Dhan file not found at {DHAN_FILE}")

        df = pd.read_csv(DHAN_FILE, low_memory=False)
        df = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'].str.upper() == 'EQUITY')]

        df = df.rename(columns=column_mapping)

        for col in ['exchange_token', 'last_price']:
            df[col] = None

        df['trading_symbol'] = df['tradingsymbol'].str.strip().str.upper()
        df['symbol_name'] = df.get('SM_SYMBOL_NAME', '')
        df['security_id'] = df['instrument_key']

        logger.info(f"Extracted {len(df)} Dhan NSE Equity records")
        return df
    except Exception as e:
        logger.error(f"Dhan extraction error: {str(e)}")
        raise

def load_to_mongodb(upstox_df):
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        # Batch processing with correct UpdateOne objects
        batch_size = 1000
        records = upstox_df.to_dict('records')
        total_records = len(records)
        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            operations = [
                UpdateOne(
                    {'instrument_key': record['instrument_key']},
                    {'$set': record},
                    upsert=True
                )
                for record in batch
            ]
            collection.bulk_write(operations, ordered=False)
            logger.info(f"Inserted batch {i//batch_size + 1} of {(total_records + batch_size - 1)//batch_size} into MongoDB")

        logger.info(f"Inserted {total_records} records into MongoDB {COLLECTION_NAME}")
        client.close()
    except Exception as e:
        logger.error(f"MongoDB insert error: {str(e)}")
        raise

def load_to_sqlite(dhan_df):
    try:
        conn = sqlite3.connect(SQLITE_DB)
        cursor = conn.cursor()

        for _, row in dhan_df.iterrows():
            cursor.execute('''
                INSERT OR REPLACE INTO dhan_nse (exchange, security_id, symbol_name, trading_symbol, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                row['exchange'],
                row['security_id'],
                row['symbol_name'],
                row['trading_symbol'],
                datetime.utcnow(),
                datetime.utcnow()
            ))

        conn.commit()
        logger.info(f"Inserted {len(dhan_df)} records into SQLite dhan_nse")
        conn.close()
    except Exception as e:
        logger.error(f"SQLite insert error: {str(e)}")
        raise

def compare_datasets(upstox_df, dhan_df):
    try:
        merged = pd.merge(upstox_df, dhan_df, on=['exchange', 'trading_symbol'], how='inner')

        common_stocks = merged[[
            'exchange', 'instrument_key_x', 'security_id', 'symbol_name',
            'short_name', 'name_x', 'isin', 'trading_symbol'
        ]].rename(columns={'instrument_key_x': 'instrument_key', 'name_x': 'name'})

        only_upstox = upstox_df[~upstox_df['trading_symbol'].isin(dhan_df['trading_symbol'])]
        only_dhan = dhan_df[~dhan_df['trading_symbol'].isin(upstox_df['trading_symbol'])]

        common_stocks.to_csv(os.path.join(OUTPUT_DIR, 'common_stocks.csv'), index=False)
        only_upstox.to_csv(os.path.join(OUTPUT_DIR, 'only_in_upstox.csv'), index=False)
        only_dhan.to_csv(os.path.join(OUTPUT_DIR, 'only_in_dhan.csv'), index=False)

        logger.info("Comparison CSVs generated")
    except Exception as e:
        logger.error(f"Comparison error: {str(e)}")
        raise

def run_pipeline():
    try:
        logger.info("Starting ETL pipeline")
        # Download and decompress files
        download_file(UPSTOX_URL, UPSTOX_FILE)
        decompress_gz(UPSTOX_FILE, UPSTOX_EXTRACTED_FILE)
        download_file(DHAN_URL, DHAN_FILE)

        apply_sqlite_schema()
        apply_mongo_schema_and_index()

        upstox_df = extract_upstox_data()
        dhan_df = extract_dhan_data()

        load_to_mongodb(upstox_df)
        load_to_sqlite(dhan_df)

        compare_datasets(upstox_df, dhan_df)
        logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        # Clean up downloaded files
        for file_path in [UPSTOX_FILE, UPSTOX_EXTRACTED_FILE, DHAN_FILE]:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up file: {file_path}")

if __name__ == "__main__":
    run_pipeline()