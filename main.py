import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from web3 import Web3
import sqlite3
import threading
import uvicorn
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

# Configuration
RPC_URL = "https://rpc.mainnet.taraxa.io"
MAX_WORKERS = 10
BATCH_SIZE = 1000
RETRY_DELAY = 5
MAX_RETRIES = 3
CHECKPOINT_INTERVAL = 10000
POLL_INTERVAL = 15  # Seconds between new block checks

# Initialize Web3
for attempt in range(MAX_RETRIES):
    try:
        w3 = Web3(Web3.HTTPProvider(RPC_URL))
        if w3.is_connected():
            break
        print(f"RPC connection attempt {attempt + 1} failed: Not connected")
        time.sleep(RETRY_DELAY)
    except Exception as e:
        print(f"RPC connection attempt {attempt + 1} failed: {str(e)}")
        time.sleep(RETRY_DELAY)
else:
    raise ConnectionError("Failed to connect to RPC after retries")

# Database setup
def init_db():
    conn = sqlite3.connect('addresses.db', check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS addresses (
            address TEXT PRIMARY KEY
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS progress (
            last_block INTEGER,
            is_complete BOOLEAN DEFAULT 0,
            last_updated REAL
        )
    ''')
    conn.commit()
    return conn

db_conn = init_db()

# Data model
class AddressCountResponse(BaseModel):
    total_addresses: int
    last_block_processed: int
    last_updated: float
    is_complete: bool
    message: Optional[str] = None

# Background scanner
class BlockScanner(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.lock = threading.Lock()
        self.running = True

    def run(self):
        while self.running:
            try:
                if not self.is_initial_scan_complete():
                    self.run_initial_scan()
                else:
                    self.monitor_new_blocks()
            except Exception as e:
                print(f"Scanner error: {e}")
                time.sleep(RETRY_DELAY)

    def is_initial_scan_complete(self):
        cursor = db_conn.cursor()
        cursor.execute("SELECT is_complete FROM progress LIMIT 1")
        result = cursor.fetchone()
        return result and result[0]

    def get_last_processed_block(self):
        cursor = db_conn.cursor()
        cursor.execute("SELECT last_block FROM progress LIMIT 1")
        result = cursor.fetchone()
        return result[0] if result else 0

    def save_progress(self, block_num, complete=False):
        with self.lock:
            cursor = db_conn.cursor()
            cursor.execute("DELETE FROM progress")
            cursor.execute(
                "INSERT INTO progress (last_block, is_complete, last_updated) VALUES (?, ?, ?)",
                (block_num, int(complete), time.time())
            )
            db_conn.commit()

    def save_addresses(self, addresses):
        with self.lock:
            cursor = db_conn.cursor()
            cursor.executemany(
                "INSERT OR IGNORE INTO addresses (address) VALUES (?)",
                [(addr,) for addr in addresses]
            )
            db_conn.commit()

    def process_block_range(self, start, end):
        addresses = set()
        retries = 0
        
        while retries < MAX_RETRIES:
            try:
                for block_num in range(start, end + 1):
                    block = w3.eth.get_block(block_num, full_transactions=False)
                    if not block or not block.transactions:
                        continue
                    
                    full_block = w3.eth.get_block(block_num, full_transactions=True)
                    for tx in full_block.transactions:
                        addresses.add(tx['from'].lower())
                        if tx['to']:
                            addresses.add(tx['to'].lower())
                
                return addresses
            
            except Exception as e:
                print(f"Error processing blocks {start}-{end}: {e}")
                retries += 1
                if retries < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * retries)
                else:
                    print(f"Max retries reached for blocks {start}-{end}")
                    return set()

    def run_initial_scan(self):
        last_processed = self.get_last_processed_block()
        end_block = w3.eth.block_number

        print(f"Starting initial scan from block {last_processed} to {end_block}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for batch_start in range(last_processed, end_block + 1, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE - 1, end_block)
                futures.append(executor.submit(self.process_block_range, batch_start, batch_end))
            
            for future in as_completed(futures):
                addresses = future.result()
                if addresses:
                    self.save_addresses(addresses)
                
                current_block = min(futures.index(future) * BATCH_SIZE + BATCH_SIZE, end_block)
                if current_block % CHECKPOINT_INTERVAL == 0:
                    self.save_progress(current_block)
                    print(f"Progress: {current_block}/{end_block} blocks")

        self.save_progress(end_block, complete=True)
        print(f"Initial scan complete. Processed up to block {end_block}")

    def monitor_new_blocks(self):
        last_block = self.get_last_processed_block()
        
        while self.running:
            try:
                current_block = w3.eth.block_number
                if current_block > last_block:
                    print(f"Processing new blocks {last_block + 1}-{current_block}")
                    
                    addresses = self.process_block_range(last_block + 1, current_block)
                    if addresses:
                        self.save_addresses(addresses)
                        print(f"Added {len(addresses)} new addresses")
                    
                    self.save_progress(current_block, complete=True)
                    last_block = current_block
                
                time.sleep(POLL_INTERVAL)
                
            except Exception as e:
                print(f"Monitoring error: {e}")
                time.sleep(RETRY_DELAY)

# Start the scanner when app starts
scanner = BlockScanner()

@app.on_event("startup")
async def startup_event():
    scanner.start()

@app.get("/total-addresses", response_model=AddressCountResponse)
async def get_total_addresses():
    with scanner.lock:
        cursor = db_conn.cursor()
        
        # Get address count
        cursor.execute("SELECT COUNT(*) FROM addresses")
        total_addresses = cursor.fetchone()[0]
        
        # Get progress info
        cursor.execute("SELECT last_block, is_complete, last_updated FROM progress LIMIT 1")
        progress = cursor.fetchone()
        
        if progress:
            last_block, is_complete, last_updated = progress
            message = "Complete" if is_complete else "Initial scan in progress"
        else:
            last_block, is_complete, last_updated = 0, False, 0
            message = "No scan progress yet"
        
        return {
            "total_addresses": total_addresses,
            "last_block_processed": last_block,
            "last_updated": last_updated,
            "is_complete": bool(is_complete),
            "message": message
        }

@app.get("/health")
async def health_check():
    return {"status": "alive", "scanner_alive": scanner.is_alive()}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
