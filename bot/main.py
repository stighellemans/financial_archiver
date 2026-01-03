import os
import requests
import logging
import sys
import time
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Import our new extraction logic
import extraction 

# --- 1. CONFIGURATION & LOGGING ---
LOG_FILE = "/app/logs/bot.log"
DOWNLOAD_DIR = "/app/downloads"

# Ensure directories exist
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FinancialBot")

# --- 2. DATABASE MANAGER ---
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=10,
        host=os.environ["DB_HOST"], database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"], password=os.environ["DB_PASS"]
    )
except Exception as e:
    logger.error(f"Fatal Error: Could not create DB pool: {e}")
    sys.exit(1)

@contextmanager
def get_db_cursor():
    """Yields a cursor from a pooled connection and handles commit/rollback."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Database Transaction Error: {e}")
        raise
    finally:
        db_pool.putconn(conn)

def migrate_db_schema():
    """Safely adds columns if they don't exist by checking information_schema first."""
    logger.info("Checking database schema...")
    
    # Define the columns you want to ensure exist
    # format: "column_name": "data_type"
    desired_columns = {
        "transaction_date": "DATE",
        "date_extracted": "BOOLEAN DEFAULT FALSE",
        "amount": "NUMERIC(10, 2)",
        "category": "TEXT",
        "description": "TEXT",
        "transcription": "TEXT",
        "status": "TEXT DEFAULT 'new'"
    }

    with get_db_cursor() as cur:
        # 1. Get list of columns that ALREADY exist in the DB
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'messages';
        """)
        existing_columns = {row[0] for row in cur.fetchall()}

        # 2. Loop through desired columns and add them ONLY if missing
        for col_name, col_type in desired_columns.items():
            if col_name not in existing_columns:
                logger.info(f"Migrating: Adding missing column '{col_name}'...")
                cur.execute(f"ALTER TABLE messages ADD COLUMN {col_name} {col_type};")
            else:
                logger.debug(f"Column '{col_name}' already exists. Skipping.")
    
    logger.info("Schema migration checks complete.")

def backfill_data():
    """
    One-time adjustment: Finds rows where status is 'new' but amount/transaction_date is NULL,
    and runs the extraction logic on them.
    """
    logger.info("Starting Backfill of existing data...")
    try:
        with get_db_cursor() as cur:
            # Find messages that haven't been processed for data yet
            cur.execute("""
                SELECT id, text, timestamp 
                FROM messages 
                WHERE status = 'new' AND (amount IS NULL OR transaction_date IS NULL)
            """)
            rows = cur.fetchall()
            
            count = 0
            for row in rows:
                row_id, text, ts = row
                
                # Run Logic
                data = extraction.extract_transaction_data(text, ts)
                
                # Update
                cur.execute("""
                    UPDATE messages 
                    SET amount = %s,
                        transaction_date = %s,
                        date_extracted = %s,
                        description = %s
                    WHERE id = %s
                """, (
                    data['amount'], 
                    data['transaction_date'], 
                    data['date_extracted'], 
                    data['description'], 
                    row_id
                ))
                count += 1
            
            logger.info(f"Backfilled {count} rows with extracted data.")
    except Exception as e:
        logger.error(f"Backfill failed: {e}")

# --- 3. CORE LOGIC ---

def download_files(files_list, timestamp_prefix):
    """Downloads files and returns a semicolon-separated string of paths."""
    saved_paths = []
    headers = {'Authorization': f'Bearer {os.environ["SLACK_BOT_TOKEN"]}'}

    for f in files_list:
        url = f.get("url_private_download")
        if not url: continue

        # Naming convention: TS_Filename
        file_name = f"{timestamp_prefix}_{f.get('name')}"
        file_path = os.path.join(DOWNLOAD_DIR, file_name)

        # Skip if already exists (idempotency)
        if os.path.exists(file_path):
            saved_paths.append(file_path)
            continue

        try:
            logger.info(f"Downloading: {file_name}")
            r = requests.get(url, headers=headers, stream=True, timeout=30)
            if r.status_code == 200:
                with open(file_path, 'wb') as file_handle:
                    for chunk in r.iter_content(1024):
                        file_handle.write(chunk)
                saved_paths.append(file_path)
        except Exception as e:
            logger.error(f"Failed to download {file_name}: {e}")

    return ";".join(saved_paths) if saved_paths else None

def process_and_save_message(channel_id, ts, user_id, text, files_list):
    """
    Handles Upserts with Status Locking and Data Extraction.
    """
    try:
        # 1. Check existing status
        with get_db_cursor() as cur:
            cur.execute("SELECT status FROM messages WHERE channel_id = %s AND timestamp = %s", (channel_id, ts))
            res = cur.fetchone()
            
            if res:
                current_status = res[0]
                # LOCK: If status is not 'new', do not update
                if current_status != 'new':
                    logger.info(f"Skipping update for {ts}: Status is '{current_status}' (Immutable)")
                    return

        # 2. Extract Data
        data = extraction.extract_transaction_data(text, ts)
        file_path_string = download_files(files_list, ts) if files_list else None

        # 3. Upsert
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages (
                    user_id, channel_id, text, file_path, timestamp, 
                    amount, transaction_date, date_extracted, description, status
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'new') 
                ON CONFLICT (channel_id, timestamp) 
                DO UPDATE SET 
                    text = EXCLUDED.text, 
                    file_path = EXCLUDED.file_path,
                    amount = EXCLUDED.amount,
                    transaction_date = EXCLUDED.transaction_date,
                    date_extracted = EXCLUDED.date_extracted,
                    description = EXCLUDED.description
                -- Note: We don't update 'status' here, it stays what it was (or defaults to new on insert)
                """,
                (
                    user_id, channel_id, text, file_path_string, ts,
                    data['amount'], data['transaction_date'], data['date_extracted'], data['description']
                )
            )
            
    except Exception as e:
        logger.error(f"Failed to save message {ts}: {e}")

def delete_message_from_db(channel_id, ts):
    try:
        with get_db_cursor() as cur:
            # LOCK: Check status before deleting
            cur.execute("SELECT status FROM messages WHERE channel_id = %s AND timestamp = %s", (channel_id, ts))
            res = cur.fetchone()
            
            if res:
                current_status = res[0]
                if current_status != 'new':
                    logger.warning(f"Blocked deletion for {ts}: Status is '{current_status}'")
                    return
                
                # Proceed with delete
                cur.execute(
                    "DELETE FROM messages WHERE channel_id = %s AND timestamp = %s",
                    (channel_id, ts)
                )
                logger.info(f"Deleted message {ts} from DB.")
                
    except Exception as e:
        logger.error(f"Failed to delete message {ts}: {e}")

# --- 4. STARTUP SYNC (Simplified for brevity, uses process_and_save_message) ---
def sync_missing_data():
    logger.info("--- Starting Startup Sync ---")
    client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])
    
    try:
        # Get all channels
        result = client.users_conversations(types="public_channel,private_channel")
        channels = result.get("channels", [])
        
        for channel in channels:
            cid = channel["id"]
            logger.info(f"Syncing channel: {channel['name']}")
            
            # Fetch history
            try:
                # Fetch the last 50 messages
                history = client.conversations_history(channel=cid, limit=50)
                messages = history.get("messages", [])
                
                for msg in messages:
                    if msg.get("subtype") in ["channel_join", "channel_leave", "channel_topic"]: continue
                    
                    # process_and_save_message handles idempotency and regex extraction
                    process_and_save_message(cid, msg.get("ts"), msg.get("user"), msg.get("text"), msg.get("files", []))

            except SlackApiError:
                continue
    except Exception as e:
        logger.error(f"Sync failed: {e}")
    
    logger.info("--- Startup Sync Complete ---")

# --- 5. REAL-TIME EVENT LISTENER ---
app = App(token=os.environ["SLACK_BOT_TOKEN"])

@app.event("message")
def handle_message_events(body):
    logger.debug(f"EVENT RECEIVED:\n{body}")
    
    event = body.get("event", {})
    subtype = event.get("subtype")
    channel_id = event.get("channel")
    ts = event.get("ts")

    if subtype in ["channel_join", "channel_leave", "channel_topic"]: return

    # 2. Handle Deletion
    if subtype == "message_deleted":
        deleted_ts = event.get("deleted_ts") or event.get("previous_message", {}).get("ts")
        if deleted_ts: delete_message_from_db(channel_id, deleted_ts)
        return

    # Normal or Edited Message
    if subtype == "message_changed":
        logger.debug("Processing EDIT event")
        # For edits, the actual text/files are inside the inner 'message' dict
        payload = event.get("message", {})
        message_ts = payload.get("ts") 
    else:
        logger.debug("Processing NEW MESSAGE event")
        # For new messages, data is at the root of the event
        payload = event
        message_ts = ts

    process_and_save_message(channel_id, message_ts, payload.get("user"), payload.get("text"), payload.get("files", []))

# --- 6. ENTRY POINT ---
if __name__ == "__main__":
    # 1. Update Schema
    migrate_db_schema()
    
    # 2. Backfill existing rows with regex data
    backfill_data()
    
    # 3. Sync recent history from Slack
    sync_missing_data()
    
    # 4. Start Listener
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()