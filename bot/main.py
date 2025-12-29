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

# --- 1. CONFIGURATION & LOGGING ---
LOG_FILE = "/app/logs/bot.log"
DOWNLOAD_DIR = "/app/downloads"

# Ensure directories exist
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("FinancialBot")

# --- 2. DATABASE MANAGER (Connection Pool) ---
try:
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"]
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

def init_db():
    """Create the table if it doesn't exist."""
    # Retry logic for initial container startup
    retries = 5
    while retries > 0:
        try:
            with get_db_cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id SERIAL PRIMARY KEY,
                        user_id TEXT,
                        channel_id TEXT,
                        text TEXT,
                        file_path TEXT,
                        timestamp TEXT,
                        CONSTRAINT unique_msg UNIQUE (channel_id, timestamp)
                    );
                """)
            logger.info("Database initialized successfully.")
            return
        except psycopg2.OperationalError:
            logger.warning("DB not ready, retrying in 2s...")
            time.sleep(2)
            retries -= 1
    logger.error("Could not initialize DB.")

# --- 3. CORE LOGIC (Reusable Functions) ---
def download_files(files_list, timestamp_prefix):
    """Downloads files and returns a semicolon-separated string of paths."""
    saved_paths = []
    headers = {'Authorization': f'Bearer {os.environ["SLACK_BOT_TOKEN"]}'}

    for f in files_list:
        url = f.get("url_private_download")
        if not url:
            continue

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
    The Single Source of Truth. 
    Handles file downloads and DB Upserts for both Sync and Real-time.
    """
    try:
        # 1. Handle Files
        file_path_string = download_files(files_list, ts) if files_list else None

        # 2. Database Upsert
        with get_db_cursor() as cur:
            cur.execute(
                """
                INSERT INTO messages (user_id, channel_id, text, file_path, timestamp) 
                VALUES (%s, %s, %s, %s, %s) 
                ON CONFLICT (channel_id, timestamp) 
                DO UPDATE SET text = EXCLUDED.text, file_path = EXCLUDED.file_path
                """,
                (user_id, channel_id, text, file_path_string, ts)
            )
        # No commit needed here; context manager handles it
    except Exception as e:
        logger.error(f"Failed to save message {ts}: {e}")

def delete_message_from_db(channel_id, ts):
    try:
        with get_db_cursor() as cur:
            cur.execute(
                "DELETE FROM messages WHERE channel_id = %s AND timestamp = %s",
                (channel_id, ts)
            )
        logger.info(f"Deleted message {ts} from DB.")
    except Exception as e:
        logger.error(f"Failed to delete message {ts}: {e}")

# --- 4. STARTUP SYNC ---
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
                
                if not messages:
                    continue

                # 1. Collect valid timestamps from Slack
                valid_slack_timestamps = set()

                for msg in messages:
                    subtype = msg.get("subtype")
                    ts = msg.get("ts")
                    
                    # Ignore system messages from the "Valid" list
                    # But don't skip the loop yet, as we need to process them if valid
                    if subtype in ["channel_join", "channel_leave", "channel_topic"]:
                        continue
                    
                    valid_slack_timestamps.add(ts)
                    
                    # Extract Data & Upsert (Create/Update)
                    user_id = msg.get("user")
                    text = msg.get("text")
                    files = msg.get("files", [])
                    
                    process_and_save_message(cid, ts, user_id, text, files)

                # --- CLEANUP LOGIC ---
                # logic: Find messages in DB that are within the time window 
                # of the batch we just fetched, but are NOT in the Slack list.
                
                oldest_fetched_ts = messages[-1]["ts"]

                with get_db_cursor() as cur:
                    # Get all DB timestamps for this channel since the oldest fetched message
                    cur.execute(
                        "SELECT timestamp FROM messages WHERE channel_id = %s AND timestamp >= %s",
                        (cid, oldest_fetched_ts)
                    )
                    db_rows = cur.fetchall()
                    
                    # Convert DB results to a Set
                    db_timestamps = {row[0] for row in db_rows}

                    # Calculate the difference: Timestamps in DB but NOT in Slack
                    deleted_timestamps = db_timestamps - valid_slack_timestamps

                    if deleted_timestamps:
                        logger.info(f"Sync Cleanup: Found {len(deleted_timestamps)} deleted messages in {channel['name']}")
                        
                        # Delete them
                        for val_ts in deleted_timestamps:
                            cur.execute(
                                "DELETE FROM messages WHERE channel_id = %s AND timestamp = %s",
                                (cid, val_ts)
                            )

            except SlackApiError as e:
                logger.error(f"Cannot sync channel {cid}: {e}")

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

    # 1. Ignore Noise
    if subtype in ["channel_join", "channel_leave", "channel_topic", "channel_purpose"]:
        logger.debug(f"Ignoring noise subtype: {subtype}")
        return

    # 2. Handle Deletion
    if subtype == "message_deleted":
        logger.debug("Processing DELETION event")
        deleted_ts = event.get("deleted_ts")
        if not deleted_ts:
            deleted_ts = event.get("previous_message", {}).get("ts")
        
        if deleted_ts:
            delete_message_from_db(channel_id, deleted_ts)
        else:
            logger.warning("Received deletion event but could not find deleted_ts")
        return

    # 3. Normalize Data (New Message vs Edit)
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

    user_id = payload.get("user")
    text = payload.get("text")
    files = payload.get("files", [])

    logger.debug(f"Extracting data -> TS: {message_ts} | User: {user_id} | Text: {text}")

    # 4. Save
    process_and_save_message(channel_id, message_ts, user_id, text, files)

# --- 6. ENTRY POINT ---
if __name__ == "__main__":
    init_db()
    sync_missing_data()
    
    # Start the Socket Mode Handler
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()