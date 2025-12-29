import streamlit as st
import psycopg2
import pandas as pd
import os

# Connect to the database
def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"]
    )
    return conn

st.set_page_config(layout="wide", page_title="Financial Archive")
st.title("📂 Slack Financial Archive")

# 1. Load Data
df = pd.DataFrame() # Create empty DF so app doesn't crash later if DB fails

try:
    print("--- Attempting to connect to DB... ---") # Debug print
    conn = get_db_connection()
    print("--- Connected! Fetching data... ---")    # Debug print
    
    # SQL query to get data sorted by newest first
    query = "SELECT id, timestamp, user_id, channel_id, text, file_path FROM messages ORDER BY timestamp DESC"
    
    # Pandas often needs a specific engine hint, but let's try standard first
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"--- Success! Loaded {len(df)} rows. ---") # Debug print

except Exception as e:
    # PRINT THE ERROR TO THE TERMINAL SO WE CAN SEE IT
    print(f"CRITICAL ERROR: {e}") 
    st.error(f"Could not connect to database: {e}")

# 2. Add Filters
col1, col2 = st.columns(2)
with col1:
    search_term = st.text_input("🔍 Search Text")
with col2:
    selected_channel = st.selectbox("Filter by Channel", ["All"] + list(df['channel_id'].unique()))

if search_term:
    df = df[df['text'].str.contains(search_term, case=False, na=False)]
if selected_channel != "All":
    df = df[df['channel_id'] == selected_channel]

st.markdown(f"**Found {len(df)} records**")

# 3. Display Data with Smart Media Detection
for index, row in df.iterrows():
    with st.expander(f"{row['timestamp']} - {row['text'][:50]}..."):
        st.markdown(f"**User:** `{row['user_id']}` | **Channel:** `{row['channel_id']}`")
        st.info(row['text'])
        
        if row['file_path']:
            # Split by semicolon in case of multiple files
            files = row['file_path'].split(";")
            
            for file_p in files:
                # Fix path mapping (Bot uses ./downloads, Viewer uses /app/downloads)
                clean_path = file_p.replace("./downloads", "/app/downloads")
                
                if os.path.exists(clean_path):
                    # Get file extension (e.g., '.jpg', '.mp3')
                    _, ext = os.path.splitext(clean_path)
                    ext = ext.lower()

                    # --- MEDIA TYPE LOGIC ---
                    if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                        st.image(clean_path, caption=os.path.basename(clean_path), width=400)
                        
                    elif ext in ['.mp3', '.wav', '.ogg', '.m4a', '.flac']:
                        st.audio(clean_path)
                        st.caption(f"🎧 Audio: {os.path.basename(clean_path)}")
                        
                    elif ext in ['.mp4', '.mov', '.avi', '.webm', '.mkv']:
                        st.video(clean_path)
                        st.caption(f"🎬 Video: {os.path.basename(clean_path)}")
                        
                    elif ext in ['.pdf']:
                        # Streamlit cannot render PDFs inline easily, so we give a download button
                        with open(clean_path, "rb") as f:
                            st.download_button(
                                label=f"📄 Download PDF: {os.path.basename(clean_path)}",
                                data=f,
                                file_name=os.path.basename(clean_path),
                                mime="application/pdf"
                            )
                    else:
                        # Fallback for CSVs, Zips, etc.
                        with open(clean_path, "rb") as f:
                            st.download_button(
                                label=f"⬇️ Download File: {os.path.basename(clean_path)}",
                                data=f,
                                file_name=os.path.basename(clean_path)
                            )
                else:
                    st.warning(f"⚠️ File missing from disk: {clean_path}")