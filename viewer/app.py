import streamlit as st
import psycopg2
import pandas as pd
import os
from datetime import date

# --- CONFIGURATION ---
st.set_page_config(layout="wide", page_title="Financial Archive")

# Standard Categories for Dropdowns
DEFAULT_CATEGORIES = ["Groceries", "Transport", "Eating Out", "Utilities", "Rent", "Entertainment", "Gifts", "Other"]
STATUS_OPTIONS = ["new", "in_progress", "confirmed", "cancelled"]

# --- DATABASE FUNCTIONS ---
def get_db_connection():
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"]
    )

def load_data():
    conn = get_db_connection()
    # Fetch all columns including the new ones
    query = """
        SELECT id, transaction_date, amount, category, description, status, text, file_path, user_id, channel_id, timestamp
        FROM messages 
        ORDER BY transaction_date DESC, timestamp DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def save_changes(edited_rows, original_df):
    """
    Writes changes from the Data Editor back to Postgres.
    edited_rows is a dict: {row_index: {col_name: new_value, ...}}
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        for index, changes in edited_rows.items():
            # Get the actual database ID from the original dataframe
            # Note: The index in edited_rows corresponds to the dataframe index
            row_id = int(original_df.loc[index, "id"])
            
            # Construct dynamic UPDATE query
            set_clauses = []
            values = []
            
            for col, new_val in changes.items():
                set_clauses.append(f"{col} = %s")
                values.append(new_val)
            
            if set_clauses:
                values.append(row_id)
                sql = f"UPDATE messages SET {', '.join(set_clauses)} WHERE id = %s"
                cur.execute(sql, tuple(values))
        
        conn.commit()
        st.success(f"✅ Saved changes to {len(edited_rows)} rows!")
    except Exception as e:
        st.error(f"Save failed: {e}")
        conn.rollback()
    finally:
        conn.close()

# --- MAIN APP UI ---

st.title("💰 Financial Archive & Validator")

# 1. Load Data
try:
    df = load_data()
    
    # Ensure types for smoother editing
    df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    df["amount"] = df["amount"].astype(float)
    
except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.stop()

# 2. Sidebar Filters
st.sidebar.header("Filters")
filter_status = st.sidebar.multiselect("Status", STATUS_OPTIONS, default=["new", "in_progress"])
filter_channel = st.sidebar.selectbox("Channel", ["All"] + list(df['channel_id'].unique()))

# Apply Filters
filtered_df = df.copy()
if filter_status:
    filtered_df = filtered_df[filtered_df['status'].isin(filter_status)]
if filter_channel != "All":
    filtered_df = filtered_df[filtered_df['channel_id'] == filter_channel]

# 3. KPI Metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Rows", len(filtered_df))
col2.metric("Total Amount (Visible)", f"€ {filtered_df['amount'].sum():,.2f}")
col3.metric("Pending ('new')", len(df[df['status'] == 'new']))

st.divider()

# 4. EDITABLE DATA GRID
st.subheader("📝 Transaction Editor")
st.caption("Double-click any cell to edit. Click 'Save Changes' at the bottom when done.")

# Configure column settings for the editor
column_config = {
    "id": st.column_config.NumberColumn(disabled=True),
    "timestamp": None, # Hide raw timestamp
    "file_path": None, # Hide file path (too long)
    "text": None,      # Hide raw text (too long, view in details)
    "user_id": None,
    "channel_id": None,
    "amount": st.column_config.NumberColumn("Amount (€)", format="%.2f"),
    "transaction_date": st.column_config.DateColumn("Date"),
    "category": st.column_config.SelectboxColumn("Category", options=DEFAULT_CATEGORIES),
    "status": st.column_config.SelectboxColumn("Status", options=STATUS_OPTIONS),
    "description": st.column_config.TextColumn("Description", width="large"),
}

# The Data Editor
edited_df = st.data_editor(
    filtered_df,
    key="data_editor",
    column_config=column_config,
    num_rows="fixed",
    hide_index=True,
    use_container_width=True
)

# 5. SAVE BUTTON LOGIC
# st.data_editor returns the *final state* of the dataframe, but usually we need specific deltas to save efficiently.
# However, fetching state from session is cleaner for simple apps.
if st.button("💾 Save Changes"):
    # Streamlit doesn't give us a simple "diff" object easily in the return value of data_editor in older versions,
    # but strictly speaking, `st.data_editor` in newer versions updates session state.
    # We will assume we need to find what changed. 
    # Actually, simpler approach for this scale: Compare edited_df with filtered_df? 
    # NO, Streamlit's `experimental_data_editor` (now `data_editor`) handles this via session state if we access it directly,
    # OR we just loop over the ID map.
    
    # Let's use the simplest robust method: 
    # We can detect changes by comparing IDs.
    
    # Optimization: Use Streamlit's built-in session state for edited rows if available, 
    # otherwise we iterate.
    
    changes = {}
    # Iterate over rows to find diffs (naive but works for <1000 rows)
    # Note: filtered_df is the source of truth BEFORE edit. edited_df is AFTER.
    
    # Be careful: indices might have reset if we ignored index. 
    # We rely on 'id' column which is unique.
    
    original_map = filtered_df.set_index('id')
    edited_map = edited_df.set_index('id')
    
    diff_count = 0
    updates = {}
    
    for rid, row in edited_map.iterrows():
        if rid not in original_map.index: continue # Should not happen
        
        orig_row = original_map.loc[rid]
        row_changes = {}
        
        # Check specific columns for changes
        for col in ["amount", "transaction_date", "category", "description", "status"]:
            new_val = row[col]
            old_val = orig_row[col]
            
            # Handle NaN/None comparison
            if pd.isna(new_val) and pd.isna(old_val): continue
            
            if new_val != old_val:
                # Convert dates to string for SQL
                if col == "transaction_date":
                    row_changes[col] = str(new_val)
                else:
                    row_changes[col] = new_val
        
        if row_changes:
            updates[rid] = row_changes
    
    if updates:
        # We need to map back to the original dataframe index for the save_changes function 
        # OR just update save_changes to handle ID directly (Better).
        
        # Refactoring save_changes to take {db_id: {col: val}} directly
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            for row_id, changes_dict in updates.items():
                set_clauses = [f"{k} = %s" for k in changes_dict.keys()]
                values = list(changes_dict.values())
                values.append(row_id)
                
                sql = f"UPDATE messages SET {', '.join(set_clauses)} WHERE id = %s"
                cur.execute(sql, tuple(values))
            conn.commit()
            st.success(f"Updated {len(updates)} records!")
            st.experimental_rerun() # Refresh data
        except Exception as e:
            st.error(f"Error: {e}")
        finally:
            conn.close()
    else:
        st.info("No changes detected.")

st.divider()

# 6. DETAILED VIEW (Media & Raw Text)
st.subheader("🔍 Detailed Inspection (Files & Raw Logs)")
for index, row in filtered_df.iterrows():
    # Only show if there is a file or if user specifically wants to see raw text
    # Using an expander per row
    label = f"{row['transaction_date']} | €{row['amount'] or 0} | {row['description'] or 'No Desc'}"
    
    with st.expander(label):
        c1, c2 = st.columns([1, 2])
        
        with c1:
            st.markdown(f"**Status:** `{row['status']}`")
            st.markdown(f"**Category:** `{row['category']}`")
            st.text_area("Original Slack Text", row['text'], height=100, key=f"txt_{row['id']}")
            
        with c2:
            if row['file_path']:
                files = row['file_path'].split(";")
                for file_p in files:
                    clean_path = file_p.replace("./downloads", "/app/downloads")
                    if os.path.exists(clean_path):
                        _, ext = os.path.splitext(clean_path)
                        ext = ext.lower()
                        
                        if ext in ['.jpg', '.jpeg', '.png', '.webp']:
                            st.image(clean_path, width=300)
                        elif ext == '.pdf':
                             with open(clean_path, "rb") as f:
                                st.download_button("📄 Download PDF", f, file_name=os.path.basename(clean_path))
                    else:
                        st.warning(f"File not found: {clean_path}")
            else:
                st.info("No attachment.")