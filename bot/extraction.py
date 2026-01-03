import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

# --- CONSTANTS ---

# Map Dutch month names/abbr to numbers
# Handles 'mrt' (standard abbr) and full names.
DUTCH_MONTHS = {
    'jan': 1, 'januari': 1,
    'feb': 2, 'februari': 2,
    'mrt': 3, 'maart': 3,
    'apr': 4, 'april': 4,
    'mei': 5,
    'jun': 6, 'juni': 6,
    'jul': 7, 'juli': 7,
    'aug': 8, 'augustus': 8,
    'sep': 9, 'september': 9,
    'okt': 10, 'oktober': 10,
    'nov': 11, 'november': 11,
    'dec': 12, 'december': 12
}

# --- REGEX PATTERNS ---

# Amount Regex
# Strategy: Look for the Euro symbol, before or after a number.
# The number group is loose ([\d.,]+) to capture "1.000,00" or "10.50". 
# We clean it later in the function.
# Group 1: Amount if € is prefix (e.g. € 100)
# Group 2: Amount if € is suffix (e.g. 100 €)
AMOUNT_PATTERN = re.compile(
    r'(?:€\s*)([\d.,]+)|([\d.,]+)(?:\s*€)'
)

# Date Regex 1: Numeric (01/01/2024, 1-1-24, 1.1.2024)
# Matches: start of word, 1-2 digits, separator, 1-2 digits, separator, 2 or 4 digits
DATE_NUMERIC_PATTERN = re.compile(
    r'\b(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})\b'
)

# Date Regex 2: Dutch Textual (12 jan 2024, 1 januari 24)
# Matches: 1-2 digits, space, month name (case insensitive), space, 2 or 4 digits
# We use a broad match for the month string and validate it against the dictionary later
DATE_TEXTUAL_PATTERN = re.compile(
    r'\b(\d{1,2})\s+([a-zA-Z]+)\s+(\d{2,4})\b',
    re.IGNORECASE
)

# --- PARSING FUNCTIONS ---

def parse_amount(text):
    """
    Extracts amount in Euros.
    Handles:
    - 10,50 (Comma decimal)
    - 10.50 (Dot decimal)
    - 1000 (Integer)
    - 1.000,00 (European thousands)
    - 1,000.00 (US thousands - rare in this context but handled if possible)
    """
    if not text:
        return None
    
    match = AMOUNT_PATTERN.search(text)
    if not match:
        return None

    # Get the raw number string (from prefix or suffix group)
    raw_val = match.group(1) or match.group(2)
    
    # --- Smart Cleaning Logic ---
    # 1. Check for standard European format: "1.250,50" -> Remove dots, swap comma to dot
    if '.' in raw_val and ',' in raw_val:
        if raw_val.rfind(',') > raw_val.rfind('.'):
            # Assumed European: 1.000,50
            clean_val = raw_val.replace('.', '').replace(',', '.')
        else:
            # Assumed US: 1,000.50
            clean_val = raw_val.replace(',', '')
    
    # 2. Check for only Comma: "10,50" or "10,5"
    elif ',' in raw_val:
        clean_val = raw_val.replace(',', '.')
        
    # 3. Check for only Dot: "10.50" or "1000" (ambiguous)
    # If it has 3 decimal places (1.000), it's likely a thousand separator in EU context,
    # UNLESS it is a small number like 1.250 (could be 1 euro 25). 
    # Assumption for this bot: 
    # If dot is present, and we have 1 or 2 digits after it -> Decimal
    # If 3 digits after it -> Likely thousands, but risky. 
    # SAFEST BET: Treat dot as decimal if no comma exists, unless specific logic applies.
    # Given the prompt examples "10.50", we treat single dot as decimal.
    else:
        clean_val = raw_val

    try:
        return Decimal(clean_val)
    except (InvalidOperation, ValueError):
        return None

def parse_date(text):
    """
    Extracts a date and returns a YYYY-MM-DD string.
    Tries Numeric first, then Dutch Textual.
    """
    if not text:
        return None, False

    # 1. Try Numeric: 01/01/2024
    match = DATE_NUMERIC_PATTERN.search(text)
    if match:
        d, m, y = match.groups()
        return _format_date(d, m, y)

    # 2. Try Textual: 12 jan 2024
    match = DATE_TEXTUAL_PATTERN.search(text)
    if match:
        d, month_str, y = match.groups()
        
        # Resolve Dutch Month
        month_lower = month_str.lower()
        # Check full match or if the key is contained (for safety)
        month_num = DUTCH_MONTHS.get(month_lower)
        
        if month_num:
            return _format_date(d, str(month_num), y)
            
    return None, False

def _format_date(d, m, y):
    """Helper to validate and format date parts into YYYY-MM-DD"""
    # Normalize Year (2-digit -> 20xx)
    if len(y) == 2:
        y = "20" + y
    
    # Pad Day/Month
    d = d.zfill(2)
    m = m.zfill(2)
    
    try:
        # Validate calendar logic (e.g. catch 30 feb)
        dt_obj = datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
        return dt_obj.strftime("%Y-%m-%d"), True
    except ValueError:
        return None, False

def parse_description(text):
    """
    Removes the Date and the Amount from the text and returns the rest.
    """
    if not text:
        return ""
    
    # Remove Amount (replace with space)
    clean_text = AMOUNT_PATTERN.sub(' ', text)
    
    # Remove Date Numeric
    clean_text = DATE_NUMERIC_PATTERN.sub(' ', clean_text)
    
    # Remove Date Textual
    clean_text = DATE_TEXTUAL_PATTERN.sub(' ', clean_text)
    
    # Clean up whitespace (newlines to spaces, strip edges)
    clean_text = " ".join(clean_text.split())
    
    return clean_text

def extract_transaction_data(text, message_ts):
    """
    Main entry point. Returns a dict of fields for the DB.
    """
    amount = parse_amount(text)
    extracted_date_str, is_extracted = parse_date(text)
    
    # Default transaction_date is the Slack timestamp if regex fails
    if is_extracted:
        transaction_date = extracted_date_str
    else:
        # Convert Slack TS (unix) to YYYY-MM-DD
        transaction_date = datetime.fromtimestamp(float(message_ts)).strftime("%Y-%m-%d")
    
    description = parse_description(text)

    return {
        "amount": amount,
        "transaction_date": transaction_date,
        "date_extracted": is_extracted,
        "description": description
    }