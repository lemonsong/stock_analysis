import requests
import os
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
from utils.constants import FEISHU_APP_ID, FEISHU_APP_KEY, FEISHU_WIKI_TOKEN

@st.cache_data(ttl=3600)  # Cache for 1 hour to avoid hitting API too often
def load_feishu_quarterly_eval_data(col_li=['symbol', 'quarterly_financial_score']):
    """
    Fetch the Quarterly Eval sheet from Feishu and return a DataFrame
    containing 'symbol' and 'quarterly_financial_score'.
    """

    if not FEISHU_APP_ID or not FEISHU_APP_KEY or not FEISHU_WIKI_TOKEN:
        print("Feishu credentials or wiki token not found in environment variables.")
        return pd.DataFrame()

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_KEY
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Failed to get Feishu token: {response.text}")
            return pd.DataFrame()

        token = response.json().get('tenant_access_token')
        if not token:
            print("Feishu tenant_access_token is empty.")
            return pd.DataFrame()

        wiki_token = FEISHU_WIKI_TOKEN
        sheet_id = "VnQJUP"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Get the actual obj_token for the wiki node
        node_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
        res = requests.get(node_url, headers=headers, timeout=10)
        if res.status_code != 200:
            print(f"Failed to get Feishu node info: {res.text}")
            return pd.DataFrame()

        obj_token = res.json().get('data', {}).get('node', {}).get('obj_token')
        if not obj_token:
            print("Feishu obj_token not found.")
            return pd.DataFrame()

        # Fetch data from the specific sheet
        data_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{obj_token}/values/{sheet_id}!A:Z"
        res = requests.get(data_url, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"Failed to get Feishu sheet data: {res.text}")
            return pd.DataFrame()

        values = res.json().get('data', {}).get('valueRange', {}).get('values', [])
        if not values:
            return pd.DataFrame()

        # Parse data into DataFrame
        header = values[0]
        data = []
        for row in values[1:]:
            # Pad row with None if it's shorter than header
            padded_row = row + [None] * (len(header) - len(row))
            data.append(padded_row[:len(header)])

        df = pd.DataFrame(data, columns=header)

        # We only need symbol and quarterly_financial_score
        if 'symbol' in df.columns and 'quarterly_financial_score' in df.columns:
            df = df[col_li].drop_duplicates(subset=["symbol"], keep='last')

            # Ensure symbol is a string (Feishu API might sometimes return rich text/lists)
            df['symbol'] = df['symbol'].apply(lambda x: str(x[0]) if isinstance(x, list) else str(x) if pd.notnull(x) else x)

            df['quarterly_financial_score'] = pd.to_numeric(df['quarterly_financial_score'], errors='coerce')
            # Remove rows where symbol is missing
            df = df.dropna(subset=['symbol'])

            if 'REPORT_DATE' in col_li:
                origin_date = datetime(1899, 12, 30)
                df['REPORT_DATE'] = df['REPORT_DATE'].map(lambda x : origin_date + timedelta(days=x))
            return df
        else:
            print("Required columns not found in Feishu sheet.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching Feishu data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_feishu_invest_data(col_li=['symbol', 'target_buy']):
    """
    Fetch the Invest sheet from Feishu and return a DataFrame
    containing 'symbol' and 'target_buy'.
    """

    if not FEISHU_APP_ID or not FEISHU_APP_KEY or not FEISHU_WIKI_TOKEN:
        print("Feishu credentials or wiki token not found in environment variables.")
        return pd.DataFrame()

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_KEY
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Failed to get Feishu token: {response.text}")
            return pd.DataFrame()

        token = response.json().get('tenant_access_token')
        if not token:
            print("Feishu tenant_access_token is empty.")
            return pd.DataFrame()

        wiki_token = FEISHU_WIKI_TOKEN
        sheet_id = "n3m1ol"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Get the actual obj_token for the wiki node
        node_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
        res = requests.get(node_url, headers=headers, timeout=10)
        if res.status_code != 200:
            print(f"Failed to get Feishu node info: {res.text}")
            return pd.DataFrame()

        obj_token = res.json().get('data', {}).get('node', {}).get('obj_token')
        if not obj_token:
            print("Feishu obj_token not found.")
            return pd.DataFrame()

        # Fetch data from the specific sheet
        data_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{obj_token}/values/{sheet_id}!A:Z"
        res = requests.get(data_url, headers=headers, timeout=15)
        if res.status_code != 200:
            print(f"Failed to get Feishu sheet data: {res.text}")
            return pd.DataFrame()

        values = res.json().get('data', {}).get('valueRange', {}).get('values', [])
        if not values:
            return pd.DataFrame()

        # Parse data into DataFrame
        header = values[0]
        data = []
        for row in values[1:]:
            # Pad row with None if it's shorter than header
            padded_row = row + [None] * (len(header) - len(row))
            data.append(padded_row[:len(header)])

        df = pd.DataFrame(data, columns=header)

        # We only need specific columns
        if all(col in df.columns for col in col_li):
            df = df[col_li].drop_duplicates(subset=["symbol"], keep='last')

            # Ensure symbol is a string (Feishu API might sometimes return rich text/lists)
            df['symbol'] = df['symbol'].apply(lambda x: str(x[0]) if isinstance(x, list) else str(x) if pd.notnull(x) else x)

            # Remove rows where symbol is missing
            df = df.dropna(subset=['symbol'])

            return df
        else:
            print("Required columns not found in Feishu sheet.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching Feishu data: {e}")
        return pd.DataFrame()

def get_feishu_token_and_obj_token(wiki_token=None):
    """Helper to get tenant_access_token and obj_token for Feishu."""
    if wiki_token is None:
        wiki_token = FEISHU_WIKI_TOKEN
    if not FEISHU_APP_ID or not FEISHU_APP_KEY or not wiki_token:
        print("Feishu credentials or wiki token not found.")
        return None, None

    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_KEY
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            print(f"Failed to get Feishu token: {response.text}")
            return None, None

        token = response.json().get('tenant_access_token')
        if not token:
            print("Feishu tenant_access_token is empty.")
            return None, None

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        node_url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={wiki_token}"
        res = requests.get(node_url, headers=headers, timeout=10)
        if res.status_code != 200:
            print(f"Failed to get Feishu node info: {res.text}")
            return None, None

        obj_token = res.json().get('data', {}).get('node', {}).get('obj_token')
        if not obj_token:
            print("Feishu obj_token not found.")
            return None, None

        return token, obj_token
    except Exception as e:
        print(f"Error getting token: {e}")
        return None, None

def append_feishu_quarterly_eval_data(symbol, relevant_stocks):
    """
    Append a row to the Quarterly Eval sheet with symbol in column B and relevant_stocks in column F.
    Returns the updated range (e.g., VnQJUP!B10:F10) or None if failed.
    """
    wiki_token = FEISHU_WIKI_TOKEN
    sheet_id = "VnQJUP"
    token, obj_token = get_feishu_token_and_obj_token(wiki_token)
    if not token or not obj_token:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Prepare data for append. Columns: A, B(symbol), C, D, E, F(relevant_stocks)
    row_data = [
        "",  # A
        symbol,  # B
        "",  # C
        "",  # D
        "",  # E
        relevant_stocks  # F
    ]

    append_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{obj_token}/values/{sheet_id}!A:Z/append"
    payload = {
        "valueRange": {
            "range": f"{sheet_id}!A:Z",
            "values": [row_data]
        }
    }

    try:
        res = requests.post(append_url, headers=headers, json=payload, timeout=10)
        if res.status_code == 200:
            # The API returns tableRange, we can extract the updated range
            # Extract row number from updates.updatedRange to know which row was added
            # Example updatedRange: "VnQJUP!A10:F10"
            data = res.json().get("data", {})
            updated_range = data.get("updates", {}).get("updatedRange")
            if updated_range:
                print(f"Successfully appended Feishu data: {updated_range}")
                return updated_range
        print(f"Failed to append Feishu data: {res.text}")
        return None
    except Exception as e:
        print(f"Error appending Feishu data: {e}")
        return None

def update_feishu_quarterly_eval_author(range_str, author="AI"):
    """
    Update column J of the specified range with the author.
    """
    if not range_str:
        return

    # Extract sheet ID and row number
    # range_str looks like VnQJUP!A10:F10
    try:
        parts = range_str.split("!")
        sheet_id = parts[0]
        cells = parts[1].split(":")
        # We assume cells[0] has the row number, e.g., A10
        import re
        match = re.search(r'\d+', cells[0])
        if not match:
            print(f"Could not extract row number from {range_str}")
            return
        row_num = match.group()
    except Exception as e:
        print(f"Error parsing range {range_str}: {e}")
        return

    wiki_token = FEISHU_WIKI_TOKEN
    token, obj_token = get_feishu_token_and_obj_token(wiki_token)
    if not token or not obj_token:
        return

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Column J is the 10th column. We want to write to J{row_num}
    target_range = f"{sheet_id}!J{row_num}:J{row_num}"
    update_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{obj_token}/values"

    payload = {
        "valueRange": {
            "range": target_range,
            "values": [
                [author]
            ]
        }
    }

    try:
        res = requests.put(update_url, headers=headers, json=payload, timeout=10)
        if res.status_code == 200:
            print(f"Successfully updated Feishu author at {target_range}")
        else:
            print(f"Failed to update Feishu author: {res.text}")
    except Exception as e:
        print(f"Error updating Feishu author: {e}")
