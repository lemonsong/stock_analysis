import requests
import os
import pandas as pd
import streamlit as st
from utils.constants import FEISHU_APP_ID, FEISHU_APP_KEY

@st.cache_data(ttl=3600)  # Cache for 1 hour to avoid hitting API too often
def load_feishu_quarterly_eval_data():
    """
    Fetch the Quarterly Eval sheet from Feishu and return a DataFrame
    containing 'symbol' and 'quarterly_financial_score'.
    """

    if not FEISHU_APP_ID or not FEISHU_APP_KEY:
        print("Feishu credentials not found in environment variables.")
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

        wiki_token = "FokmwwNLbigzF7km6mEcq3Smnjb"
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
            df = df[['symbol', 'quarterly_financial_score']].drop_duplicates(subset=["symbol"], keep='last')

            # Ensure symbol is a string (Feishu API might sometimes return rich text/lists)
            df['symbol'] = df['symbol'].apply(lambda x: str(x[0]) if isinstance(x, list) else str(x) if pd.notnull(x) else x)

            df['quarterly_financial_score'] = pd.to_numeric(df['quarterly_financial_score'], errors='coerce')
            # Remove rows where symbol is missing
            df = df.dropna(subset=['symbol'])
            return df
        else:
            print("Required columns not found in Feishu sheet.")
            return pd.DataFrame()

    except Exception as e:
        print(f"Error fetching Feishu data: {e}")
        return pd.DataFrame()
