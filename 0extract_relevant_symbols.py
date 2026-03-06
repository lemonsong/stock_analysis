'''
TO REVIEW
'''
import os
import pandas as pd
from pathlib import Path
import google.generativeai as genai
from time import sleep


def extract_relevant_symbols():
    app_decision_path = Path('data/dwa/app_decision.csv')
    market_cap_path = Path('data/ak_fundamental/daily_market_cap.csv')
    output_path = Path('data/basic/relevant_symbol.csv')

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not app_decision_path.exists():
        print(f"Error: {app_decision_path} not found.")
        return

    df = pd.read_csv(app_decision_path)
    print(f"Loaded {len(df)} symbols from app_decision.csv")

    # Check for market cap
    if market_cap_path.exists():
        mc_df = pd.read_csv(market_cap_path)
        # get latest market cap per symbol
        mc_df['date'] = pd.to_datetime(mc_df['date'])
        latest_mc = mc_df.loc[mc_df.groupby('symbol')['date'].idxmax()]

        df = pd.merge(df, latest_mc[['symbol', 'market_cap']], on='symbol', how='left')

        # Sort by market_cap
        if 'market_cap' in df.columns:
            df = df.sort_values(by='market_cap', ascending=False)
            print("Sorted by market_cap.")
        else:
            print("Warning: market_cap column not found in merge result.")
    else:
        print(
            f"Warning: {market_cap_path} not found. Proceeding without market cap sort (or fallback to close if desired, but user asked for market cap).")
        # In actual usage, daily_market_cap should be present.
        # Just sort by index or whatever.

    symbols_to_process = df['symbol'].dropna().unique()

    # Setup Gemini LLM
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Warning: GEMINI_API_KEY is not set. Generating mock data for testing.")

        results = []
        for symbol in symbols_to_process:
            # Mock relevant symbols based on instructions
            mock_rel = f"{symbol},SH688433,SH603308,SZ000969"
            results.append({'symbol': symbol, 'relevant_symbol': mock_rel})

        res_df = pd.DataFrame(results)
        res_df.to_csv(output_path, index=False)
        print(f"Saved mock relevant symbols to {output_path}")
        return

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-2.5-flash')

    results = []
    print(f"Processing {len(symbols_to_process)} symbols...")

    for i, symbol in enumerate(symbols_to_process):
        prompt = f"Look over competitors and supply chain for the stock {symbol}. Return a comma-separated list of relevant stock symbols (like SH688333,SH688433,SH603308,SH688456,SZ000969,SZ300337,SZ300227). Only output the comma-separated list, nothing else."

        try:
            response = model.generate_content(prompt)
            rel_symbols = response.text.strip()
            # Basic validation
            if " " in rel_symbols or "\n" in rel_symbols:
                # LLM didn't follow perfectly, do a quick clean
                import re
                symbols_found = re.findall(r'[A-Z]{2}[0-9]{6}', rel_symbols)
                if symbols_found:
                    rel_symbols = ",".join(symbols_found)

            results.append({'symbol': symbol, 'relevant_symbol': rel_symbols})
            print(f"[{i + 1}/{len(symbols_to_process)}] {symbol} -> {rel_symbols}")
            sleep(1)  # Simple rate limit mitigation
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    if results:
        res_df = pd.DataFrame(results)
        res_df.to_csv(output_path, index=False)
        print(f"Saved {len(res_df)} relevant symbols to {output_path}")


if __name__ == "__main__":
    extract_relevant_symbols()
