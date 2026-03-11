import logging

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,  # DEBUG,INFO,WARNING, ERROR, CRITICAL
)

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import joblib
import os
from utils.constants import PROJECT_PATH, FUNDAMENTAL_KEY_COLS


def rank_fundamental():
    # Define paths
    input_path = f'{PROJECT_PATH}/data/ak_financial/fundamental_calculated.csv'
    industry_path = f'{PROJECT_PATH}/data/basic/stock_name_industry.csv'
    output_pred_path = f'{PROJECT_PATH}/data/ak_financial/fundamental_rank_prediction.csv'
    output_model_path = f'{PROJECT_PATH}/data/ak_financial/fundamental_rank_model.pkl'

    # Load data
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)
    industry_df = pd.read_csv(industry_path)
    df=df.merge(industry_df[['symbol','industry_type_name']], on='symbol',how='left')

    # Features configuration
    # Directions: True for "Higher is Better", False for "Lower is Better"
    directions = {
        'roe': True,
        'operating_margin': True,
        'net_profit': True,
        'free_cash_flow_conversion_rate': True,
        'netcash_operate_over_net_profit': True,
        'asset_turnover': True,
        'inventory_turnover': True,
        'TOTAL_OPERATE_INCOME_YOY': True,
        'NETPROFIT_YOY': True,
        'ev_over_ebitda': False,
        'pb_ratio': False,
        'debt_to_equity': False,
        'net_debt_over_ebitda': False
    }
    features = list(directions.keys())

    # Result dataframe
    results = []

    # Process per fiscal year
    years = df['fiscal_year'].unique()
    scalers = {}

    for year in years:
        df_year = df[df['fiscal_year'] == year].copy()

        # Process by industry within the year
        df_year_processed = []
        for industry, df_group in df_year.groupby('industry_type_name'):
            df_group = df_group.copy()

            # Replace inf with nan
            for col in features:
                df_group[col] = df_group[col].replace([np.inf, -np.inf], np.nan)

            # Fill NaN with industry-specific median
            for col in features:
                median_val = df_group[col].median()
                if pd.isna(median_val):
                    median_val = 0
                df_group[col] = df_group[col].fillna(median_val)

            X = df_group[features].copy()

            # Scale within this specific industry
            scaler = MinMaxScaler()
            X_scaled = pd.DataFrame(scaler.fit_transform(X), columns=features, index=df_group.index)

            # Adjust for direction: if Lower is Better, score = (1 - scaled_value)
            for col, higher_is_better in directions.items():
                if not higher_is_better:
                    X_scaled[col] = 1 - X_scaled[col]

            df_group['fundamental_score'] = X_scaled.sum(axis=1)
            df_year_processed.append(df_group)

        if df_year_processed:
            df_year = pd.concat(df_year_processed)
        else:
            df_year['fundamental_score'] = 0

        # Rank (Higher score is better -> Rank 1)
        # We rank across all industries in the year
        df_year['fundamental_rank'] = df_year['fundamental_score'].rank(ascending=False, method='min')

        results.append(df_year[['symbol', 'fiscal_year', 'fundamental_score', 'fundamental_rank']])

    # Combine results
    final_df = pd.concat(results, ignore_index=True)
    final_df['fundamental_score'] = final_df['fundamental_score'].round(2)

    # Sort
    final_df = final_df.sort_values(['fiscal_year', 'fundamental_rank'])

    # Save prediction
    final_df.to_csv(output_pred_path, index=False)
    print(f"Saved rankings to {output_pred_path}")

    # Save model (saving the dict of scalers)
    joblib.dump(scalers, output_model_path)
    print(f"Saved model to {output_model_path}")


if __name__ == "__main__":
    rank_fundamental()
