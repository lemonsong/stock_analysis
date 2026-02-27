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
from utils.constants import PROJECT_PATH


def rank_fundamental():
    # Define paths
    input_path = f'{PROJECT_PATH}/data/ak_fundamental/fundamental_calculated_metrics.csv'
    output_pred_path = f'{PROJECT_PATH}/data/ak_fundamental/fundamental_rank_prediction.csv'
    output_model_path = f'{PROJECT_PATH}/data/ak_fundamental/fundamental_rank_model.pkl'

    # Load data
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)
    
    # Features configuration
    features = ['roe', 'netcash_operate_over_net_profit', 'debt_to_asset', 'inventory_turnover', 'ev_over_ebitda']
    # Directions: True for "Higher is Better", False for "Lower is Better"
    directions = {
        'roe': True,
        'netcash_operate_over_net_profit': True,
        'debt_to_asset': False, 
        'inventory_turnover': True,
        'ev_over_ebitda': False
    }

    # Result dataframe
    results = []
    
    # Process per fiscal year
    years = df['fiscal_year'].unique()
    scalers = {}

    for year in years:
        df_year = df[df['fiscal_year'] == year].copy()
        
        # Handle missing values: Fill with median of the year, then 0
        for col in features:
            median_val = df_year[col].median()
            if pd.isna(median_val):
                median_val = 0
            df_year[col] = df_year[col].fillna(median_val)
        
        # Prepare data for scaling
        X = df_year[features].copy()
        
        # Invert "Lower is Better" features so that higher value becomes better (for the score)
        # We can negate them before scaling, or just subtract the scaled value later.
        # Let's negate them here so StandardScaler treats them correctly (higher original -> lower negated -> lower score, wait.)
        # If lower is better (e.g. debt 10 vs 20), we want 10 to score higher.
        # If we Negate: -10 vs -20. -10 is higher than -20. So Higher negated value is better.
        # So we can just negate "Lower is Better" features and then sum everything.

        # 检查是否有无穷大
        logging.info(f"Contains inf: {np.isinf(X).values.any()}")
        # 检查是否有空值
        logging.info(f"Contains NaN: {X.isnull().values.any()}")
        # 查找具体哪一列有问题
        logging.info(X.replace([np.inf, -np.inf], np.nan).isnull().sum())

        for col, higher_is_better in directions.items():
            if not higher_is_better:
                X[col] = X[col] * -1
            # 所有小于 0 的值替换为 0
            X[col] = X[col].clip(lower=0)
            # 1. 将 inf 替换为 NaN，以便计算最大值
            temp_series = X[col].replace([np.inf, -np.inf], np.nan)
            # 2. 获取该列的最大非无穷数值
            max_val = temp_series.max()
            X[col] = X[col].replace([np.inf], max_val)

        #
        logging.info("after preprocess")
        # 查找具体哪一列有问题
        logging.info(X.replace([np.inf, -np.inf], np.nan).isnull().sum())



        # Scale
        scaler = MinMaxScaler()
        X_scaled = scaler.fit_transform(X)
        scalers[year] = scaler
        
        # Calculate Score (Sum of scaled features)
        # You might want weights, but equal weights is a standard starting point
        df_year['fundamental_score'] = X_scaled.sum(axis=1)
        
        # Rank (Higher score is better -> Rank 1)
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
