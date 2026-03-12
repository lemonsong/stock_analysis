import logging
import os
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
import joblib

from utils.constants import PROJECT_PATH
from utils.feishu_helper import load_feishu_quarterly_eval_data

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO,
)

def get_feature_directions():
    return {
        'TOTAL_OPERATE_INCOME_YOY': True,
        'NETPROFIT_YOY': True,
        'TOTAL_ASSETS_YOY': True,
        'ROE': True,
        'NETCASH_OVER_NETPROFIT': True,
        'TOTAL_ASSETS': True,
        'TOTAL_LIABILITIES': False,
        'ACCOUNTS_RECE_YOY': False,
        'INVENTORY_YOY': False,
        'DEBT_TO_ASSET': False,
    }

def standardize_symbol(sym):
    return sym

def main():
    single_file_dir = Path(PROJECT_PATH) / 'data' / 'ak_financial' / 'single_file'
    industry_path = Path(PROJECT_PATH) / 'data' / 'basic' / 'stock_name_industry.csv'

    if not single_file_dir.exists():
        logging.error(f"Single file directory not found: {single_file_dir}")
        return

    logging.info("Loading Feishu labels...")
    feishu_df = load_feishu_quarterly_eval_data()
    if feishu_df.empty:
        logging.error("Failed to load Feishu labels or it is empty.")
        return

    feishu_df['symbol'] = feishu_df['symbol'].astype(str).apply(standardize_symbol)

    logging.info("Processing single financial files...")
    stock_files = {}
    for f in single_file_dir.glob('*.csv'):
        filename = f.name
        if filename.endswith('_balance.csv'):
            sym = filename.replace('_balance.csv', '')
            stock_files.setdefault(sym, {})['balance'] = f
        elif filename.endswith('_profit.csv'):
            sym = filename.replace('_profit.csv', '')
            stock_files.setdefault(sym, {})['profit'] = f
        elif filename.endswith('_cash_flow.csv'):
            sym = filename.replace('_cash_flow.csv', '')
            stock_files.setdefault(sym, {})['cash_flow'] = f

    data_rows = []

    for sym, fpaths in stock_files.items():
        row = {'symbol_raw': sym}
        symbol = standardize_symbol(sym)
        row['symbol'] = symbol

        valid = True
        if 'balance' in fpaths:
            df_bal = pd.read_csv(fpaths['balance'])
            df_bal = df_bal[df_bal['REPORT_DATE'].str.startswith('2025-09-30')]
            if not df_bal.empty:
                for col in ['TOTAL_ASSETS', 'TOTAL_LIABILITIES', 'TOTAL_EQUITY', 'TOTAL_ASSETS_YOY']:
                    row[col] = df_bal.iloc[0].get(col, np.nan)
            else:
                valid = False
        else:
            valid = False

        if 'profit' in fpaths:
            df_prof = pd.read_csv(fpaths['profit'])
            df_prof = df_prof[df_prof['REPORT_DATE'].str.startswith('2025-09-30')]
            if not df_prof.empty:
                for col in ['TOTAL_OPERATE_INCOME', 'NETPROFIT', 'TOTAL_OPERATE_INCOME_YOY', 'NETPROFIT_YOY']:
                    row[col] = df_prof.iloc[0].get(col, np.nan)
            else:
                valid = False
        else:
            valid = False

        if 'cash_flow' in fpaths:
            df_cf = pd.read_csv(fpaths['cash_flow'])
            df_cf = df_cf[df_cf['REPORT_DATE'].str.startswith('2025-09-30')]
            if not df_cf.empty:
                for col in ['NETCASH_OPERATE']:
                    row[col] = df_cf.iloc[0].get(col, np.nan)
            else:
                valid = False
        else:
            valid = False

        if valid:
            data_rows.append(row)

    df_financial = pd.DataFrame(data_rows)
    if df_financial.empty:
        logging.error("No valid financial data for 2025-09-30 found.")
        return

    logging.info("Merging industry data...")
    industry_df = pd.read_csv(industry_path)

    industry_df['symbol'] = industry_df['symbol'].astype(str).apply(standardize_symbol)
    df_financial = df_financial.merge(industry_df[['symbol', 'industry_type_name']], on='symbol', how='left')

    # Fill NaN for industry
    df_financial['industry_type_name'] = df_financial['industry_type_name'].fillna('unknown')

    df_financial['ROE'] = df_financial['NETPROFIT'] / df_financial['TOTAL_EQUITY']
    df_financial['DEBT_TO_ASSET'] = df_financial['TOTAL_LIABILITIES'] / df_financial['TOTAL_ASSETS']
    df_financial['NETCASH_OVER_NETPROFIT'] = df_financial['NETCASH_OPERATE'] / df_financial['NETPROFIT']

    numeric_features = [
        'TOTAL_ASSETS_YOY', 'TOTAL_OPERATE_INCOME_YOY', 'NETPROFIT_YOY',
        'ROE', 'DEBT_TO_ASSET', 'NETCASH_OVER_NETPROFIT'
    ]

    directions = get_feature_directions()
    for feature in numeric_features:
        if feature in directions and not directions[feature]:
            df_financial[feature] = -df_financial[feature]

    # Future warning handled by not using replace inplace for inf
    for col in numeric_features:
        df_financial[col] = df_financial[col].replace([np.inf, -np.inf], np.nan)

    df_train = df_financial.merge(feishu_df, on='symbol', how='inner')
    logging.info(f"Training data size after merging labels: {len(df_train)}")

    X_train = df_train[numeric_features + ['industry_type_name']]
    y_train = df_train['quarterly_financial_score']

    if len(df_train) == 0:
        logging.error("No training data available. Make sure Feishu data and single file symbols match.")
        return

    logging.info("Training models...")
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])

    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='constant', fill_value='unknown')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, ['industry_type_name'])
        ])

    dt_model = Pipeline(steps=[('preprocessor', preprocessor),
                               ('regressor', DecisionTreeRegressor(random_state=42))])
    dt_model.fit(X_train, y_train)

    rf_model = Pipeline(steps=[('preprocessor', preprocessor),
                               ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))])
    rf_model.fit(X_train, y_train)

    svr_model = Pipeline(steps=[('preprocessor', preprocessor),
                                ('regressor', SVR())])
    svr_model.fit(X_train, y_train)

    X_all = df_financial[numeric_features + ['industry_type_name']]
    df_financial['dt_pred'] = dt_model.predict(X_all)
    df_financial['rf_pred'] = rf_model.predict(X_all)
    df_financial['svr_pred'] = svr_model.predict(X_all)

    df_financial['latest_financial_score'] = df_financial[['dt_pred', 'rf_pred', 'svr_pred']].mean(axis=1)

    df_out = df_financial.merge(feishu_df[['symbol', 'quarterly_financial_score']], on='symbol', how='left')

    output_dir = Path(PROJECT_PATH) / 'data' / 'ak_financial' / 'scoring_model'
    output_dir.mkdir(parents=True, exist_ok=True)

    df_out.to_csv(output_dir / 'financial_score_data.csv', index=False)
    joblib.dump(dt_model, output_dir / 'financial_score_DT.pkl')
    joblib.dump(rf_model, output_dir / 'financial_score_RF.pkl')
    joblib.dump(svr_model, output_dir / 'financial_score_SVR.pkl')

    logging.info("Modeling complete and results saved.")

if __name__ == "__main__":
    main()
