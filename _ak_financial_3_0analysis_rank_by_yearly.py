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
    # TODO: add metrics used in dashboard
    # add metrics in Xueqiu dashboard
    # calculate YOY as features
    # examine calculation
    return {
        # 关键指标
        "TOTAL_OPERATE_INCOME": True,  # 营业收入
        "TOTAL_OPERATE_INCOME_YOY": True,  # 营业收入同比增长
        "NETPROFIT": True,  # 净利润
        "NETPROFIT_YOY": True,  # 净利润同比增长
        "DEDUCT_PARENT_NETPROFIT": True,  # 扣非净利润
        "DEDUCT_PARENT_NETPROFIT_YOY": True,  # 扣非净利润同比增长
        # 每股指标
        "BASIC_EPS": True,  # 每股收益
        # 每股净资产
        # 每股资本公积金
        # 每股未分配利润
        # 每股经营现金流

        # 盈利能力
        "roe": True,  # 净资产收益率
        # 净资产收益率 - 摊薄
        "roa": True,  # 总资产报酬率
        # 人力投入回报率
        "gross_margin": True,  # 销售毛利率
        "profit_margin": True,  # 销售净利率

        # 财务风险
        "debt_to_asset": False,  # 资产负债率 (通常越低财务风险越小)
        "current_ratio": True,  # 流动比率
        "quick_ratio": True,  # 速动比率
        # 权益乘数
        "debt_to_equity": False,  # 产权比率 (越低代表长期偿债能力越强)
        # 股东权益比率
        # 现金流量比率

        # 运营能力
        # 现金循环周期
        # 营业周期
        "asset_turnover": True,  # 总资产周转率
        "inventory_turnover": True,  # 存货周转率
        "receivables_turnover": True,  # 应收账款周转率
        # 应付账款周转率
        # 流动资产周转率
        # 固定资产周转率

        # My features
    #     "free_cash_flow_conversion_rate": True,
    # "netcash_operate_over_net_profit": True,
    # "net_debt_over_ebitda": True,

        #
        # 'TOTAL_OPERATE_INCOME_YOY': True,
        # 'NETPROFIT_YOY': True,
        # 'TOTAL_ASSETS_YOY': True,
        # 'ROE': True,
        # 'NETCASH_OVER_NETPROFIT': True,
        # 'TOTAL_ASSETS': True,
        # 'TOTAL_LIABILITIES': False,
        # 'ACCOUNTS_RECE_YOY': False,
        # 'INVENTORY_YOY': False,
        # 'DEBT_TO_ASSET': False,
    }


def main():
    financial_path = Path(PROJECT_PATH) / 'data' / 'ak_financial' / 'financial_calculated.csv'
    industry_path = Path(PROJECT_PATH) / 'data' / 'basic' / 'stock_name_industry.csv'
    # report_date_for_pred_str = '2025-12-31'
    feature_direction_dict = get_feature_directions()
    numeric_feature_li = list(feature_direction_dict.keys())

    # create dataset
    financial_df = pd.read_csv(financial_path)
    col_financial_df = financial_df.columns.tolist()
    col_financial_df_non_financial_metrics = ['symbol', 'SECURITY_NAME_ABBR', 'fiscal_year', 'ORG_TYPE', 'REPORT_DATE',
                                              'REPORT_TYPE', 'REPORT_DATE_NAME', 'industry_type_name']
    col_industry = 'industry_type_name'
    financial_df['REPORT_DATE'] = pd.to_datetime(financial_df['REPORT_DATE'])

    logging.info("Loading and merging industry data...")
    industry_df = pd.read_csv(industry_path)
    financial_df = financial_df.merge(industry_df[['symbol', col_industry]], on='symbol', how='left')

    logging.info("Loading and merging Feishu labels...")
    feishu_df = load_feishu_quarterly_eval_data(col_li=['symbol', 'quarterly_financial_score','REPORT_DATE'])
    if feishu_df.empty:
        logging.error("Failed to load Feishu labels or it is empty.")
        return
    financial_df = financial_df.merge(feishu_df, on=['symbol','REPORT_DATE'],how='left')
    logging.info(f"Is in financial_df:{len(financial_df[financial_df.symbol=='SZ000050'])}")
    # # filter to certain rows
    # financial_df = financial_df.loc[
    #     (~pd.isna(financial_df.quarterly_financial_score))
    #     |
    #     (financial_df['REPORT_DATE'] == pd.to_datetime(report_date_for_pred_str)),
    #     ['symbol', 'REPORT_DATE', 'quarterly_financial_score', col_industry]+numeric_feature_li
    # ]

    logging.info("Preprocessing ...")
    # Fill NaN for industry
    # financial_df[col_industry] = financial_df[col_industry].fillna('unknown')
    # Format numeric feature columns

    # for feature in numeric_feature_li:
    #     financial_df[feature] = financial_df[feature].replace([np.inf, -np.inf], np.nan)
    #     # Deal with features which have high value means worse financial status
    #     if feature in feature_direction_dict and not feature_direction_dict[feature]:
    #         financial_df[feature] = -financial_df[feature]

    logging.info("Generating train and test data...")
    # 【done】TODO: the training data should use the 2025-09-30 report and calculation in ak_financial_2_2. As then went by,less and less data in 'financial_calculated.csv' having report_date=2025-09-30
    train_df = financial_df.loc[~pd.isna(financial_df.quarterly_financial_score)].copy()
    logging.info(f"Length of train_df:{len(train_df)}")

    # Predict for all quarters starting from 2025-12-31
    pred_df = financial_df[financial_df['REPORT_DATE'] >= pd.to_datetime('2025-12-31')].copy()
    logging.info(f"Is in pred_df:{len(pred_df[pred_df.symbol=='SZ000050'])}")

    X_train = train_df[numeric_feature_li + [col_industry]].copy()
    y_train = train_df['quarterly_financial_score']
    X_pred = pred_df[numeric_feature_li + [col_industry]].copy()


    if pred_df.empty:
        logging.error(f"No valid pred data found.")
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
            ('num', numeric_transformer, numeric_feature_li),
            ('cat', categorical_transformer, [col_industry])
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

    pred_df['dt_pred'] = dt_model.predict(X_pred)
    pred_df['rf_pred'] = rf_model.predict(X_pred)
    pred_df['svr_pred'] = svr_model.predict(X_pred)

    pred_df['model_financial_score'] = pred_df[['dt_pred', 'rf_pred', 'svr_pred']].mean(axis=1)
    pred_df['model_financial_score'] = pred_df['model_financial_score'].clip(0, 5)

    # Calculate is_latest
    max_dates = pred_df.groupby('symbol')['REPORT_DATE'].transform('max')
    pred_df['is_latest'] = pred_df['REPORT_DATE'] == max_dates

    # Rename variables and format output
    pred_df.rename(columns={'quarterly_financial_score': 'train_financial_score', 'REPORT_DATE': 'report_date'}, inplace=True)
    df_out = pred_df[['symbol', 'report_date', 'model_financial_score', 'train_financial_score', 'is_latest']]

    # save prediction
    output_dir = Path(PROJECT_PATH) / 'data' / 'ak_financial' / 'scoring_model'
    output_dir.mkdir(parents=True, exist_ok=True)

    df_out.to_csv(output_dir / 'financial_score_data.csv', index=False)
    joblib.dump(dt_model, output_dir / 'financial_score_DT.pkl')
    joblib.dump(rf_model, output_dir / 'financial_score_RF.pkl')
    joblib.dump(svr_model, output_dir / 'financial_score_SVR.pkl')

    logging.info("Modeling complete and results saved.")

if __name__ == "__main__":
    main()
