import pandas as pd
import pandas_gbq
import pydata_google_auth
# Authentification documentation: https://pandas-gbq.readthedocs.io/en/latest/howto/authentication.html
SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]

credentials = pydata_google_auth.get_user_credentials(
    SCOPES,
    # Note, this doesn't work if you're running from a notebook on a
    # remote sever, such as over SSH or with Google Colab. In those cases,
    # install the gcloud command line interface and authenticate with the
    # `gcloud auth application-default login` command and the `--no-browser`
    # option.
    auth_local_webserver=True,
)

sql = """
SELECT symbol, relative_stock  as relevant_stock
FROM `shiji-475703.invest.relative-stocks`
"""

# 运行查询并将结果存入 DataFrame
df = pd.read_gbq(sql, project_id='shiji-475703',
    credentials=credentials)

# 将结果保存回 BigQuery 另一个表（或本地 CSV）
df.to_csv('data/basic/relevant_stock.csv', encoding='utf-8', index=False)