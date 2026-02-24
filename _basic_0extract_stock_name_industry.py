'''
A stock company list from 中国上市公司协会
Having symbol, company name, industry info
'''
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
    level=logging.INFO, # DEBUG,INFO,WARNING, ERROR, CRITICAL
)
import pdfplumber
import pandas as pd
import requests
from io import BytesIO
from utils.common import format_stock_symbol, format_df_column_name
from utils.constants import PROJECT_PATH

PROGRAM_PATH = f'{PROJECT_PATH}/data/basic'


all_data = []

# PDF 的 URL 链接
'''
method to get the stock&name&industry url:
1. open https://www.capco.org.cn/pub/zgssgsxh/xhgg/hyfl/hyfljg/index.html
2. click the first url
3. click the link for XXXX年X半年上市公司行业分类结果（按股票代码排序）.pdf 
'''

### 下载 PDF 文件 ###

# # 如果用url
# url = "https://capcofile.oss-cn-beijing.aliyuncs.com/2025/file/2025%E5%B9%B4%E4%B8%8A%E5%8D%8A%E5%B9%B4%E4%B8%8A%E5%B8%82%E5%85%AC%E5%8F%B8%E8%A1%8C%E4%B8%9A%E5%88%86%E7%B1%BB%E7%BB%93%E6%9E%9C%EF%BC%88%E6%8C%89%E8%82%A1%E7%A5%A8%E4%BB%A3%E7%A0%81%E6%8E%92%E5%BA%8F%EF%BC%89.pdf"
# response = requests.get(url)
# with pdfplumber.open(BytesIO(response.content)) as pdf:

# 如果download了文件
local_pdf_path = f'{PROGRAM_PATH}/2025年上半年上市公司行业分类结果（按股票代码排序）.pdf'
with pdfplumber.open(local_pdf_path) as pdf:

    for page in pdf.pages:
        table = page.extract_table()
        if table:
            # 每一页提取出的 table 是一个列表的列表
            # 对于第一页，跳过第一行的表名，对于之后的页，跳过第一行的列名
            all_data.extend(table[1:])

# 转换为 DataFrame (假设第一行是表头)
stock_name_industry_df = pd.DataFrame(all_data[1:], columns=all_data[0])
# 数据清洗：去除单元格内多余的换行符（PDF 提取常见问题）
stock_name_industry_df = stock_name_industry_df.replace('\n', '', regex=True)
# rename columns
stock_name_industry_df  = format_df_column_name(stock_name_industry_df)
# format stock symbol
logging.info(f"Formatting stock symbol")
stock_name_industry_df['symbol'] = stock_name_industry_df['symbol'].map(lambda x: format_stock_symbol(x,'number','MARKETnumber'))

logging.info(f"成功提取 {len(stock_name_industry_df)} 行数据")
logging.info(f'{stock_name_industry_df.head()}')
stock_name_industry_df.to_csv(f'{PROGRAM_PATH}/stock_name_industry.csv', index=False, encoding='utf-8')

# # 读取 PDF 中的所有表格
# # pages='all' 表示读取所有页面
# dfs = tabula.read_pdf(url, pages='all', multiple_tables=True)
#
# # 将提取到的所有 DataFrame 合并成一个（如果表格是跨页连贯的）
# if dfs:
#     stock_name_industry_df = pd.concat(dfs, ignore_index=True)
#     logging.info(f'{stock_name_industry_df.head()=}')
#     stock_name_industry_df.to_csv(f'{PROGRAM_PATH}/stock_name_industry.csv', index=False, encoding='utf-8')