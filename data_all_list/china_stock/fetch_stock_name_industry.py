import tabula
import pandas as pd

# PDF 的 URL 链接
url = "https://capcofile.oss-cn-beijing.aliyuncs.com/2025/file/2025%E5%B9%B4%E4%B8%8A%E5%8D%8A%E5%B9%B4%E4%B8%8A%E5%B8%82%E5%85%AC%E5%8F%B8%E8%A1%8C%E4%B8%9A%E5%88%86%E7%B1%BB%E7%BB%93%E6%9E%9C%EF%BC%88%E6%8C%89%E8%82%A1%E7%A5%A8%E4%BB%A3%E7%A0%81%E6%8E%92%E5%BA%8F%EF%BC%89.pdf"

# 读取 PDF 中的所有表格
# pages='all' 表示读取所有页面
dfs = tabula.read_pdf(url, pages='all', multiple_tables=True)

# 将提取到的所有 DataFrame 合并成一个（如果表格是跨页连贯的）
if dfs:
    full_df = pd.concat(dfs, ignore_index=True)
    print(full_df.head())