import streamlit as st
from utils.cache import clear_cache, clean_expired_cache

# 页面配置只在主文件设置一次
st.set_page_config(
    page_title="投资监测分析平台",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 侧边栏导航和设置
st.sidebar.title("💰 投资监测分析平台")
st.sidebar.markdown("---")

# 页面导航（Streamlit会自动识别pages目录下的文件）
st.sidebar.markdown("### 📑 页面导航")
st.sidebar.markdown("""
- 🌍 [世界主要资产](1_world_assets)
- 📊 [A股买卖信号](2_ashare_signals)
- 📈 [A股基本面分析](3_fundamental_analysis)
- 🤖 [AI投资顾问](4_llm_advisor)
""")

st.sidebar.markdown("---")

# 全局设置
st.sidebar.markdown("### ⚙️ 全局设置")

# 缓存管理
st.sidebar.markdown("#### 缓存管理")
if st.sidebar.button("🔄 清理过期缓存", use_container_width=True):
    clean_expired_cache()
    st.sidebar.success("已清理过期缓存")

if st.sidebar.button("🗑️ 清除所有缓存", use_container_width=True):
    clear_cache()
    st.sidebar.success("已清除所有缓存")

st.sidebar.markdown("---")

# 主页面内容
st.title("💰 投资监测分析平台")
st.markdown("欢迎使用投资监测分析平台！")

st.markdown("""
### 📋 功能概览

本平台提供以下功能：

1. **🌍 世界主要资产变动监测**
   - 实时监测全球主要指数、商品、ETF的变动
   - 支持日/周/月/季/半年/年/三年等多种时间周期
   - 提供交互式图表和数据表格

2. **📊 A股买卖信号监测**
   - 基于技术指标生成买卖信号
   - 支持多维度筛选和排序
   - 可视化信号分布和对比

3. **📈 A股基本面分析**
   - 多股票财务指标对比
   - 财务指标趋势分析
   - 关键指标雷达图

4. **🤖 AI投资顾问**
   - 基于LLM的智能投资建议
   - 投资相关问题问答
   - 市场数据分析

### 🚀 快速开始

请使用左侧导航栏选择您需要的功能页面。
""")

# 页脚
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>投资监测分析平台 | 数据仅供参考，投资有风险</div>",
    unsafe_allow_html=True
)

