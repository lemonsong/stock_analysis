import streamlit as st

def setup_page_config(page_title="投资监测分析平台", page_icon="💰", layout="wide", initial_sidebar_state="expanded"):
    st.set_page_config(
        page_title=page_title,
        page_icon=page_icon,
        layout=layout,
        initial_sidebar_state=initial_sidebar_state
    )


def clear_cache():
    st.cache_data.clear()
    st.cache_resource.clear()

def clean_expired_cache():
    # Streamlit automatically manages cache expiration, but we can force clear
    st.cache_data.clear()

