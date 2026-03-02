import streamlit as st
import pandas as pd
from pathlib import Path
from utils.constants import PROJECT_PATH
from utils.streamlit_helper import setup_page_config
import google.generativeai as genai
import os

setup_page_config()

st.title("🤖 My Holdings - AI Financial Advisor")

# Paths
HOLDING_FILE = Path(PROJECT_PATH) / 'data/dwa/my_holding.csv'
DECISION_FILE = Path(PROJECT_PATH) / 'data/dwa/app_decision.csv'

# Configure Gemini
api_key = os.environ.get("GEMINI_API_KEY")

if not api_key:
    st.warning("⚠️ GEMINI_API_KEY is not set in environment variables. Please set it to use the AI features.")
    st.stop()

genai.configure(api_key=api_key)

# We use the recommended model for chat
model = genai.GenerativeModel('gemini-2.5-flash')

@st.cache_data
def load_holdings_data():
    if not HOLDING_FILE.exists():
        return None

    df_holdings = pd.read_csv(HOLDING_FILE)
    if df_holdings.empty:
        return None

    if DECISION_FILE.exists():
        df_decision = pd.read_csv(DECISION_FILE)
        df_holdings['symbol'] = df_holdings['symbol'].astype(str)
        df_decision['symbol'] = df_decision['symbol'].astype(str)
        merged_df = pd.merge(df_holdings, df_decision, on='symbol', how='left')
        return merged_df

    return df_holdings

df = load_holdings_data()

if df is None or df.empty:
    st.warning("No holdings found or file is empty.")
    st.stop()

# Initialize chat session in session state if not exists
if "messages" not in st.session_state:
    st.session_state.messages = []

    # Create the initial prompt
    system_prompt = "You are a professional financial advisor and quant analyst. "
    system_prompt += "I will provide you with my current stock holdings and their metrics. "
    system_prompt += "Please give a well-rounded analysis based on micro and macro factors, "
    system_prompt += "including potential company news, industry trends, market sentiment, and economics. "
    system_prompt += "Here are my current holdings data:\n\n"

    # Select important columns to show to LLM
    cols_to_include = ['symbol', 'company', 'industry_type_name', 'close', 'overall_signal_count',
                       'roe', 'fundamental_score', 'fundamental_rank']

    available_cols = [c for c in cols_to_include if c in df.columns]

    data_str = df[available_cols].to_string(index=False)
    system_prompt += data_str + "\n\n"
    system_prompt += "Please provide your initial daily analysis now."

    # We will just insert a system message, but since Gemini chat expects user/model roles:
    # We send the initial prompt as the first user message, but don't display it raw in the UI.

    with st.spinner("🧠 AI is analyzing your portfolio..."):
        try:
            chat = model.start_chat(history=[])
            response = chat.send_message(system_prompt)
            st.session_state.chat_session = chat

            # Add to UI history (we won't show the huge system prompt to the user)
            st.session_state.messages.append({"role": "assistant", "content": response.text})
        except Exception as e:
            st.error(f"Failed to communicate with AI: {e}")
            st.stop()


# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if prompt := st.chat_input("Ask a follow-up question..."):
    # Display user message in chat message container
    st.chat_message("user").markdown(prompt)
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        with st.spinner("Thinking..."):
            try:
                if "chat_session" in st.session_state:
                    response = st.session_state.chat_session.send_message(prompt)
                    full_response = response.text
                else:
                    full_response = "Error: Chat session not initialized."

                message_placeholder.markdown(full_response)
                # Add assistant response to chat history
                st.session_state.messages.append({"role": "assistant", "content": full_response})
            except Exception as e:
                st.error(f"Error: {e}")
