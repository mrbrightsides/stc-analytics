import streamlit as st, duckdb, pandas as pd, json, re
import os, json, re, io
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
from pathlib import Path
import hashlib

def render_tourism_sidebar():
    st.sidebar.title("🧭 STC Analytics")
    with st.sidebar.expander("⚙️ Data control", expanded=True):
        st.checkbox("Load existing stored data", value=False, key="load_existing")
        if st.button("🧹 Clear all DuckDB data", use_container_width=True):
            con = duckdb.connect(DB_PATH)
            for t in ["vision_costs","swc_findings","bench_runs","bench_tx"]:
                con.execute(f"DELETE FROM {t};")
            con.close()
            st.success("Database cleared. Siap upload data baru.")
        if st.button("🧨 Reset schema (DROP & CREATE)", use_container_width=True):
            drop_all(); ensure_db()
            st.success("Schema di-reset. Tabel dibuat ulang dengan struktur terbaru.")

def render_cost_page():
    import streamlit as st
    st.title("💰 Cost Analytics — STC Vision")
    st.info("Halaman Cost (Vision). Tempelkan konten lengkapmu di sini.")

def render_swc_page():
    import streamlit as st
    st.title("🛡️ Security Analytics — STC for SWC")
    st.info("Halaman SWC. Tempelkan konten lengkapmu di sini.")

def render_bench_page():
    import streamlit as st
    st.title("🚀 Performance Analytics — STC Bench")
    st.info("Halaman Bench. Tempelkan konten lengkapmu di sini.")

