import os, json, re, io, hashlib
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# --- Import Tourism pages
from tourism_pages import (
    render_tourism_sidebar,
    render_cost_page, render_swc_page, render_bench_page,
)

# --- Import Tools (dengan fallback stub biar app tetap nyala)
try:
    from tools_scan import scan_tool
except Exception:
    def scan_tool():
        st.info("tools_scan.py belum ter-load. (Stub)")

try:
    from tools_test import test_tool
except Exception:
    def test_tool():
        st.info("tools_test.py belum ter-load. (Stub)")

try:
    from tools_contract import contract_tool
except Exception:
    def contract_tool():
        st.info("tools_contract.py belum ter-load. (Stub)")

# --- (opsional) bersihkan key lama yang sempat dipakai versi sebelumnya
for k in ("tool_choice", "module_choice"):
    if k in st.session_state:
        del st.session_state[k]

st.set_page_config(page_title="STC Analytics", layout="wide")

# ===== Top Navbar: Modules & Tools =====
MODULES = ["Tourism", "Finance (DeFi)", "NFT/Token", "Supply Chain", "Custom Monitor"]
module_choice = st.radio("Modules", MODULES, horizontal=True, key="modules_nav_main")

st.divider()

TOOLS = ["Scan", "Test", "Contract"]
tool_choice = st.radio("Tools", TOOLS, horizontal=True, key=f"tools_nav_main_{module_choice}")

# ===== Modules (render sekali) =====
if module_choice == "Tourism":
    render_tourism_sidebar()
    t1, t2, t3 = st.tabs(["Cost (Vision)", "Security (SWC)", "Performance (Bench)"])
    with t1:
        render_cost_page()
    with t2:
        render_swc_page()
    with t3:
        render_bench_page()
else:
    st.markdown(f"## Module: {module_choice}")
    st.markdown(
        "<h1 style='text-align:center; color:gray; margin-top: 2rem;'>COMING SOON</h1>",
        unsafe_allow_html=True,
    )

st.divider()

# ===== Tools (render sekali) =====
if tool_choice == "Scan":
    scan_tool()
elif tool_choice == "Test":
    test_tool()
else:
    contract_tool()
