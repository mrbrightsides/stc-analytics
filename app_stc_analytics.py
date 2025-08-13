import os, json, re, io
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime
from pathlib import Path
import hashlib
from tourism_pages import render_tourism_sidebar, render_cost_page, render_swc_page, render_bench_page
from tools_scan import scan_tool
from tools_test import test_tool
from tools_contract import contract_tool

for k in ("tool_choice", "module_choice"):
    if k in st.session_state:
        del st.session_state[k]

st.set_page_config(page_title="STC Analytics", layout="wide")

MODULES = ["Tourism", "Finance (DeFi)", "NFT/Token", "Supply Chain", "Custom Monitor"]
module_choice = st.radio("Modules", MODULES, horizontal=True, key="modules_nav_main")

st.divider()

TOOLS = ["Scan", "Test", "Contract"]
tool_choice = st.radio("Tools", TOOLS, horizontal=True, key=f"tools_nav_main_{module_choice}")

if module_choice == "Tourism":
    render_tourism_sidebar()
    t1, t2, t3 = st.tabs(["Cost (Vision)", "Security (SWC)", "Performance (Bench)"])
     with t1: render_cost_page()
    with t2: render_swc_page()
    with t3: render_bench_page()
else:
    st.info("Module ini **coming soon**. Fokus dulu ke Tourism.")

st.markdown("---")
if tool_choice == "Scan":
    scan_tool()
elif tool_choice == "Test":
    test_tool()
else:
    contract_tool()
