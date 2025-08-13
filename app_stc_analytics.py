import os, json, re, io, hashlib
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ===== App config (panggilan st.* pertama) =====
st.set_page_config(page_title="STC Analytics", layout="wide")

# ===== Imports halaman (defensif) =====
try:
    from tourism_pages import (
        render_tourism_sidebar,
        render_cost_page,
        render_swc_page,
        render_bench_page,
    )
except Exception as e:
    def render_tourism_sidebar():
        st.error(f"Gagal memuat `tourism_pages`: {err}")
    def render_cost_page():
        st.error(f"Gagal memuat `render_cost_page`: {err}")
    def render_swc_page():
        st.error(f"Gagal memuat `render_swc_page`: {err}")
    def render_bench_page():
        st.error(f"Gagal memuat `render_bench_page`: {err}")

from tools_scan import scan_tool
from tools_test import test_tool
from tools_contract import contract_tool

# ===== State hygiene (opsional) =====
# Bersihkan key lama yang tak terpakai
for k in ("tool_choice", "module_choice"):
    if k in st.session_state:
        del st.session_state[k]

# ===== Top Nav: Modules =====
MODULES = ["Tourism", "Finance (DeFi)", "NFT/Token", "Supply Chain", "Custom Monitor"]
module_choice = st.radio("Modules", MODULES, horizontal=True, key="modules_nav_main")

st.divider()

# ===== Tools Nav (key stabil, reset saat modul berubah) =====
tool_key = f"tools_nav_main_{module_choice}"
if tool_key not in st.session_state:
    # reset pilihan tools jika user pindah modul
    for k in list(st.session_state.keys()):
        if k.startswith("tools_nav_main_") and k != tool_key:
            del st.session_state[k]

TOOLS = ["Scan", "Test", "Contract"]
tool_choice = st.radio("Tools", TOOLS, horizontal=True, key=tool_key)

# ===== Content =====
if module_choice == "Tourism":
    render_tourism_sidebar()

    t1, t2, t3 = st.tabs(["Cost (Vision)", "Security (SWC)", "Performance (Bench)"])

    with t1:
        try:
            render_cost_page()
        except Exception as e:
            st.exception(e)

    with t2:
        try:
            render_swc_page()
        except Exception as e:
            st.exception(e)

    with t3:
        try:
            render_bench_page()
        except Exception as e:
            st.exception(e)
else:
    st.info("Module ini **coming soon**. Fokus dulu ke Tourism.")

st.markdown("---")

# ===== Tools panel =====
if tool_choice == "Scan":
    scan_tool()
elif tool_choice == "Test":
    test_tool()
else:
    contract_tool()
