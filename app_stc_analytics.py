import os, json, re, io
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime
from pathlib import Path
import hashlib
from tourism_pages import (
    render_tourism_sidebar,
    render_cost_page, render_swc_page, render_bench_page
)
from tools_scan import scan_tool
from tools_test import test_tool
from tools_contract import contract_tool

for k in ("tool_choice", "module_choice"):
    if k in st.session_state:
        del st.session_state[k]

st.set_page_config(page_title="STC Analytics", layout="wide")

# ===== Top Navbar: Modules & Tools =====
MODULES = ["Tourism", "Finance (DeFi)", "NFT/Token", "Supply Chain", "Custom Monitor"]
module_choice = st.radio("Modules", MODULES, horizontal=True, key="modules_nav_main")

st.divider()

TOOLS = ["Scan", "Test", "Contract"]
# pakai key berbeda supaya tidak tabrakan
tool_choice = st.radio("Tools", TOOLS, horizontal=True, key=f"tools_nav_main_{module_choice}")

# Modules
if module_choice == "Tourism":
    render_tourism_sidebar()
    t1, t2, t3 = st.tabs(["Cost (Vision)", "Security (SWC)", "Performance (Bench)"])
    with t1: render_cost_page()
    with t2: render_swc_page()
    with t3: render_bench_page()

# Tools
if tool_choice == "Scan":
    scan_tool()
elif tool_choice == "Test":
    test_tool()
else:
    contract_tool()

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"

def _file_hash(p: Path) -> str:
    try:
        with p.open("rb") as f:
            h = hashlib.sha256()
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return "missing"

@st.cache_data(show_spinner=False)
def _read_csv_with_key(path_str: str, content_hash: str) -> pd.DataFrame:
    # cache key = (path_str, content_hash) -> isi file berubah, cache auto refresh
    return pd.read_csv(path_str)

def _load_csv(path: Path, fallback_cols: list[str]) -> pd.DataFrame:
    try:
        if path.exists():
            return _read_csv_with_key(str(path), _file_hash(path))
        else:
            st.warning(f"Template tidak ditemukan: {path.name} — pakai fallback kosong.")
    except Exception as e:
        st.error(f"Gagal baca {path.name}: {e}")
    return pd.DataFrame(columns=fallback_cols).head(0)

def sample_templates():
    tpl_cost = _load_csv(
        TEMPLATES_DIR / "vision_template.csv",
        ["Network","Tx Hash","From","To","Block","Gas Used","Gas Price (Gwei)",
         "Estimated Fee (ETH)","Estimated Fee (Rp)","Contract","Function","Timestamp","Status"]
    )
    tpl_swc = _load_csv(
        TEMPLATES_DIR / "swc_findings_template.csv",
        ["finding_id","timestamp","network","contract","file","line_start","line_end",
         "swc_id","title","severity","confidence","status","remediation","commit_hash"]
    )
    tpl_runs = _load_csv(
        TEMPLATES_DIR / "bench_runs_template.csv",
        ["run_id","timestamp","network","scenario","contract","function_name",
         "concurrency","tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"]
    )
    tpl_tx = _load_csv(
        TEMPLATES_DIR / "bench_tx_template.csv",
        ["run_id","tx_hash","submitted_at","mined_at","latency_ms","status",
         "gas_used","gas_price_wei","block_number","function_name"]
    )
    return tpl_cost, tpl_swc, tpl_runs, tpl_tx

# --- NDJSON reader helper ---
def read_ndjson(uploaded):
    """Baca NDJSON dari st.file_uploader atau file-like object."""
    if uploaded is None:
        return None
    try:
        uploaded.seek(0)
    except Exception:
        pass
    try:
        return pd.read_json(uploaded, lines=True)
    except Exception as e:
        st.error(f"Gagal membaca NDJSON: {e}")
        return None

# --- CSV reader yang toleran (mobile-friendly) ---
def read_csv_any(uploaded):
    """Baca CSV dari st.file_uploader apa pun MIME/ekstensinya."""
    if uploaded is None:
        return None
    # coba pointer ke awal (kalau objeknya mendukung)
    try:
        uploaded.seek(0)
    except Exception:
        pass
    # percobaan 1: langsung ke pandas
    try:
        return pd.read_csv(uploaded, engine="python", on_bad_lines="skip", encoding="utf-8")
    except Exception:
        # percobaan 2: paksa decode bytes → StringIO
        data = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
        return pd.read_csv(io.StringIO(data.decode("utf-8", "ignore")),
                           engine="python", on_bad_lines="skip")

# Quick CSS theme (dark + teal accents)
st.markdown("""
<style>
:root { --accent:#20c997; --accent2:#7c4dff; }
.block-container { padding-top: 1rem; }
section[data-testid="stSidebar"] .st-expander { border:1px solid #313131; border-radius:12px; }
div[data-testid="stMetric"]{
  background: linear-gradient(135deg, rgba(32,201,151,.08), rgba(124,77,255,.06));
  border: 1px solid rgba(128,128,128,.15);
  padding: 12px; border-radius: 12px;
}
.stButton>button, .stDownloadButton>button{
  border-radius:10px; border:1px solid rgba(255,255,255,.15);
}
.stTabs [data-baseweb="tab-list"] { gap: 6px; }
.stTabs [data-baseweb="tab"]{
  background: rgba(255,255,255,.03); border: 1px solid rgba(255,255,255,.08);
  border-radius: 10px; padding: 6px 12px;
}
[data-testid="stHeader"] { background: transparent; }
</style>
""", unsafe_allow_html=True)

DB_PATH = os.getenv("EDA_DB_PATH", "stc_analytics.duckdb")
SWC_KB_PATH = os.getenv("SWC_KB_PATH", "swc_kb.json")

def ensure_db():
    con = duckdb.connect(DB_PATH)
    con.execute("""
CREATE TABLE IF NOT EXISTS vision_costs (
    id TEXT PRIMARY KEY,
    project TEXT,
    network TEXT,
    timestamp TIMESTAMP,
    tx_hash TEXT,
    contract TEXT,
    function_name TEXT,
    block_number BIGINT,
    gas_used BIGINT,
    gas_price_wei BIGINT,
    cost_eth DOUBLE,
    cost_idr DOUBLE,
    meta_json TEXT
);
""")
    con.execute("""CREATE TABLE IF NOT EXISTS swc_findings (
      finding_id TEXT PRIMARY KEY,
      timestamp TIMESTAMP, network TEXT, contract TEXT, file TEXT,
      line_start BIGINT, line_end BIGINT, swc_id TEXT, title TEXT,
      severity TEXT, confidence DOUBLE, status TEXT, remediation TEXT, commit_hash TEXT
    );""")
    con.execute("""CREATE TABLE IF NOT EXISTS bench_runs (
      run_id TEXT PRIMARY KEY, timestamp TIMESTAMP, network TEXT, scenario TEXT,
      contract TEXT, function_name TEXT, concurrency BIGINT, tx_per_user BIGINT,
      tps_avg DOUBLE, tps_peak DOUBLE, p50_ms DOUBLE, p95_ms DOUBLE, success_rate DOUBLE
    );""")
    con.execute("""CREATE TABLE IF NOT EXISTS bench_tx (
      run_id TEXT, tx_hash TEXT, submitted_at TIMESTAMP, mined_at TIMESTAMP,
      latency_ms DOUBLE, status TEXT, gas_used BIGINT, gas_price_wei TEXT,
      block_number BIGINT, function_name TEXT
    );""")
    con.close()

def drop_all():
    con = duckdb.connect(DB_PATH)
    for t in ["vision_costs","swc_findings","bench_runs","bench_tx"]:
        con.execute(f"DROP TABLE IF EXISTS {t};")
    con.close()

# -------------------------------
# SWC KB loader
# -------------------------------
def load_swc_kb():
    """
    Load SWC KB from JSON file.
    Supports:
      1) List of objects: {id,title,description,mitigation}
      2) Dict keyed by SWC-ID: { "SWC-xxx": {title, description|impact, mitigation|fix[]} }
    Returns: Dict[SWC-ID] -> {title, description, mitigation}
    """
    try:
        with open(SWC_KB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            out = {}
            for item in data:
                sid = str(item.get("id", "")).strip()
                if not sid:
                    continue
                out[sid] = {
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                    "mitigation": item.get("mitigation", ""),
                }
            return out
        if isinstance(data, dict):
            out = {}
            for sid, val in data.items():
                out[str(sid)] = {
                    "title": val.get("title", ""),
                    "description": val.get("description", val.get("impact", "")),
                    "mitigation": (
                        val.get("mitigation")
                        if isinstance(val.get("mitigation"), str)
                        else "\n".join(val.get("fix", [])) if isinstance(val.get("fix"), list) else ""
                    ),
                }
            return out
        return {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

# Make sure DB & tables exist
ensure_db()

def get_conn():
    return duckdb.connect(DB_PATH)

# -------------------------------
# Helpers (DB)
# -------------------------------
def upsert(table: str, df: pd.DataFrame, key_cols: list, cols: list) -> int:
    if df is None or df.empty:
        return 0
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns for {table}: {missing}")
    con = get_conn()
    con.execute(f"CREATE TEMP TABLE stg AS SELECT {', '.join(cols)} FROM {table} WITH NO DATA;")
    con.register("df_stage", df[cols])
    con.execute("INSERT INTO stg SELECT * FROM df_stage;")
    where = " AND ".join([f"{table}.{k}=stg.{k}" for k in key_cols])
    con.execute(f"DELETE FROM {table} USING stg WHERE {where};")
    con.execute(f"INSERT INTO {table} SELECT * FROM stg;")
    n = con.execute("SELECT COUNT(*) FROM stg").fetchone()[0]
    con.close()
    return n

def render_tourism_sidebar():
    import streamlit as st, duckdb
    st.sidebar.title("🧭 STC Analytics")
    with st.sidebar.expander("⚙️ Data control", expanded=True):
        st.checkbox("Load existing stored data", value=False, key="load_existing")
        if st.button("🧹 Clear all DuckDB data", use_container_width=True):
            con = duckdb.connect(DB_PATH)
            for t in ["vision_costs", "swc_findings", "bench_runs", "bench_tx"]:
                con.execute(f"DELETE FROM {t};")
            con.close()
            st.success("Database cleared. Siap upload data baru.")
        if st.button("🧨 Reset schema (DROP & CREATE)", use_container_width=True):
            drop_all()
            ensure_db()
            st.success("Schema di-reset. Tabel dibuat ulang.")
