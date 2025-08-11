import os, json, re, io, time
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime

# =========================
# Helpers: IO
# =========================
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

# Normalisasi schema Vision
VISION_COLS = [
    "id","project","network","timestamp","tx_hash","contract","function_name",
    "block_number","gas_used","gas_price_wei","cost_eth","cost_idr","meta_json"
]

def map_vision_rows(rows: list[dict]) -> pd.DataFrame:
    """Terima list(dict) NDJSON → DataFrame sesuai VISION_COLS."""
    if not rows:
        return pd.DataFrame(columns=VISION_COLS)
    d = pd.DataFrame(rows).copy()

    # id fallback
    if "id" not in d.columns or d["id"].isna().any():
        d["id"] = d.apply(lambda r: f"{r.get('tx_hash','')}::{(r.get('function_name') or '')}".strip(), axis=1)

    # meta_json fallback
    if "meta_json" not in d.columns:
        if "meta" in d.columns:
            d["meta_json"] = d["meta"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (x if x else "{}"))
        else:
            d["meta_json"] = "{}"

    # kolom wajib + tipe
    for c in VISION_COLS:
        if c not in d.columns: d[c] = None
    d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
    for numc in ["block_number","gas_used","gas_price_wei","cost_eth","cost_idr"]:
        d[numc] = pd.to_numeric(d[numc], errors="coerce")
    d["project"] = d.get("project", "STC")
    d["project"] = d["project"].fillna("STC")
    return d[VISION_COLS]

def watch_ndjson_file(path: str, state_key: str = "live_cost_offset") -> int:
    """Baca baris baru dari file NDJSON append-only, simpan offset di session_state."""
    if not os.path.exists(path):
        st.warning("File NDJSON belum ada / path salah.")
        return 0
    offset = st.session_state.get(state_key, 0)
    with open(path, "rb") as f:
        f.seek(offset)
        chunk = f.read()
        new_offset = f.tell()
    if new_offset == offset:
        return 0
    lines = [ln for ln in chunk.splitlines() if ln.strip()]
    rows = []
    for ln in lines:
        try:
            rows.append(json.loads(ln.decode("utf-8", "ignore")))
        except Exception:
            pass
    st.session_state[state_key] = new_offset
    if not rows:
        return 0
    d = map_vision_rows(rows)
    return upsert("vision_costs", d, ["id"], VISION_COLS)

def read_csv_any(uploaded):
    """CSV reader toleran (mobile-friendly)."""
    if uploaded is None:
        return None
    try:
        uploaded.seek(0)
    except Exception:
        pass
    try:
        return pd.read_csv(uploaded, engine="python", on_bad_lines="skip", encoding="utf-8")
    except Exception:
        data = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
        return pd.read_csv(io.StringIO(data.decode("utf-8", "ignore")),
                           engine="python", on_bad_lines="skip")

# =========================
# App & Theme
# =========================
st.set_page_config(page_title="STC Analytics (Hybrid)", layout="wide")
st.sidebar.image("assets/stc-logo.png", width=120)

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

# =========================
# DuckDB
# =========================
DB_PATH = os.getenv("EDA_DB_PATH") or st.secrets.get("EDA_DB_PATH", "/tmp/stc_analytics.duckdb")
SWC_KB_PATH = os.getenv("SWC_KB_PATH") or st.secrets.get("SWC_KB_PATH", "swc_kb.json")

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
);""")
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

def get_conn():
    return duckdb.connect(DB_PATH)

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

# =========================
# SWC KB loader
# =========================
def load_swc_kb():
    try:
        with open(SWC_KB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            out = {}
            for item in data:
                sid = str(item.get("id", "")).strip()
                if not sid: continue
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
                    "mitigation": (val.get("mitigation") if isinstance(val.get("mitigation"), str)
                                   else "\n".join(val.get("fix", [])) if isinstance(val.get("fix"), list) else "")
                }
            return out
        return {}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

# =========================
# Sidebar — About & Data controls
# =========================
ensure_db()

with st.sidebar.expander("📘 About / Cara pakai", expanded=True):
    st.markdown("""
### STC Analytics — Hybrid Dashboard
Pantau **biaya gas (Vision)**, **temuan keamanan (SWC)**, dan **hasil benchmark (Bench)** — cepat, ringan, terstruktur.

**Alur singkat**
1) Unggah CSV/NDJSON di tiap tab  
2) (Opsional) centang *Load existing stored data*  
3) Gunakan filter, baca **SWC Knowledge**, dan **Download CSV** hasil filter

**Privasi** — semua data lokal di DuckDB.
""")

st.sidebar.title("🧭 STC Analytics")
with st.sidebar.expander("⚙️ Data control", expanded=True):
    load_existing = st.checkbox("Load existing stored data", value=False, key="load_existing")
    col_btn = st.columns(2)
    with col_btn[0]:
        if st.button("🧹 Clear all DuckDB data", use_container_width=True):
            con = duckdb.connect(DB_PATH)
            for t in ["vision_costs","swc_findings","bench_runs","bench_tx"]:
                con.execute(f"DELETE FROM {t};")
            con.close()
            st.success("Database cleared.")
    with col_btn[1]:
        if st.button("🧨 Reset schema (DROP & CREATE)", use_container_width=True):
            drop_all(); ensure_db()
            st.success("Schema di-reset & dibuat ulang.")

page = st.sidebar.radio("Pilih tab", ["Cost (Vision)","Security (SWC)","Performance (Bench)"], index=0)

# =========================
# COST (Vision)
# =========================
if page == "Cost (Vision)":
    st.title("💰 Cost Analytics — STC Vision")

    with st.expander("Ingest data (NDJSON/CSV) → DuckDB", expanded=False):
        left, right = st.columns(2)
        with left:
            nd = st.file_uploader("Upload NDJSON (vision_costs.ndjson / jsonl)", type=["ndjson","jsonl"], key="nd_cost")
        with right:
            # type=None agar mobile file picker tdk memfilter (kasus abu-abu di Android)
            cs = st.file_uploader("Upload CSV (dari STC-Vision)", type=None, key="csv_cost")

        st.info("Sumber dari **STC GasVision** → set Jaringan & masukkan TxHash → **Download CSV** → upload di sini.", icon="ℹ️")
        st.link_button("🔗 Buka STC GasVision", "https://stc-gasvision.streamlit.app/", use_container_width=True)

        # Templates
        def sample_templates():
            cost_cols = ["Network","Tx Hash","From","To","Block","Gas Used","Gas Price (Gwei)","Estimated Fee (ETH)","Estimated Fee (Rp)","Contract","Function","Timestamp","Status"]
            df_cost = pd.DataFrame([{
                "Network":"Sepolia","Tx Hash":"0x...","From":"0x...","To":"0x...","Block":123456,
                "Gas Used":21000,"Gas Price (Gwei)":22.5,"Estimated Fee (ETH)":0.00047,"Estimated Fee (Rp)":15000,
                "Contract":"SmartReservation","Function":"bookHotel","Timestamp":pd.Timestamp.utcnow().isoformat(),"Status":"Success"
            }], columns=cost_cols)
            return df_cost

        def csv_bytes(df: pd.DataFrame) -> bytes:
            buff = io.StringIO(); df.to_csv(buff, index=False); return buff.getvalue().encode("utf-8")

        c1, c2 = st.columns(2)
        with c1:
            st.download_button("⬇️ Template CSV (Vision)", data=csv_bytes(sample_templates()),
                               file_name="vision_template.csv", mime="text/csv", use_container_width=True)
        with c2:
            st.download_button("⬇️ Contoh NDJSON (Vision)",
                               data=b'{"id":"demo::bookHotel","network":"Sepolia","cost_idr":15000}\n',
                               file_name="vision_sample.ndjson", mime="application/x-ndjson",
                               use_container_width=True)

        # Mapper CSV → schema vision_costs
        def map_csv_cost(df_raw: pd.DataFrame) -> pd.DataFrame:
            m = {
                "Network": "network", "Tx Hash": "tx_hash",
                "From": "from_address", "To": "to_address",
                "Block": "block_number", "Gas Used": "gas_used",
                "Gas Price (Gwei)": "gas_price_gwei",
                "Estimated Fee (ETH)": "cost_eth",
                "Estimated Fee (Rp)": "cost_idr",
                "Contract": "contract", "Function": "function_name",
                "Timestamp": "timestamp", "Status": "status"
            }
            df = df_raw.rename(columns=m).copy()
            df["project"] = "STC"
            df["timestamp"] = pd.to_datetime(df.get("timestamp", pd.NaT), errors="coerce").fillna(pd.Timestamp.utcnow())
            gwei = pd.to_numeric(df.get("gas_price_gwei", 0), errors="coerce").fillna(0)
            df["gas_price_wei"] = (gwei * 1_000_000_000).round().astype("Int64")
            status_series = df.get("status", None)
            df["meta_json"] = status_series.astype(str).apply(lambda s: json.dumps({"status": s}) if s else "{}") if status_series is not None else "{}"
            df["id"] = df.apply(lambda r: f"{r.get('tx_hash','')}::{(r.get('function_name') or '')}".strip(), axis=1)

            cols = VISION_COLS
            for c in cols:
                if c not in df.columns: df[c] = None
            df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce").astype("Int64")
            df["gas_used"]     = pd.to_numeric(df["gas_used"], errors="coerce").astype("Int64")
            df["cost_eth"]     = pd.to_numeric(df["cost_eth"], errors="coerce")
            df["cost_idr"]     = pd.to_numeric(df["cost_idr"], errors="coerce")
            return df[cols]

        ing = 0
        # NDJSON (drag&drop)
        if nd is not None:
            rows = []
            for line in nd:
                if not line: continue
                try:
                    rows.append(json.loads(line.decode("utf-8")))
                except Exception:
                    pass
            if rows:
                d = map_vision_rows(rows)
                ing += upsert("vision_costs", d, ["id"], VISION_COLS)

        # CSV (tanpa filter MIME)
        if cs is not None:
            raw = read_csv_any(cs)
            d = map_csv_cost(raw)
            ing += upsert("vision_costs", d, ["id"], d.columns.tolist())

        if ing:
            st.success(f"{ing} baris masuk ke vision_costs.")

    # --- Auto loop hanya untuk tab Cost (Vision) ---
live_enabled  = st.session_state.get("live_cost_enabled", False)
live_auto     = st.session_state.get("live_cost_auto", False)
live_path     = (st.session_state.get("live_cost_path", "") or "").strip()
live_interval = int(st.session_state.get("live_cost_interval", 5))

if live_enabled and live_auto and live_path:
    now  = time.time()
    last = st.session_state.get("_live_cost_last_ts", 0)
    if now - last >= live_interval:
        added = watch_ndjson_file(live_path)
        if added:
            st.toast(f"Live ingest: +{added} baris", icon="✅")
        st.session_state["_live_cost_last_ts"] = now
        try:
            st.rerun()
        except Exception:
            st.experimental_rerun()

# === Guard (SELALU jalan, tidak di dalam if live...) ===
want_load = st.session_state.get("load_existing", False)
no_new_upload = (st.session_state.get('nd_cost') is None) and (st.session_state.get('csv_cost') is None)
if no_new_upload and not want_load:
    st.info("Belum ada data cost. Upload data atau aktifkan ‘Load existing stored data’.")
    st.stop()

# === View (SELALU jalan) ===
con = get_conn()
df = con.execute("SELECT * FROM vision_costs ORDER BY timestamp DESC").df()
con.close()

if df.empty:
    st.info("Belum ada data cost.")
    st.stop()

cols = st.columns(3)
nets = ["(All)"] + sorted(df["network"].dropna().astype(str).unique().tolist())
fns  = ["(All)"] + sorted(df["function_name"].dropna().astype(str).unique().tolist())
with cols[0]: f_net = st.selectbox("Network", nets, index=0)
with cols[1]: f_fn  = st.selectbox("Function", fns, index=0)
with cols[2]:
    min_d = pd.to_datetime(df["timestamp"]).min().date()
    max_d = pd.to_datetime(df["timestamp"]).max().date()
    dr = st.date_input("Rentang Tanggal", (min_d, max_d))

df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
if f_net != "(All)": df = df[df["network"] == f_net]
if f_fn  != "(All)": df = df[df["function_name"] == f_fn]
if isinstance(dr, tuple) and len(dr) == 2:
    df = df[(df["timestamp"].dt.date >= dr[0]) & (df["timestamp"].dt.date <= dr[1])]

c1,c2,c3 = st.columns(3)
c1.metric("Total Transactions", f"{len(df):,}")
c2.metric("Avg Cost (IDR)", f"Rp {df['cost_idr'].mean():,.2f}" if not df.empty else "Rp 0")
top = df.groupby('function_name', dropna=False)['cost_idr'].mean().sort_values(ascending=False).head(1)
c3.metric("Top Function (Avg Cost)", f"{top.index[0]} — Rp {top.iloc[0]:,.0f}" if len(top)>0 else "-")

colA,colB = st.columns(2)
with colA:
    agg = df.groupby('function_name', dropna=False)['cost_idr'].mean().reset_index()
    fig = px.bar(agg, x='function_name', y='cost_idr', title="Avg Cost per Function (IDR)")
    st.plotly_chart(fig, use_container_width=True)
with colB:
    df_sorted = df.sort_values('timestamp')
    fig = px.line(df_sorted, x='timestamp', y='cost_idr', color='function_name', title="Cost Trend Over Time")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("### Detail Transaksi")
show_cols = ["timestamp","network","tx_hash","contract","function_name","block_number","gas_used","gas_price_wei","cost_eth","cost_idr"]
st.dataframe(df[show_cols], use_container_width=True)
st.download_button("⬇️ Download hasil filter (CSV)",
                   data=io.StringIO(df[show_cols].to_csv(index=False)).getvalue().encode("utf-8"),
                   file_name="vision_filtered.csv", mime="text/csv", use_container_width=True)

# =========================
# SECURITY (SWC)
# =========================
elif page == "Security (SWC)":
    st.title("🛡️ Security Analytics — STC for SWC")

    def map_swc(df: pd.DataFrame) -> pd.DataFrame:
        cols = ["finding_id","timestamp","network","contract","file","line_start","line_end",
                "swc_id","title","severity","confidence","status","remediation","commit_hash"]
        for c in cols:
            if c not in df.columns: df[c] = None
        fallback = df.apply(lambda r: f"{r.get('contract','')}::{r.get('swc_id','')}::{r.get('line_start','')}", axis=1)
        if "finding_id" not in df.columns:
            df["finding_id"] = fallback
        else:
            mask = df["finding_id"].isna() | (df["finding_id"].astype(str).str.strip() == "")
            df.loc[mask, "finding_id"] = fallback[mask]
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
        df = df.drop_duplicates(subset=["finding_id"], keep="last").copy()
        return df[cols]

    with st.expander("Ingest CSV/NDJSON SWC Findings", expanded=False):
        left, right = st.columns(2)
        with left:
            swc_csv = st.file_uploader("Upload CSV swc_findings.csv", type=None, key="swc_csv")
        with right:
            swc_nd = st.file_uploader("Upload NDJSON swc_findings.ndjson", type=["ndjson","jsonl"], key="swc_nd")

        # Template & contoh
        t1, t2 = st.columns(2)
        with t1:
            tmpl = pd.DataFrame(columns=["finding_id","timestamp","network","contract","file","line_start","line_end","swc_id","title","severity","confidence","status","remediation","commit_hash"])
            buff = io.StringIO(); tmpl.to_csv(buff, index=False)
            st.download_button("⬇️ Template CSV (SWC)", data=buff.getvalue().encode("utf-8"),
                               file_name="swc_findings_template.csv", mime="text/csv", use_container_width=True)
        with t2:
            sample_rows = [
                {"finding_id":"","timestamp":pd.Timestamp.utcnow().isoformat(),"network":"Sepolia","contract":"SmartReservation","file":"contracts/SmartReservation.sol","line_start":98,"line_end":102,"swc_id":"SWC-105","title":"Potential issue SWC-105 detected","severity":"Low","confidence":0.82,"status":"Open","remediation":"Review and document","commit_hash":"0xa36e...c5b0"},
                {"finding_id":"SmartTourismToken::SWC-108::279","timestamp":pd.Timestamp.utcnow().isoformat(),"network":"Arbitrum Sepolia","contract":"SmartTourismToken","file":"contracts/SmartTourismToken.sol","line_start":279,"line_end":288,"swc_id":"SWC-108","title":"Potential issue SWC-108 detected","severity":"Medium","confidence":0.87,"status":"Fixed","remediation":"Refactor code and add checks","commit_hash":"0xc54f...54c8"},
            ]
            ndjson_bytes = ("\n".join(json.dumps(r) for r in sample_rows)).encode("utf-8")
            st.download_button("⬇️ Contoh NDJSON (SWC)", data=ndjson_bytes,
                               file_name="swc_findings_sample.ndjson", mime="application/x-ndjson", use_container_width=True)

        ing = 0
        if 'swc_csv' in st.session_state and st.session_state['swc_csv'] is not None:
            d = read_csv_any(st.session_state['swc_csv'])
            d = map_swc(d)
            ing += upsert("swc_findings", d, ["finding_id"], d.columns.tolist())

        if 'swc_nd' in st.session_state and st.session_state['swc_nd'] is not None:
            rows = []
            for line in st.session_state['swc_nd']:
                if not line: continue
                try:
                    rows.append(json.loads(line.decode("utf-8")))
                except Exception:
                    pass
            if rows:
                d = pd.DataFrame(rows)
                d = map_swc(d)
                ing += upsert("swc_findings", d, ["finding_id"], d.columns.tolist())

        if ing:
            st.success(f"{ing} temuan masuk ke swc_findings.")

    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (st.session_state.get('swc_csv') is None and st.session_state.get('swc_nd') is None)
    if no_new_upload and not want_load:
        st.info("Belum ada data temuan SWC. Upload atau aktifkan ‘Load existing stored data’.")
        st.stop()

    con = get_conn()
    df = con.execute("SELECT * FROM swc_findings ORDER BY timestamp DESC").df()
    con.close()

    if df.empty:
        st.info("Belum ada data temuan SWC.")
        st.stop()

    cols = st.columns(3)
    nets = ["(All)"] + sorted(df["network"].dropna().astype(str).unique().tolist())
    sevs = ["(All)"] + sorted(df["severity"].dropna().astype(str).unique().tolist())
    with cols[0]: f_net = st.selectbox("Network", nets, index=0)
    with cols[1]: f_sev = st.selectbox("Severity", sevs, index=0)
    with cols[2]: f_swc = st.text_input("Cari SWC-ID (mis. SWC-107)", "")

    if f_net != "(All)": df = df[df["network"] == f_net]
    if f_sev != "(All)": df = df[df["severity"] == f_sev]
    if f_swc.strip(): df = df[df["swc_id"].astype(str).str.contains(f_swc.strip(), case=False, na=False)]

    total = len(df); high = (df["severity"].astype(str).str.lower() == "high").sum()
    c1,c2,c3 = st.columns(3)
    c1.metric("Total Findings", f"{total:,}")
    c2.metric("High Severity", f"{high:,}")
    c3.metric("Unique SWC IDs", f"{df['swc_id'].nunique():,}")

    pivot = df.pivot_table(index="swc_id", columns="severity", values="finding_id", aggfunc="count", fill_value=0)
    if not pivot.empty:
        fig = px.imshow(pivot, text_auto=True, aspect="auto", title="SWC-ID × Severity (count)")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Detail Temuan")
    detail_cols = ["timestamp","network","contract","file","line_start","swc_id","title","severity","confidence","status","remediation"]
    st.dataframe(df[detail_cols], use_container_width=True)
    st.download_button("⬇️ Download hasil filter (CSV)",
                       data=io.StringIO(df[detail_cols].to_csv(index=False)).getvalue().encode("utf-8"),
                       file_name="swc_filtered.csv", mime="text/csv", use_container_width=True)

    # SWC Knowledge
    st.markdown("### 🔎 SWC Knowledge")
    kb = load_swc_kb()
    if not kb:
        st.warning("SWC KB JSON belum ditemukan. Letakkan **swc_kb.json** atau set env `SWC_KB_PATH`.")
    else:
        available_ids = sorted(df["swc_id"].dropna().astype(str).unique().tolist())
        if not available_ids:
            st.info("Tidak ada SWC-ID pada data saat ini.")
        else:
            sel = st.selectbox("Pilih SWC-ID untuk penjelasan", available_ids, index=0)
            entry = kb.get(sel)
            if entry:
                st.subheader(f"{sel} — {entry.get('title','')}")
                desc = entry.get("description","").strip()
                if desc: st.markdown(desc)
                mit = entry.get("mitigation","").strip()
                if mit:
                    st.markdown("**Mitigation:**")
                    for b in [x.strip() for x in re.split(r"[\n;]", mit) if x.strip()]:
                        st.markdown(f"- {b}")
            else:
                st.info("SWC ini belum ada di KB JSON.")

# =========================
# PERFORMANCE (Bench)
# =========================
else:
    st.title("🚀 Performance Analytics — STC Bench")

    def csv_bytes(df: pd.DataFrame) -> bytes:
        buff = io.StringIO(); df.to_csv(buff, index=False); return buff.getvalue().encode("utf-8")

    with st.expander("Ingest CSV Bench (runs & tx)", expanded=False):
        col1,col2 = st.columns(2)
        with col1:
            runs = st.file_uploader("bench_runs.csv", type=None, key="runs_csv")
            if runs is not None:
                d = read_csv_any(runs)
                cols = ["run_id","timestamp","network","scenario","contract","function_name","concurrency",
                        "tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"]
                for c in cols:
                    if c not in d.columns: d[c] = None
                d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
                n = upsert("bench_runs", d, ["run_id"], cols)
                st.success(f"{n} baris masuk ke bench_runs.")
        with col2:
            tx = st.file_uploader("bench_tx.csv", type=None, key="tx_csv")
            if tx is not None:
                d = read_csv_any(tx)
                cols = ["run_id","tx_hash","submitted_at","mined_at","latency_ms","status","gas_used","gas_price_wei","block_number","function_name"]
                for c in cols:
                    if c not in d.columns: d[c] = None
                d["submitted_at"] = pd.to_datetime(d["submitted_at"], errors="coerce")
                d["mined_at"] = pd.to_datetime(d["mined_at"], errors="coerce")
                con = get_conn()
                con.execute("CREATE TEMP TABLE stg AS SELECT * FROM bench_tx WITH NO DATA;")
                con.register("df_stage", d[cols])
                con.execute("INSERT INTO stg SELECT * FROM df_stage;")
                con.execute("""DELETE FROM bench_tx USING (SELECT DISTINCT run_id, tx_hash FROM stg) d
                               WHERE bench_tx.run_id=d.run_id AND bench_tx.tx_hash=d.tx_hash;""")
                con.execute("INSERT INTO bench_tx SELECT * FROM stg;")
                n = con.execute("SELECT COUNT(*) FROM stg").fetchone()[0]
                con.close()
                st.success(f"{n} baris masuk ke bench_tx.")

        # Templates
        tpl_runs = pd.DataFrame([{
            "run_id":"run-001","timestamp":pd.Timestamp.utcnow().isoformat(),"network":"Sepolia","scenario":"LoadTestSmall",
            "contract":"SmartReservation","function_name":"checkIn","concurrency":50,"tx_per_user":5,
            "tps_avg":85.2,"tps_peak":110.4,"p50_ms":220,"p95_ms":540,"success_rate":0.97
        }])
        tpl_tx = pd.DataFrame([{
            "run_id":"run-001","tx_hash":"0x...","submitted_at":pd.Timestamp.utcnow().isoformat(),"mined_at":pd.Timestamp.utcnow().isoformat(),
            "latency_ms":450,"status":"success","gas_used":21000,"gas_price_wei":"22000000000","block_number":123456,"function_name":"checkIn"
        }])
        dcol1, dcol2 = st.columns(2)
        with dcol1:
            st.download_button("⬇️ Template bench_runs.csv", data=csv_bytes(tpl_runs),
                               file_name="bench_runs_template.csv", mime="text/csv", use_container_width=True)
        with dcol2:
            st.download_button("⬇️ Template bench_tx.csv", data=csv_bytes(tpl_tx),
                               file_name="bench_tx_template.csv", mime="text/csv", use_container_width=True)

    want_load = st.session_state.get("load_existing", False)
    no_new_upload = ((st.session_state.get('runs_csv') is None) and (st.session_state.get('tx_csv') is None))
    if no_new_upload and not want_load:
        st.info("Belum ada data benchmark. Upload atau aktifkan ‘Load existing stored data’.")
        st.stop()

    con = get_conn()
    runs_df = con.execute("SELECT * FROM bench_runs ORDER BY timestamp DESC").df()
    con.close()

    if runs_df.empty:
        st.info("Belum ada data benchmark.")
        st.stop()

    cols = st.columns(3)
    nets = ["(All)"] + sorted(runs_df["network"].dropna().astype(str).unique().tolist())
    scns = ["(All)"] + sorted(runs_df["scenario"].dropna().astype(str).unique().tolist())
    with cols[0]: f_net = st.selectbox("Network", nets, index=0)
    with cols[1]: f_scn = st.selectbox("Scenario", scns, index=0)
    with cols[2]: f_fn  = st.selectbox("Function", ["(All)"] + sorted(runs_df["function_name"].dropna().astype(str).unique().tolist()), index=0)

    df = runs_df.copy()
    if f_net != "(All)": df = df[df["network"] == f_net]
    if f_scn != "(All)": df = df[df["scenario"] == f_scn]
    if f_fn  != "(All)": df = df[df["function_name"] == f_fn]

    k1,k2,k3 = st.columns(3)
    k1.metric("TPS Peak", f"{df['tps_peak'].max():,.2f}" if not df.empty else "0")
    k2.metric("Latency p95 (ms)", f"{df['p95_ms'].mean():,.0f}" if not df.empty else "0")
    k3.metric("Success Rate", f"{(df['success_rate'].mean()*100):.1f}%" if not df.empty else "0%")

    c1,c2 = st.columns(2)
    with c1:
        fig = px.line(df.sort_values('concurrency'), x='concurrency', y='tps_avg', color='scenario', markers=True, title="TPS vs Concurrency")
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        lat = df.melt(id_vars=['concurrency','scenario'], value_vars=['p50_ms','p95_ms'], var_name='metric', value_name='latency_ms')
        fig = px.line(lat.sort_values('concurrency'), x='concurrency', y='latency_ms', color='metric', markers=True, title="Latency (p50/p95) vs Concurrency")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Detail Runs")
    st.dataframe(df, use_container_width=True)
    st.download_button("⬇️ Download hasil filter (CSV)",
                       data=io.StringIO(df.to_csv(index=False)).getvalue().encode("utf-8"),
                       file_name="bench_runs_filtered.csv", mime="text/csv", use_container_width=True)
