import os, json, re, io
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime

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

# -------------------------------
# App & DB setup
# -------------------------------
st.set_page_config(page_title="STC Analytics (Hybrid)", layout="wide")

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
# UI helpers: About + Help + Sample templates + CSV util
# -------------------------------
GITHUB_URL = "https://github.com/mrbrightsides"

with st.sidebar.expander("📘 About / Cara pakai", expanded=True):
    st.markdown(
        """
### STC Analytics — Hybrid Dashboard

Satu tempat buat pantau **biaya gas (Vision)**, **temuan keamanan (SWC)**, dan **hasil benchmark (Bench)** — cepat, ringan, dan terstruktur.

---

#### 🚀 Alur kerja singkat
1. **Upload data** (CSV / NDJSON) di tiap tab.  
2. (Opsional) **Load existing stored data** di sidebar untuk pakai data yang sudah tersimpan.  
3. Gunakan **filter** untuk eksplorasi + buka **SWC Knowledge** buat penjelasan tiap _SWC-ID_.  
4. **Export** hasil filter via tombol **Download CSV**.

> ℹ️ Catatan: WebApp ini hanya sebagai **reader/analytics**. Analisis kelemahan detail tetap mengacu ke referensi SWC & tool audit resmi.

---

#### 📦 Format & sumber data (ringkas)
- **Vision (Cost)**  
  - NDJSON: `id, project, network, timestamp, tx_hash, contract, function_name, block_number, gas_used, gas_price_wei, cost_eth, cost_idr, meta_json`.  
  - CSV (dari STC-Vision): pakai **Template CSV (Vision)** / **Contoh NDJSON (Vision)** di tab.
- **Security (SWC)**  
  - CSV/NDJSON: `finding_id (opsional), timestamp, network, contract, file, line_start, line_end, swc_id, title, severity, confidence, status, remediation, commit_hash`.  
  - Kalau `finding_id` kosong, app akan auto-generate `contract::swc_id::line_start` & **de-dup** per batch.  
  - Lihat tombol **Template CSV (SWC)** & **Contoh NDJSON (SWC)** di tab.
- **Performance (Bench)**  
  - `bench_runs.csv`: `run_id, timestamp, network, scenario, contract, function_name, concurrency, tx_per_user, tps_avg, tps_peak, p50_ms, p95_ms, success_rate`.  
  - `bench_tx.csv`: `run_id, tx_hash, submitted_at, mined_at, latency_ms, status, gas_used, gas_price_wei, block_number, function_name`.

---

#### 🧰 Tips & trik
- Struktur kolom berubah? Pakai **Reset schema (DROP & CREATE)** di sidebar.  
- Mau mulai bersih? Klik **Clear all DuckDB data**.  
- Gunakan **date range** & **select filter** buat narrowing cepat.

---

#### 🔒 Privasi
Semua data disimpan **lokal** di **DuckDB**. Aplikasi ini tidak mengirim data ke layanan eksternal.

---

#### 🙌 Dukungan & kontributor
- ⭐ **Star / Fork**: [GitHub repo]({https://github.com/mrbrightsides/stc-analytics})
- Made for STC — _lightweight analytics for web3 dev teams_.

Versi UI: v1.0 • Streamlit + DuckDB • Theme Dark
""",
        unsafe_allow_html=False,
    )

FAQ_MD = """
### FAQs — STC Analytics

**1) Apa itu STC Analytics?**  
STC Analytics adalah dashboard hibrida untuk memvisualisasikan **biaya gas (Vision)**, **temuan keamanan SWC**, dan **hasil benchmark** smart contract.

**2) Apa yang dapat dilakukan?**  
Unggah CSV/NDJSON, lihat metrik/grafik/tabel, unduh template & hasil filter, serta baca ringkasan **SWC Knowledge**.

**3) Penyimpanan & privasi data**  
Data disimpan **lokal** di DuckDB (`stc_analytics.duckdb`) pada mesin Anda; tidak dikirim ke pihak ketiga. Gunakan **Clear data** / **Reset schema** bila diperlukan.

**4) Format yang didukung**  
- Vision: `Network, Tx Hash, Block, Gas Used, Gas Price (Gwei), Estimated Fee (ETH/Rp), Contract, Function, Timestamp, Status`  
- SWC: `finding_id (opsional), timestamp, network, contract, file, line_start, line_end, swc_id, title, severity, confidence, status, remediation, commit_hash`  
- Bench (runs): `run_id, timestamp, network, scenario, contract, function_name, concurrency, tx_per_user, tps_avg, tps_peak, p50_ms, p95_ms, success_rate`  
- Bench (tx opsional): `run_id, tx_hash, submitted_at, mined_at, latency_ms, status, gas_used, gas_price_wei, block_number, function_name`

**5) Akurasi**  
Dashboard menampilkan data sumber; akurasi bergantung input. SWC **bukan** audit engine, gunakan sebagai panduan.

**6) SWC Knowledge**  
Dibaca dari `swc_kb.json` (bisa diatur via `SWC_KB_PATH`). Mendukung format **list** atau **dict** berindeks SWC-ID. Anda bisa menambah/ubah konten.

**7) Duplikasi temuan SWC**  
PK `finding_id`. Jika kosong, app membuat **contract::swc_id::line_start** dan de-dup per batch.

**8) Impor lambat?**  
Pengaruh ukuran file, parsing, upsert, render grafik. Pecah file besar, pastikan header sesuai template, gunakan angka bersih & UTF-8.

**9) Cara meningkatkan kualitas**  
Ikuti template, konsisten `timestamp/function_name/network`. SWC: stabilkan `finding_id`. Bench: `run_id` unik & metrik lengkap.

**10) Integrasi AI**  
Tidak aktif secara default. Bisa ditambahkan (BYO API key) sesuai kebijakan data organisasi.

**11) Ekspor data**  
Gunakan tombol **Download hasil filter (CSV)** di setiap tab.

**12) Multi-chain**  
Didukung; gunakan filter **Network**.

**13) Istilah Bench**  
TPS Peak/Avg, p50_ms/p95_ms (latensi), Success Rate.

**14) Troubleshooting**  
Kolom hilang/PK conflict/parsing tanggal/angka & encoding/berkas besar—lihat bantuan di setiap tab atau gunakan template resmi.
"""

with st.expander("❓ FAQ", expanded=False):
    st.markdown(FAQ_MD)

HELP_COST = """
**Apa itu Cost (Vision)?**  
Menampilkan biaya gas per transaksi/function dari file output STC-Vision.

**Format CSV (header contoh):**  
`Network, Tx Hash, Block, Gas Used, Gas Price (Gwei), Estimated Fee (ETH), Estimated Fee (Rp), Contract, Function, Timestamp, Status`

**Tips:** Timestamp boleh kosong (kita auto-isi); NDJSON didukung (1 objek per baris).
"""

HELP_SWC = """
**Apa itu Security (SWC)?**  
Menampilkan daftar temuan berdasarkan **Smart Contract Weakness Classification**.

**Kolom minimal:**  
`finding_id (opsional), timestamp, network, contract, file, line_start, line_end, swc_id, title, severity, confidence, status, remediation, commit_hash`

> Kalau `finding_id` kosong, kita auto-generate **contract::swc_id::line_start** dan *de-dup* batch sebelum upsert.
"""

HELP_BENCH = """
**Apa itu Performance (Bench)?**  
Menampilkan hasil uji beban (TPS/latency/success rate).

**runs.csv:**  
`run_id, timestamp, network, scenario, contract, function_name, concurrency, tx_per_user, tps_avg, tps_peak, p50_ms, p95_ms, success_rate`

**bench_tx.csv (opsional):**  
`run_id, tx_hash, submitted_at, mined_at, latency_ms, status, gas_used, gas_price_wei, block_number, function_name`
"""

def show_help(which: str):
    with st.expander("🆘 Help", expanded=False):
        if which == "cost":
            st.markdown(HELP_COST)
        elif which == "swc":
            st.markdown(HELP_SWC)
        elif which == "bench":
            st.markdown(HELP_BENCH)

def sample_templates():
    """Buat sample DF utk user download sebagai template."""
    cost_cols = ["Network","Tx Hash","From","To","Block","Gas Used","Gas Price (Gwei)","Estimated Fee (ETH)","Estimated Fee (Rp)","Contract","Function","Timestamp","Status"]
    swc_cols  = ["finding_id","timestamp","network","contract","file","line_start","line_end","swc_id","title","severity","confidence","status","remediation","commit_hash"]
    runs_cols = ["run_id","timestamp","network","scenario","contract","function_name","concurrency","tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"]
    tx_cols   = ["run_id","tx_hash","submitted_at","mined_at","latency_ms","status","gas_used","gas_price_wei","block_number","function_name"]

    df_cost = pd.DataFrame([{
        "Network":"Sepolia","Tx Hash":"0x...","From":"0x...","To":"0x...","Block":123456,
        "Gas Used":21000,"Gas Price (Gwei)":22.5,"Estimated Fee (ETH)":0.00047,"Estimated Fee (Rp)":15000,
        "Contract":"SmartReservation","Function":"bookHotel","Timestamp":pd.Timestamp.utcnow().isoformat(),"Status":"Success"
    }], columns=cost_cols)

    df_swc = pd.DataFrame([{
        "finding_id":"","timestamp":pd.Timestamp.utcnow().isoformat(),"network":"Arbitrum Sepolia",
        "contract":"SmartTourismToken","file":"contracts/SmartTourismToken.sol",
        "line_start":332,"line_end":342,"swc_id":"SWC-108","title":"Potential issue SWC-108 detected",
        "severity":"Medium","confidence":0.83,"status":"Open","remediation":"Refactor code and add checks","commit_hash":"abc123"
    }], columns=swc_cols)

    df_runs = pd.DataFrame([{
        "run_id":"run-001","timestamp":pd.Timestamp.utcnow().isoformat(),"network":"Sepolia","scenario":"LoadTestSmall",
        "contract":"SmartReservation","function_name":"checkIn","concurrency":50,"tx_per_user":5,
        "tps_avg":85.2,"tps_peak":110.4,"p50_ms":220,"p95_ms":540,"success_rate":0.97
    }], columns=runs_cols)

    df_tx = pd.DataFrame([{
        "run_id":"run-001","tx_hash":"0x...","submitted_at":pd.Timestamp.utcnow().isoformat(),"mined_at":pd.Timestamp.utcnow().isoformat(),
        "latency_ms":450,"status":"success","gas_used":21000,"gas_price_wei":"22000000000","block_number":123456,"function_name":"checkIn"
    }], columns=tx_cols)

    return df_cost, df_swc, df_runs, df_tx

def csv_bytes(df: pd.DataFrame) -> bytes:
    buff = io.StringIO()
    df.to_csv(buff, index=False)
    return buff.getvalue().encode("utf-8")

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

# -------------------------------
# Sidebar
# -------------------------------
st.sidebar.title("🧭 STC Analytics")
with st.sidebar.expander("⚙️ Data control", expanded=True):
    load_existing = st.checkbox("Load existing stored data", value=False, key="load_existing")
    if st.button("🧹 Clear all DuckDB data", use_container_width=True):
        con = duckdb.connect(DB_PATH)
        for t in ["vision_costs","swc_findings","bench_runs","bench_tx"]:
            con.execute(f"DELETE FROM {t};")
        con.close()
        st.success("Database cleared. Siap upload data baru.")
    if st.button("🧨 Reset schema (DROP & CREATE)", use_container_width=True):
        drop_all()
        ensure_db()
        st.success("Schema di-reset. Tabel dibuat ulang dengan struktur terbaru.")

page = st.sidebar.radio("Pilih tab", ["Cost (Vision)","Security (SWC)","Performance (Bench)"], index=0)

# -------------------------------
# COST (Vision)
# -------------------------------
if page == "Cost (Vision)":
    st.title("💰 Cost Analytics — STC Vision")

    with st.expander("Ingest data (NDJSON/CSV) → DuckDB", expanded=False):
        # uploader, info, download template ...
        # (kode map_csv_cost kamu di sini)

        if nd is not None:
            rows = []
            for line in nd:
                try:
                    rows.append(json.loads(line.decode("utf-8")))
                except Exception:
                    pass
            if rows:
                d = pd.DataFrame(rows)
                if "id" not in d.columns:
                    d["id"] = d.apply(lambda r: f"{r.get('tx_hash','')}::{(r.get('function_name') or '')}".strip(), axis=1)
                if "meta_json" not in d.columns:
                    if "meta" in d.columns:
                        d["meta_json"] = d["meta"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (x if x else "{}"))
                    else:
                        d["meta_json"] = "{}"
                cols = [
                    "id","project","network","timestamp","tx_hash","contract","function_name",
                    "block_number","gas_used","gas_price_wei","cost_eth","cost_idr","meta_json"
                ]
                for c in cols:
                    if c not in d.columns:
                        d[c] = None
                d["project"] = d.get("project").fillna("STC")
                d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
                for numc in ["block_number","gas_used","gas_price_wei","cost_eth","cost_idr"]:
                    d[numc] = pd.to_numeric(d[numc], errors="coerce")
                ing += upsert("vision_costs", d, ["id"], cols)

        if cs is not None:
            raw = read_csv_any(cs)
            d = map_csv_cost(raw)
            ing += upsert("vision_costs", d, ["id"], d.columns.tolist())

        if ing:
            st.success(f"{ing} baris masuk ke vision_costs.")

    # ← keluar dari expander, tapi masih di dalam if page == ...
    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (
        (st.session_state.get('nd_cost') is None) and
        (st.session_state.get('csv_cost') is None)
    )
    if no_new_upload and not want_load:
        st.info("Belum ada data cost untuk sesi ini. Upload NDJSON/CSV atau aktifkan ‘Load existing stored data’ di sidebar.")
        st.stop()

    con = get_conn()
    df = con.execute("SELECT * FROM vision_costs ORDER BY timestamp DESC").df()
    con.close()

    if df.empty:
        st.info("Belum ada data cost.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Rows", f"{len(df):,}")
        c2.metric("Unique Tx", f"{df['tx_hash'].nunique():,}" if 'tx_hash' in df else "—")
        c3.metric("Total IDR", f"{int(pd.to_numeric(df.get('cost_idr', 0), errors='coerce').fillna(0).sum()):,}")

        st.markdown("### Detail Vision Costs")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "⬇️ Download CSV (All)",
            data=csv_bytes(df),
            file_name="vision_costs_all.csv",
            mime="text/csv",
            use_container_width=True
        )

# -------------------------------
# SECURITY (SWC)
# -------------------------------
elif page == "Security (SWC)":
    st.title("🛡️ Security Analytics — STC for SWC")

    # --- mapping CSV/NDJSON -> schema + id fallback + dedup ---
    def map_swc(df: pd.DataFrame) -> pd.DataFrame:
        cols = ["finding_id","timestamp","network","contract","file","line_start","line_end",
                "swc_id","title","severity","confidence","status","remediation","commit_hash"]
        for c in cols:
            if c not in df.columns:
                df[c] = None

        # fallback id: contract::swc_id::line_start
        fallback = df.apply(
            lambda r: f"{r.get('contract','')}::{r.get('swc_id','')}::{r.get('line_start','')}",
            axis=1
        )
        if "finding_id" not in df.columns:
            df["finding_id"] = fallback
        else:
            mask = df["finding_id"].isna() | (df["finding_id"].astype(str).str.strip() == "")
            df.loc[mask, "finding_id"] = fallback[mask]

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
        df = df.drop_duplicates(subset=["finding_id"], keep="last").copy()
        return df[cols]

    # --- Ingest (AUTO seperti Bench/Vision) ---
    with st.expander("Ingest CSV/NDJSON SWC Findings", expanded=False):
        left, right = st.columns(2)
        with left:
            swc_csv = st.file_uploader("Upload CSV swc_findings.csv", type=None, key="swc_csv")
        with right:
            swc_nd = st.file_uploader("Upload NDJSON swc_findings.ndjson", type=["ndjson","jsonl"], key="swc_nd")

        # ==== DOWNLOAD BUTTONS ====
        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            st.download_button(
                "⬇️ Template CSV (SWC)",
                data=csv_bytes(pd.DataFrame(columns=[
                    "finding_id","timestamp","network","contract","file","line_start","line_end",
                    "swc_id","title","severity","confidence","status","remediation","commit_hash"
                ]).head(0)),
                file_name="swc_findings_template.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with col_dl2:
            sample_rows = [
                {"finding_id":"", "timestamp":"2025-08-11T09:45:00Z", "network":"Sepolia",
                 "contract":"SmartReservation","file":"contracts/SmartReservation.sol",
                 "line_start":98,"line_end":102,"swc_id":"SWC-105","title":"Potential issue SWC-105 detected",
                 "severity":"Low","confidence":0.82,"status":"Open","remediation":"Review and document",
                 "commit_hash":"0xa36e...c5b0"},
                {"finding_id":"SmartTourismToken::SWC-108::279","timestamp":"2025-08-10T16:20:00Z",
                 "network":"Arbitrum Sepolia","contract":"SmartTourismToken",
                 "file":"contracts/SmartTourismToken.sol","line_start":279,"line_end":288,"swc_id":"SWC-108",
                 "title":"Potential issue SWC-108 detected","severity":"Medium","confidence":0.87,"status":"Fixed",
                 "remediation":"Refactor code and add checks","commit_hash":"0xc54f...54c8"},
            ]
            ndjson_bytes = ("\n".join(json.dumps(r) for r in sample_rows)).encode("utf-8")
            st.download_button(
                "⬇️ Contoh NDJSON (SWC)",
                data=ndjson_bytes,
                file_name="swc_findings_sample.ndjson",
                mime="application/x-ndjson",
                use_container_width=True,
            )
        # ==== END DOWNLOAD BUTTONS ====

        # ---- Auto-ingest (langsung proses saat upload) ----
        ing = 0

        if swc_csv is not None:
            d = read_csv_any(swc_csv)
            d = map_swc(d)
            ing += upsert("swc_findings", d, ["finding_id"], d.columns.tolist())

        if swc_nd is not None:
            rows = []
            for line in swc_nd:
                if not line:
                    continue
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

    # ===== DI LUAR EXPANDER (tapi masih di halaman SWC) =====
    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (st.session_state.get("swc_csv") is None and st.session_state.get("swc_nd") is None)
    if no_new_upload and not want_load:
        st.info("Belum ada data temuan SWC untuk sesi ini. Upload CSV/NDJSON atau aktifkan ‘Load existing stored data’.")
        st.stop()

    # --- Load & tampilkan ---
    con = get_conn()
    df = con.execute("SELECT * FROM swc_findings ORDER BY timestamp DESC").df()
    con.close()

    if df.empty:
        st.info("Belum ada data temuan SWC.")
    else:
        cols = st.columns(3)
        nets = ["(All)"] + sorted(df["network"].dropna().astype(str).unique().tolist())
        sevs = ["(All)"] + sorted(df["severity"].dropna().astype(str).unique().tolist())
        with cols[0]: f_net = st.selectbox("Network", nets, index=0)
        with cols[1]: f_sev = st.selectbox("Severity", sevs, index=0)
        with cols[2]: f_swc = st.text_input("Cari SWC-ID (mis. SWC-107)", "")

        if f_net != "(All)":
            df = df[df["network"] == f_net]
        if f_sev != "(All)":
            df = df[df["severity"] == f_sev]
        if f_swc.strip():
            df = df[df["swc_id"].astype(str).str.contains(f_swc.strip(), case=False, na=False)]

        total = len(df); high = (df["severity"].astype(str).str.lower() == "high").sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Findings", f"{total:,}")
        c2.metric("High Severity", f"{high:,}")
        c3.metric("Unique SWC IDs", f"{df['swc_id'].nunique():,}")

        pivot = df.pivot_table(index="swc_id", columns="severity", values="finding_id",
                               aggfunc="count", fill_value=0)
        if not pivot.empty:
            fig = px.imshow(pivot, text_auto=True, aspect="auto", title="SWC-ID × Severity (count)")
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Detail Temuan")
        detail_cols = ["timestamp","network","contract","file","line_start","swc_id","title",
                       "severity","confidence","status","remediation"]
        st.dataframe(df[detail_cols], use_container_width=True)
        st.download_button(
            "⬇️ Download hasil filter (CSV)",
            data=csv_bytes(df[detail_cols]),
            file_name="swc_filtered.csv", mime="text/csv", use_container_width=True
        )

        # --- SWC Knowledge ---
        st.markdown("### 🔎 SWC Knowledge")
        kb = load_swc_kb()
        if not kb:
            st.warning("SWC KB JSON belum ditemukan. Letakkan file **swc_kb.json** di direktori app atau set env `SWC_KB_PATH`.")
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
                    if desc:
                        st.markdown(desc)
                    mit = entry.get("mitigation","").strip()
                    if mit:
                        st.markdown("**Mitigation:**")
                        for b in [x.strip() for x in re.split(r"[\n;]", mit) if x.strip()]:
                            st.markdown(f"- {b}")
                else:
                    st.info("SWC ini belum ada di KB JSON.")

# -------------------------------
# PERFORMANCE (Bench)
# -------------------------------
else:
    st.title("🚀 Performance Analytics — STC Bench")

    with st.expander("Ingest CSV Bench (runs & tx)", expanded=False):
        col1, col2 = st.columns(2)

        # ---- bench_runs ----
        with col1:
            runs = st.file_uploader("bench_runs.csv", type=None, key="runs_csv")
            if runs is not None:
                d = read_csv_any(runs)
                cols = [
                    "run_id","timestamp","network","scenario","contract","function_name",
                    "concurrency","tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"
                ]
                for c in cols:
                    if c not in d.columns:
                        d[c] = None
                d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
                n = upsert("bench_runs", d, ["run_id"], cols)
                st.success(f"{n} baris masuk ke bench_runs.")

        # ---- bench_tx ----
        with col2:
            tx = st.file_uploader("bench_tx.csv", type=None, key="tx_csv")
            if tx is not None:
                d = read_csv_any(tx)
                cols = [
                    "run_id","tx_hash","submitted_at","mined_at","latency_ms","status",
                    "gas_used","gas_price_wei","block_number","function_name"
                ]
                for c in cols:
                    if c not in d.columns:
                        d[c] = None
                d["submitted_at"] = pd.to_datetime(d["submitted_at"], errors="coerce")
                d["mined_at"] = pd.to_datetime(d["mined_at"], errors="coerce")

                con = get_conn()
                con.execute("CREATE TEMP TABLE stg AS SELECT * FROM bench_tx WITH NO DATA;")
                con.register("df_stage", d[cols])
                con.execute("INSERT INTO stg SELECT * FROM df_stage;")
                con.execute("""
                    DELETE FROM bench_tx USING (
                        SELECT DISTINCT run_id, tx_hash FROM stg
                    ) d
                    WHERE bench_tx.run_id = d.run_id AND bench_tx.tx_hash = d.tx_hash;
                """)
                con.execute("INSERT INTO bench_tx SELECT * FROM stg;")
                n = con.execute("SELECT COUNT(*) FROM stg").fetchone()[0]
                con.close()
                st.success(f"{n} baris masuk ke bench_tx.")

        # ---- Templates ----
        _, _, tpl_runs, tpl_tx = sample_templates()
        dcol1, dcol2 = st.columns(2)
        with dcol1:
            st.download_button(
                "⬇️ Template bench_runs.csv",
                data=csv_bytes(tpl_runs),
                file_name="bench_runs_template.csv",
                mime="text/csv",
                use_container_width=True
            )
        with dcol2:
            st.download_button(
                "⬇️ Template bench_tx.csv",
                data=csv_bytes(tpl_tx),
                file_name="bench_tx_template.csv",
                mime="text/csv",
                use_container_width=True
            )

    # ===== di luar expander =====
    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (
        (st.session_state.get("runs_csv") is None) and
        (st.session_state.get("tx_csv") is None)
    )
    if no_new_upload and not want_load:
        st.info("Belum ada data benchmark untuk sesi ini. Upload bench_runs/bench_tx atau aktifkan ‘Load existing stored data’.")
        st.stop()

    con = get_conn()
    runs_df = con.execute("SELECT * FROM bench_runs ORDER BY timestamp DESC").df()
    con.close()

    if runs_df.empty:
        st.info("Belum ada data benchmark.")
    else:
        cols = st.columns(3)
        nets = ["(All)"] + sorted(runs_df["network"].dropna().astype(str).unique().tolist())
        scns = ["(All)"] + sorted(runs_df["scenario"].dropna().astype(str).unique().tolist())
        with cols[0]:
            f_net = st.selectbox("Network", nets, index=0)
        with cols[1]:
            f_scn = st.selectbox("Scenario", scns, index=0)
        with cols[2]:
            f_fn = st.selectbox(
                "Function",
                ["(All)"] + sorted(runs_df["function_name"].dropna().astype(str).unique().tolist()),
                index=0
            )

        df = runs_df.copy()
        if f_net != "(All)":
            df = df[df["network"] == f_net]
        if f_scn != "(All)":
            df = df[df["scenario"] == f_scn]
        if f_fn != "(All)":
            df = df[df["function_name"] == f_fn]

        k1, k2, k3 = st.columns(3)
        k1.metric("TPS Peak", f"{df['tps_peak'].max():,.2f}" if not df.empty else "0")
        k2.metric("Latency p95 (ms)", f"{df['p95_ms'].mean():,.0f}" if not df.empty else "0")
        k3.metric("Success Rate", f"{(df['success_rate'].mean()*100):.1f}%" if not df.empty else "0%")

        c1, c2 = st.columns(2)
        with c1:
            fig = px.line(
                df.sort_values("concurrency"),
                x="concurrency", y="tps_avg", color="scenario",
                markers=True, title="TPS vs Concurrency"
            )
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            lat = df.melt(
                id_vars=["concurrency","scenario"],
                value_vars=["p50_ms","p95_ms"],
                var_name="metric", value_name="latency_ms"
            )
            fig = px.line(
                lat.sort_values("concurrency"),
                x="concurrency", y="latency_ms", color="metric",
                markers=True, title="Latency (p50/p95) vs Concurrency"
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Detail Runs")
        st.dataframe(df, use_container_width=True)
        st.download_button(
            "⬇️ Download hasil filter (CSV)",
            data=csv_bytes(df),
            file_name="bench_runs_filtered.csv",
            mime="text/csv",
            use_container_width=True
        )

        show_help("bench")
