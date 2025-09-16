import os, io
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import json, re, hashlib
import numpy as np
import csv
from datetime import datetime
from pathlib import Path
from tools_bench import render_bench_validation_db

if st.query_params.get("ping") == "1":
    st.write("ok"); st.stop()

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

def load_templates_from_repo():
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

if st.sidebar.button("🔄 Reload templates (clear cache)"):
    st.cache_data.clear()
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass

# --- NDJSON reader helper ---
def read_ndjson(uploaded):
    """Baca NDJSON dari st.file_uploader atau file-like object."""
    if uploaded is None:
        return None
    try:
        uploaded.seek(0)
    except Exception:
        pass
    rows = []
    for raw in uploaded:  # raw bisa bytes ATAU str
        if not raw:
            continue
        s = raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
        s = s.strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        rows.append(obj)

    if not rows:
        return None

    # Jika ada objek bersarang -> luruskan
    from pandas import json_normalize
    try:
        df = json_normalize(rows, sep="_")
    except Exception:
        df = pd.DataFrame(rows)
    return df

# --- CSV reader yang toleran (mobile-friendly) ---
def read_csv_any(uploaded):
    """Baca CSV dari st.file_uploader apa pun MIME/ekstensinya."""
    if uploaded is None:
        return None

    # coba pointer ke awal
    try:
        uploaded.seek(0)
    except Exception:
        pass

    # Percobaan 1: langsung ke pandas dengan setting yang aman untuk teks
    try:
        return pd.read_csv(
            uploaded,
            sep=",",
            engine="python",
            on_bad_lines="skip",
            encoding="utf-8",
            dtype=str,                # semua kolom str biar gak diubah-ubah
            keep_default_na=False,    # "" tetap "", bukan NaN
            na_filter=False,          # jangan auto-NA
            quoting=csv.QUOTE_MINIMAL # hormati quotes dari exporter
        )
    except Exception:
        pass

    # Percobaan 2: paksa bytes -> StringIO
    try:
        data = uploaded.getvalue() if hasattr(uploaded, "getvalue") else uploaded.read()
        return pd.read_csv(
            io.StringIO(data.decode("utf-8", "ignore")),
            sep=",",
            engine="python",
            on_bad_lines="skip",
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            quoting=csv.QUOTE_MINIMAL
        )
    except Exception:
        return None

# -------------------------------
# App & DB setup
# -------------------------------
st.set_page_config(
    page_title="STC Analytics",
    page_icon="🧬",
    layout="wide"
)

LOGO_URL = "https://i.imgur.com/7j5aq4l.png"

col1, col2 = st.columns([1, 4])
with col1:
    st.image(LOGO_URL, width=60)
with col2:
    st.markdown("""
        ## STC Analytics
    """)

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

with st.sidebar.expander("📘 About / Cara pakai", expanded=False):
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
5. Template CSV adalah file kosong berisi kolom sesuai format sistem. Isi dengan data Anda sendiri. Untuk contoh berisi data, gunakan [file dummy](https://github.com/mrbrightsides/stc-analytics/tree/main/dummy) untuk melihat grafik secara cepat.

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

#### 🧩 RANTAI Ecosystem
1. [STC Bench](https://stc-bench.streamlit.app/)
2. [STC GasVision](https://stc-gasvision.streamlit.app/)
3. [STC Converter](https://stc-converter.streamlit.app/)
4. [STC Insight](https://stc-insight.streamlit.app/)
5. [STC Plugin](https://smartourism.elpeef.com/)
6. [SmartFaith](https://smartfaith.streamlit.app/)
7. [Learn3](https://learn3.streamlit.app/)
8. [Nexus](https://rantai-nexus.streamlit.app/)
9. [DataHub](https://stc-data.streamlit.app/)

---
#### 🙌 Dukungan & kontributor
- ⭐ **Star / Fork**: [GitHub repo](https://github.com/mrbrightsides/stc-analytics)
- Built with 💙 by [Khudri](https://s.id/khudri)
- Dukung pengembangan proyek ini melalui: 
[💖 GitHub Sponsors](https://github.com/sponsors/mrbrightsides) • 
[☕ Ko-fi](https://ko-fi.com/khudri) • 
[💵 PayPal](https://www.paypal.com/paypalme/akhmadkhudri) • 
[🍵 Trakteer](https://trakteer.id/akhmad_khudri)

Versi UI: v1.0 • Streamlit • Theme Dark
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
Dibaca dari `swc_kb.json` (bisa diatur via `SWC_KB_PATH`). Mendukung format **list** atau **dict** berindeks SWC-ID.

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

**15) Sumber Referensi SWC Knowledge** 
Konten SWC Knowledge dirujuk dan disesuaikan dari:

- [SWC Registry](https://swcregistry.io)  
- [MythX SWC Database](https://docs.mythx.io/knowledge-base/swc-registry)  
- [Trail of Bits - Smart Contract Weaknesses](https://github.com/crytic/swc-registry)  
- Hasil audit dan ringkasan pribadi tim STC

> ✏️ Anda juga dapat mengontribusikan deskripsi baru via fitur **Tambah SWC KB** dan mengajukan sebagai Pull Request.
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
    cost_cols = [...]
    swc_cols = [...]
    runs_cols = [...]
    tx_cols = [...]

    # return template-only headers, tidak perlu isi
    return {
        "cost": pd.DataFrame(columns=cost_cols),
        "swc": pd.DataFrame(columns=swc_cols),
        "runs": pd.DataFrame(columns=runs_cols),
        "tx": pd.DataFrame(columns=tx_cols),
    }

def csv_bytes(df: pd.DataFrame) -> bytes:
    if df is None or not isinstance(df, pd.DataFrame):
        return b""
    buff = io.StringIO()
    df.to_csv(buff, index=False)
    return buff.getvalue().encode("utf-8")

def _keyify(name: str) -> str:
    return hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
def fig_export_buttons(fig, base_name: str) -> None:
    html = fig.to_html(include_plotlyjs="cdn", full_html=False)
    k = _keyify(base_name)
    c1, c2 = st.columns(2)
    
    with c1:
        st.download_button(
            "⬇️ Export chart (HTML)",
            data=html.encode("utf-8"),
            file_name=f"{base_name}.html",
            mime="text/html",
            key=f"dl_html_{k}",
            use_container_width=True,
        )
    with c2:
        try:
            import plotly.io as pio
            png_bytes = pio.to_image(fig, format="png")  
            c2.download_button(
                "⬇️ Export PNG",
                data=png_bytes,
                file_name=f"{base_name}.png",
                mime="image/png",
                key=f"dl_png_{k}",
                use_container_width=True,
            )
        except Exception:
            c2.caption("Tambah `kaleido` di requirements.txt untuk export PNG")

def mark_outliers_iqr(series: pd.Series) -> pd.Series:
    
    s = pd.to_numeric(series, errors="coerce")
    q1, q3 = s.quantile(0.25), s.quantile(0.75)
    iqr = q3 - q1
    thresh = q3 + 1.5 * iqr
    return s > thresh

# -------------------------------
# Helpers (DB)
# -------------------------------
import pandas as pd
from pandas.api import types as pdt

def upsert(table: str, d: pd.DataFrame, key_cols: list, col_list: list | None = None) -> int:
    if d is None or d.empty:
        return 0

    use_cols = col_list or d.columns.tolist()
    missing = [c for c in use_cols if c not in d.columns]
    if missing:
        raise ValueError(f"Missing columns for {table}: {missing}")

    d = d[use_cols].copy()

    # --- NORMALISASI KEY KE STRING ---
    for k in key_cols:
        d[k] = d[k].astype(str).fillna("").str.strip()

    # --- DEDUP PER KEY (ambil terakhir) ---
    d = d.drop_duplicates(subset=key_cols, keep="last")

    # --- NORMALISASI DATETIME: jadikan naive (tanpa TZ) ---
    for c in d.columns:
        # kalau sudah tz-aware => buang TZ
        if pdt.is_datetime64tz_dtype(d[c]):
            d[c] = pd.to_datetime(d[c], errors="coerce").dt.tz_localize(None)
        # kalau datetime tapi bukan tz => pastikan datetime
        elif pdt.is_datetime64_any_dtype(d[c]):
            d[c] = pd.to_datetime(d[c], errors="coerce")
        # kalau masih string/object dan kelihatan kolom waktu => parse + buang TZ
        elif pdt.is_object_dtype(d[c]) and c.lower() in ("timestamp","ts","time","created_at","updated_at"):
            d[c] = pd.to_datetime(d[c], errors="coerce", utc=True).dt.tz_localize(None)

    col_list_sql = ", ".join(use_cols)
    key_list_sql = ", ".join(key_cols)
    join_cond = " AND ".join([f"{table}.{k} = s.{k}" for k in key_cols])

    con = get_conn()
    try:
        # stg schema identik (kolom yang diinsert saja)
        con.execute(f"CREATE TEMP TABLE stg AS SELECT {col_list_sql} FROM {table} LIMIT 0;")
        con.register("df_stage", d)
        con.execute(f"INSERT INTO stg ({col_list_sql}) SELECT {col_list_sql} FROM df_stage;")

        con.execute(f"""
            DELETE FROM {table}
            USING (SELECT DISTINCT {key_list_sql} FROM stg) AS s
            WHERE {join_cond};
        """)
        con.execute(f"INSERT INTO {table} ({col_list_sql}) SELECT {col_list_sql} FROM stg;")

        n = con.execute("SELECT COUNT(*) FROM stg").fetchone()[0]
        return n
    finally:
        con.close()

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

    # --- helper: mapping CSV Vision -> schema standar (mendukung kolom minimal) ---
    def map_csv_cost(df_raw: pd.DataFrame) -> pd.DataFrame:
        m = {
            "Network": "network", "network": "network",
            "Tx Hash": "tx_hash", "tx_hash": "tx_hash",
            "From": "from_address", "from": "from_address",
            "To": "to_address", "to": "to_address",
            "Block": "block_number", "block": "block_number",
            "Gas Used": "gas_used", "gas_used": "gas_used",
            "Gas Price (Gwei)": "gas_price_gwei", "gas_price_gwei": "gas_price_gwei",
            "Estimated Fee (ETH)": "cost_eth", "estimated_fee_eth": "cost_eth",
            "Estimated Fee (Rp)": "cost_idr", "estimated_fee_rp": "cost_idr",
            "Contract": "contract", "contract": "contract",
            "Function": "function_name", "function": "function_name",
            "Timestamp": "timestamp", "timestamp": "timestamp",
            "Status": "status", "status": "status",
            "id": "id",
        }
        df = df_raw.rename(columns=m, errors="ignore").copy()

        # default project
        df["project"] = "STC"

        ts_src = df["timestamp"] if "timestamp" in df.columns else pd.Series(pd.NaT, index=df.index)
        ts_raw = df.get("timestamp")
        if ts_raw is not None:
            ts_clean = (
                pd.Series(ts_raw, index=df.index)
                  .astype(str)
                  .str.strip()
                  .str.replace(r"Z$", "+00:00", regex=True)
            )
            ts = pd.to_datetime(ts_clean, errors="coerce", utc=True, format="ISO8601")
            ts = ts.fillna(pd.to_datetime(ts_clean, errors="coerce", dayfirst=True, utc=True))
            df["timestamp"] = ts.dt.tz_localize(None)
        else:
            df["timestamp"] = pd.NaT

        if "gas_price_gwei" in df.columns:
            gwei_src = df["gas_price_gwei"]
        else:
            gwei_src = pd.Series(0, index=df.index, dtype="float64")
        gwei = pd.to_numeric(gwei_src, errors="coerce").fillna(0)
        df["gas_price_wei"] = (gwei * 1_000_000_000).round().astype("Int64")

        # meta_json dari status
        if "status" in df.columns:
            df["meta_json"] = df["status"].astype(str).apply(lambda s: json.dumps({"status": s}) if s else "{}")
        elif "meta_json" not in df.columns:
            df["meta_json"] = "{}"

        tx_series = df["tx_hash"] if "tx_hash" in df.columns else pd.Series("", index=df.index)
        fn_series = df["function_name"] if "function_name" in df.columns else pd.Series("", index=df.index)
        tx = tx_series.astype(str).fillna("")
        fn = fn_series.astype(str).fillna("")

        if "id" in df.columns:
            df["id"] = df["id"].astype(str).fillna("").str.strip()
        else:
            df["id"] = ""
        need_id = df["id"].eq("")
        df["id"] = (tx + "::" + fn).where(need_id, df["id"])

        still_empty = df["id"].eq("")
        if still_empty.any():
            unique_fallback = (
                df.astype(str)
                  .agg("|".join, axis=1)
                  .pipe(lambda s: s.str.encode("utf-8"))
                  .map(lambda b: hashlib.sha256(b).hexdigest())
                  .str.slice(0, 16)
            )
            df.loc[still_empty, "id"] = "csv::" + unique_fallback[still_empty]

        cols = [
            "id","project","network","timestamp","tx_hash","contract","function_name",
            "block_number","gas_used","gas_price_wei","cost_eth","cost_idr","meta_json"
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None

        # casts numerik
        df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce").astype("Int64")
        df["gas_used"]     = pd.to_numeric(df["gas_used"], errors="coerce").astype("Int64")
        df["cost_eth"]     = pd.to_numeric(df["cost_eth"], errors="coerce")
        df["cost_idr"]     = pd.to_numeric(df["cost_idr"], errors="coerce")

        # network fallback
        net_series = df["network"] if "network" in df.columns else pd.Series("(Unknown)", index=df.index)
        df["network"] = net_series.fillna("(Unknown)")

        keep_mask = (
            df["id"].ne("") |
            df["function_name"].astype(str).str.strip().ne("") |
            df["gas_used"].fillna(0).ne(0) |
            df["cost_eth"].fillna(0).ne(0) |
            df["cost_idr"].fillna(0).ne(0)
        )
        df = df[keep_mask].copy()

        return df[cols]

    ing = 0

    with st.expander("Ingest data (NDJSON/CSV) → DuckDB", expanded=False):
        left, right = st.columns(2)
        with left:
            cs = st.file_uploader(
                "Upload CSV (dari STC-Vision)",
                type=None, key="csv_cost"
            )
        with right:
            nd = st.file_uploader("Upload NDJSON (vision_costs.ndjson / jsonl)", type=["ndjson", "jsonl"], key="nd_cost")

        # === Templates / samples ===
        tpl_cost = pd.DataFrame(columns=[
            "Timestamp", "Network", "Tx Hash", "Contract", "Function",
            "Block", "Gas Used", "Gas Price (Gwei)",
            "Estimated Fee (ETH)", "Estimated Fee (Rp)", "Status"
        ]).head(0)
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "⬇️ Template CSV (Vision)",
                data=csv_bytes(tpl_cost),
                file_name="vision_template.csv",
                mime="text/csv",
                use_container_width=True
            )
        with c2:
            vision_sample_rows = [{
                "id": "demo::bookHotel", "project": "STC", "network": "Sepolia",
                "timestamp": "2025-08-12T09:45:00Z", "tx_hash": "0xabc123...",
                "contract": "SmartReservation", "function_name": "bookHotel",
                "block_number": 123456, "gas_used": 21000, "gas_price_wei": 22500000000,
                "cost_eth": 0.0005, "cost_idr": 15000, "meta_json": "{\"status\":\"Success\"}"
            }]
            ndjson_bytes = ("\n".join(json.dumps(r) for r in vision_sample_rows)).encode("utf-8")
            st.download_button(
                "⬇️ Contoh NDJSON (Vision)",
                data=ndjson_bytes,
                file_name="vision_sample.ndjson",
                mime="application/x-ndjson",
                use_container_width=True
            )

        # === NDJSON ingest ===
        if nd is not None:
            rows = []
            for line in nd:
                if not line:
                    continue
                try:
                    rows.append(json.loads(line.decode("utf-8")))
                except Exception:
                    pass

            if rows:
                d = pd.DataFrame(rows)
                if "id" not in d.columns:
                    d["id"] = d.apply(lambda r: f"{r.get('tx_hash','')}::{(r.get('function_name') or '')}".strip(), axis=1)
                d["id"] = d["id"].astype(str).fillna("").str.strip()
                if "meta_json" not in d.columns:
                    d["meta_json"] = d["meta_json"].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else (str(x) if x is not None else "{}")
                    )
                elif "meta" in d.columns:
                    d["meta_json"] = d["meta"].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else (str(x) if x else "{}")
                    )
                else:
                    d["meta_json"] = "{}"

                cols = [
                    "id", "project", "network", "timestamp", "tx_hash", "contract", "function_name",
                    "block_number", "gas_used", "gas_price_wei", "cost_eth", "cost_idr", "meta_json"
                ]
                for c in cols:
                    if c not in d.columns:
                        d[c] = None

                d["project"] = d.get("project").fillna("STC").astype(str)

                ts = pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
                d["timestamp"] = ts.dt.tz_convert(None).astype("datetime64[ns]")
                d["timestamp"] = d["timestamp"].fillna(pd.Timestamp.utcnow())
                d["block_number"]  = pd.to_numeric(d["block_number"], errors="coerce").astype("Int64")
                d["gas_used"]      = pd.to_numeric(d["gas_used"], errors="coerce").astype("Int64")
                d["gas_price_wei"] = pd.to_numeric(d["gas_price_wei"], errors="coerce").round().astype("Int64")
                d["cost_eth"]      = pd.to_numeric(d["cost_eth"], errors="coerce")
                d["cost_idr"]      = pd.to_numeric(d["cost_idr"], errors="coerce")

                d["network"]       = d.get("network").astype(str).replace({"nan": None}).fillna("(Unknown)")
                d["contract"]      = d.get("contract").astype(str)
                d["function_name"] = d.get("function_name").astype(str)

                keep_mask = (
                    d["id"].ne("") |
                    d["function_name"].astype(str).str.strip().ne("") |
                    d["gas_used"].fillna(0).ne(0) |
                    d["cost_eth"].fillna(0).ne(0) |
                    d["cost_idr"].fillna(0).ne(0)
                )
                d = d[keep_mask].copy()

                ing += upsert("vision_costs", d, ["id"], cols)
                
        # === CSV ingest ===
        if cs is not None:
            raw = read_csv_any(cs)
            if not raw.empty:
                d = map_csv_cost(raw)
                ing += upsert("vision_costs", d, ["id"], d.columns.tolist())
            else:
                st.warning("CSV kosong atau tidak terbaca.")

        if ing:
            st.success(f"{ing} baris masuk ke vision_costs.")

    # ==== Load & tampilkan data (di luar expander) ====
    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (st.session_state.get("nd_cost") is None and st.session_state.get("csv_cost") is None)
    if no_new_upload and not want_load:
        st.info("Belum ada data cost untuk sesi ini. Upload NDJSON/CSV atau aktifkan ‘Load existing stored data’ di sidebar.")
        st.stop()

    con = get_conn()
    df = con.execute("SELECT * FROM vision_costs ORDER BY timestamp DESC").df()
    con.close()

    if df.empty:
        st.info("Belum ada data cost.")
    else:
        # Ringkasan
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

        # ====== Filters & plotting (with explorer links) ======
        UNPARSED_LABEL = "⚠ Unparsed Function"

        df_base = df.copy()
        df_base["ts"] = pd.to_datetime(df_base["timestamp"], errors="coerce")
        df_base["fn_raw"] = df_base["function_name"]
        df_base["fn"] = df_base["fn_raw"].fillna(UNPARSED_LABEL).replace({"(unknown)": UNPARSED_LABEL})
        df_base["cost_idr_num"] = pd.to_numeric(df_base.get("cost_idr", 0), errors="coerce").fillna(0)
        df_base["gas_used_num"] = pd.to_numeric(df_base.get("gas_used", 0), errors="coerce").fillna(0)
        df_base["gas_price_num"] = pd.to_numeric(df_base.get("gas_price_wei", 0), errors="coerce").fillna(0)

        def short_tx(x: str) -> str:
            x = str(x or "")
            return x[:6] + "…" + x[-4:] if len(x) > 12 else x

        def explorer_tx_url(network: str, tx: str) -> str:
            base = {
                "Ethereum": "https://etherscan.io/tx/{}",
                "Sepolia": "https://sepolia.etherscan.io/tx/{}",
                "Arbitrum": "https://arbiscan.io/tx/{}",
                "Arbitrum One": "https://arbiscan.io/tx/{}",
                "Arbitrum Sepolia": "https://sepolia.arbiscan.io/tx/{}",
                "Polygon": "https://polygonscan.com/tx/{}",
                "Polygon Amoy": "https://amoy.polygonscan.com/tx/{}",
            }.get(str(network), "https://etherscan.io/tx/{}")
            return base.format(tx)

        fc1, fc2, fc3, fc4, fc5, fc6, fc7 = st.columns([1.4, 1, 1, 1, 1, 1, 1])
        with fc1:
            dmin = df_base["ts"].min(); dmax = df_base["ts"].max()
            date_range = st.date_input(
                "Tanggal",
                value=(None if pd.isna(dmin) else dmin.date(),
                       None if pd.isna(dmax) else dmax.date())
            )
        with fc2:
            f_net = st.selectbox("Network", ["(All)"] + sorted(df_base["network"].dropna().astype(str).unique().tolist()), index=0)
        with fc3:
            f_fn = st.selectbox(
                "Function",
                ["(All)"] + sorted(df_base["fn"].dropna().astype(str).unique().tolist()),
                index=0,
                help=f"'{UNPARSED_LABEL}' berarti nama fungsi tidak terdeteksi dari data transaksi/ABI."
            )
        hide_unknown_default = (f_fn != "(All)")
        with fc4:
            hide_unknown = st.checkbox(f"Sembunyikan ({UNPARSED_LABEL})", value=hide_unknown_default)
        with fc5:
            do_smooth = st.checkbox("Smoothing (7-pt)", value=False)
        with fc6:
            line_log = st.checkbox("Line: log scale (Y)", value=False)
        with fc7:
            scatter_scale = st.selectbox("Scatter scale", ["linear", "log x", "log y", "log x & y"], index=0)

        # Apply filters
        df_plot = df_base.copy()
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start, end = date_range
            if start:
                df_plot = df_plot[df_plot["ts"] >= pd.Timestamp(start)]
            if end:
                df_plot = df_plot[df_plot["ts"] < (pd.Timestamp(end) + pd.Timedelta(days=1))]
        if f_net != "(All)":
            df_plot = df_plot[df_plot["network"] == f_net]
        if f_fn != "(All)":
            df_plot = df_plot[df_plot["fn"] == f_fn]
        if hide_unknown or (f_fn != "(All)"):
            df_plot = df_plot[df_plot["fn"] != UNPARSED_LABEL]

        # Stats & downloads
        df_filtered_for_stats = df_base.copy()
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start, end = date_range
            if start:
                df_filtered_for_stats = df_filtered_for_stats[df_filtered_for_stats["ts"] >= pd.Timestamp(start)]
            if end:
                df_filtered_for_stats = df_filtered_for_stats[df_filtered_for_stats["ts"] < (pd.Timestamp(end) + pd.Timedelta(days=1))]
        if f_net != "(All)":
            df_filtered_for_stats = df_filtered_for_stats[df_filtered_for_stats["network"] == f_net]
        if f_fn != "(All)":
            df_filtered_for_stats = df_filtered_for_stats[df_filtered_for_stats["fn"] == f_fn]

        total_rows_stats = len(df_filtered_for_stats)
        unparsed_count = int((df_filtered_for_stats["fn"] == UNPARSED_LABEL).sum())
        pct_unparsed = (unparsed_count / total_rows_stats * 100.0) if total_rows_stats > 0 else 0.0

        b1, b2, b3 = st.columns([2, 1, 1])
        with b1:
            st.caption(
                f"Menampilkan **{len(df_plot):,}** transaksi"
                + (f" | Network: **{f_net}**"  if f_net != "(All)" else "")
                + (f" | Function: **{f_fn}**"  if f_fn != "(All)" else "")
                + (f" | Unparsed: **{pct_unparsed:.1f}%**" if total_rows_stats > 0 else "")
            )
        with b2:
            helper_cols = ["cost_idr_num", "gas_used_num", "gas_price_num", "ts", "fn", "fn_raw"]
            st.download_button(
                "⬇️ Download CSV (Filtered)",
                data=csv_bytes(df_plot.drop(columns=helper_cols, errors="ignore")),
                file_name="vision_filtered.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with b3:
            df_unparsed_filtered = df_filtered_for_stats[df_filtered_for_stats["fn"] == UNPARSED_LABEL]
            st.download_button(
                "⬇️ Unparsed CSV",
                data=csv_bytes(df_unparsed_filtered.drop(columns=helper_cols, errors="ignore")),
                file_name="vision_unparsed_filtered.csv",
                mime="text/csv",
                use_container_width=True,
                disabled=df_unparsed_filtered.empty,
            )

        # Charts
        g1, g2 = st.columns(2)
        with g1:
            ts = df_plot.dropna(subset=["ts"]).sort_values("ts")
            if not ts.empty:
                y = "cost_idr_num"
                if do_smooth and len(ts) >= 7:
                    ts = ts.assign(cost_smooth=ts.groupby("network")[y].transform(lambda s: s.rolling(7, min_periods=1).mean()))
                    y = "cost_smooth"
                show_median = st.checkbox("Tampilkan garis median", value=False)
                tight_range = st.checkbox("Tight Y-range (tanpa 0)", value=True)
                y_pad_pct = st.slider("Padding Y-axis (%)", 0, 25, 8, key="y_pad_pct") if tight_range else 0
                fig = px.line(
                    ts, x="ts", y=y, color="network", markers=not do_smooth,
                    title="Biaya per Transaksi (Rp) vs Waktu",
                    labels={"ts": "Waktu", y: "Biaya (Rp)", "network": "Jaringan"},
                    template="plotly_white",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                if line_log:
                    fig.update_yaxes(type="log")

                if show_median:
                    med = pd.to_numeric(ts[y], errors="coerce").median()
                    fig.add_hline(y=med, line_dash="dot",
                                  annotation_text=f"Median: {med:,.0f} Rp",
                                  annotation_position="top left")
                if tight_range:
                    yvals = pd.to_numeric(ts[y], errors="coerce").dropna()
                    if not yvals.empty:
                        ymin, ymax = float(yvals.min()), float(yvals.max())
                        if ymin == ymax:  
                            ymin *= 0.9; ymax *= 1.05
                        pad = (ymax - ymin) * (y_pad_pct / 100.0)
                        fig.update_yaxes(range=[max(0, ymin - pad), ymax + pad])
                st.plotly_chart(fig, use_container_width=True)
                fig_export_buttons(fig, "vision_cost_timeseries")

        with g2:
            by_fn = (
                df_plot.groupby("fn", as_index=False)["cost_idr_num"]
                .sum()
                .sort_values("cost_idr_num", ascending=False)
                .head(15)
            )
            if not by_fn.empty:
                fig = px.bar(
                    by_fn, x="fn", y="cost_idr_num", color="fn", text_auto=True,
                    title="Total Biaya per Function (Rp) — Top 15",
                    labels={"fn": "Function", "cost_idr_num": "Total Biaya (Rp)"},
                    color_discrete_map={UNPARSED_LABEL: "#F59E0B"},
                    template="plotly_white",
                    color_discrete_sequence=px.colors.qualitative.Set2,
                )
                fig.update_xaxes(categoryorder="total descending")
                st.plotly_chart(fig, use_container_width=True)
                fig_export_buttons(fig, "vision_fn_top15")

        sc = df_plot[(df_plot["gas_used_num"] > 0) & (df_plot["gas_price_num"] > 0)].copy()
        if not sc.empty:
            sc["tx_short"] = sc["tx_hash"].astype(str).map(short_tx)
            sc["cost_str"] = sc["cost_idr_num"].round().astype(int).map(lambda v: f"{v:,}")
            sc["gas_used_str"] = sc["gas_used_num"].round().astype(int).map(lambda v: f"{v:,}")
            sc["gas_price_str"] = sc["gas_price_num"].round().astype(int).map(lambda v: f"{v:,}")
            sc["explorer_url"] = sc.apply(lambda r: explorer_tx_url(r["network"], r["tx_hash"]), axis=1)

            sc["is_outlier"] = mark_outliers_iqr(sc["cost_idr_num"])

            fig = px.scatter(
                sc, x="gas_used_num", y="gas_price_num", size="cost_idr_num", color="network",
                title="Gas Used vs Gas Price (size = Biaya Rp)",
                labels={"gas_used_num": "Gas Used", "gas_price_num": "Gas Price (wei)", "network": "Jaringan"},
                hover_data=None,
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            
            out = sc[sc["is_outlier"]]
            if not out.empty:
                fig.add_scatter(
                    x=out["gas_used_num"], y=out["gas_price_num"],
                    mode="markers",
                    marker=dict(symbol="star", size=16, line=dict(width=2)),
                    name="Outliers (Biaya tinggi)",
                    text=out.apply(
                        lambda r: (
                            f"Function={r['fn']}"
                            f"<br>Tx={r['tx_short']}"
                            f"<br>Biaya (Rp)={r['cost_str']}"
                        ), axis=1),
                    hovertemplate="%{text}",
                )
            
            fig.update_traces(
                text=sc.apply(
                    lambda r: (
                        f"Function={r['fn']}"
                        f"<br>Tx={r['tx_short']}"
                        f"<br>Gas Used={r['gas_used_str']}"
                        f"<br>Gas Price (wei)={r['gas_price_str']}"
                        f"<br>Biaya (Rp)={r['cost_str']}"
                        f"<br>(Buka detail di tabel Unparsed di bawah)"
                    ),
                    axis=1,
                ),
                hovertemplate="%{text}",
            )
            if scatter_scale in ("log x", "log x & y"):
                fig.update_xaxes(type="log")
            if scatter_scale in ("log y", "log x & y"):
                fig.update_yaxes(type="log")
            st.plotly_chart(fig, use_container_width=True)
            fig_export_buttons(fig, "vision_gas_vs_price")

            topn = st.slider("Tampilkan Top N transaksi berdasarkan biaya (Rp)", 5, 50, 15, key="topn_cost")
            top_tbl = sc.sort_values("cost_idr_num", ascending=False).head(topn)
            st.markdown("#### 💸 Top transaksi berdasarkan biaya (Rp)")
            st.dataframe(
                top_tbl[["timestamp","network","contract","fn","tx_short","cost_idr_num","explorer_url"]],
                use_container_width=True,
                column_config={
                    "tx_short": "Tx (short)",
                    "fn": "Function",
                    "cost_idr_num": st.column_config.NumberColumn("Biaya (Rp)", format="%,d"),
                    "timestamp": st.column_config.DatetimeColumn("Waktu"),
                    "explorer_url": st.column_config.LinkColumn("Explorer", display_text="Open"),
                },
                hide_index=True,
            )
            st.download_button(
                "⬇️ Download Top transaksi (CSV)",
                data=csv_bytes(top_tbl.drop(columns=["gas_used_str","gas_price_str"], errors="ignore")),
                file_name="vision_top_cost.csv",
                mime="text/csv",
                use_container_width=True,
            )

        # Tabel Unparsed
        unparsed = df_base[df_base["fn"] == UNPARSED_LABEL].copy()
        if not unparsed.empty:
            unparsed["Explorer"] = unparsed.apply(lambda r: explorer_tx_url(r["network"], r["tx_hash"]), axis=1)
            unparsed["Tx (short)"] = unparsed["tx_hash"].map(short_tx)
            st.markdown("#### 🔎 Unparsed Function — periksa di explorer")
            st.dataframe(
                unparsed[["timestamp", "network", "contract", "Tx (short)", "Explorer", "cost_idr"]],
                use_container_width=True,
                column_config={
                    "Explorer": st.column_config.LinkColumn("Explorer", display_text="Open"),
                    "cost_idr": st.column_config.NumberColumn("Biaya (Rp)", format="%,d"),
                    "timestamp": st.column_config.DatetimeColumn("Waktu"),
                },
            )
            st.caption("Catatan: Unparsed berarti nama fungsi tidak terdeteksi dari data transaksi. Cek ABI/source di explorer.")

# -------------------------------
# SECURITY (SWC)
# -------------------------------
elif page == "Security (SWC)":
    st.title("🛡️ Security Analytics — STC for SWC")

    COLS_SWC = [
        "finding_id","timestamp","network","contract","file",
        "line_start","line_end","swc_id","title","severity",
        "confidence","status","remediation","commit_hash",
    ]

    # --- mapping CSV/NDJSON -> schema + id fallback + dedup ---
    def map_swc(df: pd.DataFrame) -> pd.DataFrame:
        # pastikan semua kolom ada
        for c in COLS_SWC:
            if c not in df.columns:
                df[c] = pd.NA
    
        # kolom teks jadi string & isi kosong
        text_cols = [
            "finding_id","network","contract","file","swc_id",
            "title","severity","status","remediation","commit_hash"
        ]
        df[text_cols] = df[text_cols].astype("string").fillna("")
    
        # numeric
        df["confidence"] = pd.to_numeric(
            df["confidence"].replace(r"^\s*$", np.nan, regex=True), errors="coerce"
        )
        for c in ["line_start", "line_end"]:
            df[c] = pd.to_numeric(
                df[c].replace(r"^\s*$", np.nan, regex=True), errors="coerce"
            ).astype("Int64")
    
        # normalisasi severity ringan
        df["severity"] = (
            df["severity"].str.lower()
            .replace({"info": "informational", "informative": "informational"})
        )
    
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
        df["finding_id"] = df["finding_id"].fillna("UNKNOWN")
    
        # parsing timestamp aman
        from dateutil import parser
        def parse_timestamp_safe(ts):
            try:
                dt = pd.Timestamp(parser.isoparse(str(ts).strip()))
                try:
                    dt = dt.tz_localize(None)
                except Exception:
                    pass
                return dt
            except Exception:
                return pd.NaT
    
        df["timestamp"] = df["timestamp"].apply(parse_timestamp_safe)
        invalid_rows = int(df["timestamp"].isna().sum())
        st.info(f"🔴 Jumlah timestamp gagal parsing: {invalid_rows}")
        df["timestamp"] = df["timestamp"].fillna(pd.Timestamp.utcnow())
    
        st.write("📅 Preview timestamp:")
        st.write(df["timestamp"].head())
    
        # dedup by finding_id
        df = df.drop_duplicates(subset=["finding_id"], keep="last").copy()
        return df[COLS_SWC]

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
                {
                    "finding_id":"", "timestamp":"2025-08-11T09:45:00Z", "network":"Sepolia",
                    "contract":"SmartReservation","file":"contracts/SmartReservation.sol",
                    "line_start":98,"line_end":102,"swc_id":"SWC-105","title":"Potential issue SWC-105 detected",
                    "severity":"Low","confidence":0.82,"status":"Open","remediation":"Review and document",
                    "commit_hash":"0xa36e...c5b0"
                },
                {
                    "finding_id":"SmartTourismToken::SWC-108::279","timestamp":"2025-08-10T16:20:00Z",
                    "network":"Arbitrum Sepolia","contract":"SmartTourismToken",
                    "file":"contracts/SmartTourismToken.sol","line_start":279,"line_end":288,"swc_id":"SWC-108",
                    "title":"Potential issue SWC-108 detected","severity":"Medium","confidence":0.87,"status":"Fixed",
                    "remediation":"Refactor code and add checks","commit_hash":"0xc54f...54c8"
                },
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
            ing += upsert("swc_findings", d, ["finding_id"], COLS_SWC)

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
                ing += upsert("swc_findings", d, ["finding_id"], COLS_SWC)

        if ing:
            st.success(f"{ing} temuan masuk ke swc_findings.")

    # ===== DI LUAR EXPANDER (tapi masih di halaman SWC) =====
    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (st.session_state.get("swc_csv") is None and st.session_state.get("swc_nd") is None)
    if no_new_upload and not want_load:
        st.info("Belum ada data temuan SWC untuk sesi ini. Upload CSV/NDJSON atau aktifkan ‘Load existing stored data’.")
        st.stop()

    # --- Load data ---
    con = get_conn()
    swc_df = con.execute("SELECT * FROM swc_findings ORDER BY timestamp DESC").df()
    con.close()

    if swc_df.empty:
        st.info("Belum ada data temuan SWC.")
    else:
        # ====== base + helpers ======
        swc_base = swc_df.copy()
        swc_base["ts"]  = pd.to_datetime(swc_base["timestamp"], errors="coerce")
        swc_base["sev"] = swc_base["severity"].fillna("(unknown)")
        swc_base["conf_num"] = pd.to_numeric(swc_base.get("confidence", 0), errors="coerce").fillna(0.0)

        # ====== filters (mirip Vision) ======
        fc1, fc2, fc3 = st.columns([1.4, 1, 1])
        with fc1:
            dmin, dmax = swc_base["ts"].min(), swc_base["ts"].max()
            date_range = st.date_input(
                "Tanggal",
                value=(None if pd.isna(dmin) else dmin.date(),
                       None if pd.isna(dmax) else dmax.date())
            )
        with fc2:
            nets = ["(All)"] + sorted(swc_base["network"].dropna().astype(str).unique().tolist())
            f_net = st.selectbox("Network", nets, index=0)
        with fc3:
            sevs = ["(All)"] + sorted(swc_base["sev"].dropna().astype(str).unique().tolist())
            f_sev = st.selectbox("Severity", sevs, index=0)

        # apply filters
        swc_plot = swc_base.copy()
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start, end = date_range
            if start:
                swc_plot = swc_plot[swc_plot["ts"] >= pd.Timestamp(start)]
            if end:
                swc_plot = swc_plot[swc_plot["ts"] < (pd.Timestamp(end) + pd.Timedelta(days=1))]
        if f_net != "(All)":
            swc_plot = swc_plot[swc_plot["network"] == f_net]
        if f_sev != "(All)":
            swc_plot = swc_plot[swc_plot["sev"] == f_sev]

        # ====== badge + download ======
        b1, b2 = st.columns([2, 1])
        with b1:
            st.caption(
                f"Menampilkan **{len(swc_plot):,}** temuan"
                + (f" | Network: **{f_net}**" if f_net != "(All)" else "")
                + (f" | Severity: **{f_sev}**" if f_sev != "(All)" else "")
            )
        with b2:
            st.download_button(
                "⬇️ Download CSV (Filtered)",
                data=csv_bytes(swc_plot.drop(columns=["ts","sev","conf_num"], errors="ignore")),
                file_name="swc_findings_filtered.csv",
                mime="text/csv",
                use_container_width=True
            )

        # ====== metrics ======
        total = len(swc_plot)
        high  = (swc_plot["sev"].astype(str).str.lower() == "high").sum()
        uniq  = swc_plot["swc_id"].nunique()
        m1, m2, m3 = st.columns(3)
        m1.metric("Total Findings", f"{total:,}")
        m2.metric("High Severity", f"{high:,}")
        m3.metric("Unique SWC IDs", f"{uniq:,}")

        # ====== heatmap ======
        pivot = swc_plot.pivot_table(
            index="swc_id", columns="sev", values="finding_id",
            aggfunc="count", fill_value=0
        )
        if not pivot.empty:
            fig = px.imshow(
                pivot,
                text_auto=True, aspect="auto",
                title="SWC-ID × Severity (count)",
                template="plotly_white",
                color_continuous_scale="Blues"
            )
            st.plotly_chart(fig, use_container_width=True)
            fig_export_buttons(fig, "swc_heatmap")

        by_sev = swc_plot.groupby("sev", as_index=False).size()
        if not by_sev.empty:
            fig = px.bar(
                by_sev, x="sev", y="size", color="sev",
                title="Findings by Severity",
                labels={"sev":"Severity", "size":"Count"},
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_xaxes(categoryorder="array", categoryarray=["Critical","High","Medium","Low","(unknown)"])
            st.plotly_chart(fig, use_container_width=True)
            fig_export_buttons(fig, "swc_by_severity")

        # ====== table ======
        st.markdown("### Detail Temuan")
        detail_cols = COLS_SWC
        base = swc_plot  # hasil query DuckDB
        dfv = base.copy()

        # pastikan semua kolom ada
        for c in detail_cols:
            if c not in dfv.columns:
                dfv[c] = pd.NA
        
        # kolom teks (TANPA confidence)
        text_cols = [
            "finding_id","network","contract","file","swc_id",
            "title","severity","status","remediation","commit_hash"
        ]
        dfv[text_cols] = dfv[text_cols].astype("string").fillna("")
        
        # line number -> Int64 dengan fallback 0
        for c in ["line_start","line_end"]:
            dfv[c] = pd.to_numeric(dfv[c], errors="coerce").fillna(0).astype("Int64")
        
        # normalize severity termasuk 'informational'
        dfv["severity"] = (
            dfv["severity"].str.lower()
                           .replace({"info": "informational", "informative": "informational"})
        )
        sev_order = pd.CategoricalDtype(["critical","high","medium","low","informational"], ordered=True)
        dfv["severity"] = dfv["severity"].astype(sev_order)
        
        # --- confidence: angka dulu, kalau gagal map dari kata ---
        if "confidence" in dfv.columns:
            conf_str = dfv["confidence"].astype("string")
            conf_num = pd.to_numeric(conf_str, errors="coerce")                # 0.5, 0.75, dst
            conf_map = conf_str.str.lower().map({"low": 0.25, "medium": 0.50, "high": 0.75})
            dfv["confidence"] = conf_num.combine_first(conf_map).round(2)      # hasil akhir numeric
        
        # urutkan
        dfv = dfv.sort_values(["severity","timestamp"], ascending=[True, False])
        
        # tampilkan & unduh
        dfv_display = dfv[detail_cols].copy()
        dfv_display["timestamp"] = (
            pd.to_datetime(dfv_display["timestamp"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        mask_zero = (dfv_display["line_start"] == 0) & (dfv_display["line_end"] == 0)
        dfv_display.loc[mask_zero, ["line_start","line_end"]] = pd.NA
        
        st.dataframe(dfv_display, use_container_width=True)

        st.download_button(
            "⬇️ Download tabel di atas (CSV)",
            data=dfv_display.to_csv(index=False, na_rep="").encode("utf-8"),
            file_name="swc_table_filtered.csv",
            mime="text/csv",
            use_container_width=True,
            key="dl_swc_table_filtered",
        )


        # ====== SWC Knowledge ======
        st.markdown("### 🔎 SWC Knowledge")
        kb = load_swc_kb()
        if not kb:
            st.warning("SWC KB JSON belum ditemukan. Letakkan file **swc_kb.json** di direktori app atau set env `SWC_KB_PATH`.")
        else:
            available_ids = sorted(swc_plot["swc_id"].dropna().astype(str).unique().tolist())
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

                    with st.expander("➕ Tambahkan penjelasan untuk SWC ini"):
                        new_title = st.text_input("Judul SWC", key="title_input")
                        new_desc = st.text_area("Deskripsi SWC", key="desc_input", height=200)
                        new_mitigation = st.text_area("Mitigasi (opsional)", key="mitigation_input", height=100)

                        if st.button("💾 Buat Draft JSON untuk Pull Request"):
                            if new_title and new_desc:
                                draft_kb = {
                                    sel: {
                                        "title": new_title.strip(),
                                        "description": new_desc.strip(),
                                        "mitigation": new_mitigation.strip()
                                    }
                                }
                                draft_json = json.dumps(draft_kb, indent=2)

                                st.download_button(
                                    label="⬇️ Download Draft KB (JSON)",
                                    data=draft_json,
                                    file_name=f"{sel}_kb_contribution.json",
                                    mime="application/json",
                                    use_container_width=True
                                )

                                st.success("✅ Draft berhasil dibuat.")
                                st.info("Silakan ajukan file ini sebagai Pull Request ke repositori kami.")
                                st.markdown("[📌 Buat PR di GitHub](https://github.com/mrbrightsides/stc-analytics/pulls)", unsafe_allow_html=True)
                            else:
                                st.error("Judul dan deskripsi wajib diisi.")

# -------------------------------
# PERFORMANCE (Bench)
# -------------------------------
elif page == "Performance (Bench)":
    st.title("🚀 Performance Analytics — STC Bench")

    with st.expander("Ingest CSV Bench (runs & tx)", expanded=False):
        col1, col2 = st.columns(2)

        # ---- bench_runs ----
        with col1:
            runs = st.file_uploader("bench_runs.csv", type=None, key="runs_csv")
            if runs is not None:
                d = read_csv_any(runs)
                d["run_id"] = d["run_id"].astype(str).str.strip()
                cols = [
                    "run_id","timestamp","network","scenario","contract","function_name",
                    "concurrency","tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"
                ]
                for c in cols:
                    if c not in d.columns:
                        d[c] = None
                d["timestamp"] = (
                    pd.to_datetime(d["timestamp"], errors="coerce", utc=True)
                      .dt.tz_localize(None)
                )
                d["run_id"] = d["run_id"].astype(str).str.strip()
                n = upsert("bench_runs", d, ["run_id"], cols)

                st.success(f"{n} baris masuk ke bench_runs.")

        # ---- bench_tx ----
        with st.expander("📘 Panduan Upload CSV (Wajib Baca)", expanded=False):
            st.markdown("""
            ### ℹ️ Penting! Unggah Dua File CSV secara Bersamaan

Untuk menjalankan analisis performa dengan akurat, **dua file CSV harus diunggah secara bersamaan**:

#### 📁 File yang dibutuhkan:
1. **`bench_runs.csv`** – berisi ringkasan pengujian (_run ID, skenario, concurrency, TPS, latency_, dll).
2. **`bench_tx.csv`** – berisi detail transaksi dari setiap run (_run ID, hash, status, gas used_, dll).

---

#### 🧩 Kenapa harus dua file?
Data performa dihasilkan dari **penggabungan (`JOIN`) berdasarkan kolom `run_id`**. Jika salah satu file tidak tersedia:
- Grafik dan metrik seperti **TPS vs Concurrency**, **Latency**, dan **Success Rate** tidak dapat dihitung dengan lengkap.
- Data tidak dapat dianalisis secara menyeluruh.
- Hasil akan kosong atau tidak valid.

---

#### ✅ Tips:
- Gunakan template CSV yang tersedia di [GitHub repo](https://github.com/mrbrightsides/stc-analytics/tree/main/dummy).
- Pastikan struktur kolom sesuai, terutama kolom `run_id` sebagai penghubung utama.
- Setelah diunggah, sistem akan menampilkan notifikasi sukses dan mengaktifkan dashboard analitik.
            """)

        with col2:
            tx = st.file_uploader("bench_tx.csv", type=None, key="tx_csv")
            if tx is not None:
                d = read_csv_any(tx)
                d["run_id"] = d["run_id"].astype(str).str.strip()
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
                con.execute("""
                    CREATE TEMP TABLE stg (
                        run_id TEXT,
                        tx_hash TEXT,
                        submitted_at TIMESTAMP,
                        mined_at TIMESTAMP,
                        latency_ms INTEGER,
                        status TEXT,
                        gas_used INTEGER,
                        gas_price_wei BIGINT,
                        block_number INTEGER,
                        function_name TEXT
                    );
                """)

                # Pastikan semua kolom punya tipe data sesuai DuckDB
                d["latency_ms"] = pd.to_numeric(d["latency_ms"], errors="coerce").fillna(0).astype("int64")
                d["gas_used"] = pd.to_numeric(d["gas_used"], errors="coerce").fillna(0).astype("int64")
                d["gas_price_wei"] = pd.to_numeric(d["gas_price_wei"], errors="coerce").fillna(0).astype("int64")
                d["block_number"] = pd.to_numeric(d["block_number"], errors="coerce").fillna(0).astype("int64")

                # Teks
                for col in ["run_id", "tx_hash", "status", "function_name"]:
                    d[col] = d[col].astype(str).fillna("")

                # Timestamp
                d["submitted_at"] = pd.to_datetime(d["submitted_at"], errors="coerce")
                d["mined_at"] = pd.to_datetime(d["mined_at"], errors="coerce")
              
                object_cols = d.select_dtypes(include="object").columns
                for col in object_cols:
                    d[col] = d[col].fillna("").astype(str)

                for col in d.select_dtypes(include="object").columns:
                    d[col] = d[col].astype(str).fillna("").str.replace(r"[\n\r\t]", " ", regex=True)

                d = d.loc[:, cols]

                con.register("df_stage", d.loc[:, cols])
                con.execute("""
                    INSERT INTO stg (
                        run_id, tx_hash, submitted_at, mined_at, latency_ms,
                        status, gas_used, gas_price_wei, block_number, function_name
                    )
                    SELECT
                        run_id, tx_hash, submitted_at, mined_at, latency_ms,
                        status, gas_used, gas_price_wei, block_number, function_name
                    FROM df_stage;
                """)

                con.execute("""
                    DELETE FROM bench_tx USING (
                        SELECT DISTINCT run_id, tx_hash FROM stg
                    ) d
                    WHERE bench_tx.run_id = d.run_id AND bench_tx.tx_hash = d.tx_hash;
                """)
                con.execute("INSERT INTO bench_tx SELECT * FROM stg;")
                con.execute("UPDATE bench_runs SET run_id = TRIM(run_id);")
                con.execute("UPDATE bench_tx   SET run_id = TRIM(run_id);")
                con.execute("UPDATE bench_runs SET run_id = REGEXP_REPLACE(run_id, '[\\n\\r\\t]', ' ');")
                con.execute("UPDATE bench_tx   SET run_id = REGEXP_REPLACE(run_id, '[\\n\\r\\t]', ' ');")

                match_cnt = con.execute("""
                    SELECT COUNT(*) AS match_cnt FROM (
                        SELECT DISTINCT r.run_id
                        FROM bench_runs r
                        JOIN bench_tx t ON r.run_id = t.run_id
                    )
                """).fetchone()[0]

                n = con.execute("SELECT COUNT(*) FROM stg").fetchone()[0]
                st.success(f"{n} baris masuk ke bench_tx. run_id match: {match_cnt}")

                con.close()

        render_bench_validation_db(get_conn)

        # ---- Templates ----
        button_html = lambda label, url: f"""
        <a href="{url}" target="_blank">
            <button style="width:100%;padding:0.5em 1em;font-size:1em;">{label}</button>
        </a>
        """
        dcol1, dcol2 = st.columns(2)
        with dcol1:
            st.markdown(button_html("⬇️ Template bench_runs.csv", "https://drive.google.com/uc?export=download&id=1tJMjVpuE4v7pfMhlMtYOBMLYgP3MH9fs"), unsafe_allow_html=True)
        with dcol2:
            st.markdown(button_html("⬇️ Template bench_tx.csv", "https://drive.google.com/uc?export=download&id=1GNBfIdw8c3-4xGOAud3R2NJqoLA-EFyw"), unsafe_allow_html=True)

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
        # ===== base + helper cols =====
        base = runs_df.copy()
        base["ts"]   = pd.to_datetime(base["timestamp"], errors="coerce")
        base["succ"] = pd.to_numeric(base.get("success_rate", 0), errors="coerce").fillna(0.0)

        # cast numerik biar plot/metric aman
        for col in ["concurrency","tps_avg","tps_peak","p50_ms","p95_ms"]:
            base[col] = pd.to_numeric(base.get(col, 0), errors="coerce")
        base["network"] = base.get("network").fillna("(Unknown)")

        # ===== filters (tanggal + network + scenario + function) =====
        fc1, fc2, fc3, fc4 = st.columns([1.4,1,1,1])
        with fc1:
            dmin, dmax = base["ts"].min(), base["ts"].max()
            date_range = st.date_input(
                "Tanggal",
                value=(None if pd.isna(dmin) else dmin.date(),
                       None if pd.isna(dmax) else dmax.date())
            )
        with fc2:
            nets = ["(All)"] + sorted(base["network"].dropna().astype(str).unique().tolist())
            f_net = st.selectbox("Network", nets, index=0)
        with fc3:
            scns = ["(All)"] + sorted(base["scenario"].dropna().astype(str).unique().tolist())
            f_scn = st.selectbox("Scenario", scns, index=0)
        with fc4:
            f_fn  = st.selectbox(
                "Function",
                ["(All)"] + sorted(base["function_name"].dropna().astype(str).unique().tolist()),
                index=0
            )

        # apply filters
        plot = base.copy()
        if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
            start, end = date_range
            if start: plot = plot[plot["ts"] >= pd.Timestamp(start)]
            if end:   plot = plot[plot["ts"] < (pd.Timestamp(end) + pd.Timedelta(days=1))]
        if f_net != "(All)":
            plot = plot[plot["network"] == f_net]
        if f_scn != "(All)":
            plot = plot[plot["scenario"] == f_scn]
        if f_fn  != "(All)":
            plot = plot[plot["function_name"] == f_fn]

        # ===== badge + download =====
        b1, b2 = st.columns([2,1])
        with b1:
            avg_sr = (plot["succ"].mean() * 100) if len(plot) else 0.0
            st.caption(
                f"Menampilkan **{len(plot):,}** runs"
                + (f" | Network: **{f_net}**"   if f_net != "(All)" else "")
                + (f" | Scenario: **{f_scn}**"  if f_scn != "(All)" else "")
                + (f" | Function: **{f_fn}**"   if f_fn != "(All)" else "")
                + f" | Avg Success Rate: **{avg_sr:.1f}%**"
            )
        with b2:
            st.download_button(
                "⬇️ Download CSV (Filtered)",
                data=csv_bytes(plot.drop(columns=["ts","succ"], errors="ignore")),
                file_name="bench_runs_filtered.csv",
                mime="text/csv",
                use_container_width=True
            )

        # ===== metrics =====
        k1, k2, k3 = st.columns(3)
        k1.metric("TPS Peak", f"{pd.to_numeric(plot['tps_peak'], errors='coerce').max():,.2f}" if not plot.empty else "0")
        k2.metric("Latency p95 (ms)", f"{pd.to_numeric(plot['p95_ms'], errors='coerce').mean():,.0f}" if not plot.empty else "0")
        k3.metric("Success Rate", f"{avg_sr:.1f}%")

        # ===== charts =====
        c1, c2 = st.columns(2)
        with c1:
            fig = px.line(
                plot.sort_values("concurrency"),
                x="concurrency", y="tps_avg", color="scenario",
                markers=True, title="TPS vs Concurrency",
                labels={"concurrency":"Concurrency","tps_avg":"TPS Avg","scenario":"Scenario"},
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig, use_container_width=True)
            fig_export_buttons(fig, "bench_tps_vs_concurrency")
        with c2:
            lat = plot.melt(
                id_vars=["concurrency","scenario"],
                value_vars=["p50_ms","p95_ms"],
                var_name="metric", value_name="latency_ms"
            )
            fig = px.line(
                lat.sort_values("concurrency"),
                x="concurrency", y="latency_ms", color="metric",
                markers=True, title="Latency (p50/p95) vs Concurrency",
                labels={"concurrency":"Concurrency","latency_ms":"Latency (ms)","metric":"Metric"},
                template="plotly_white",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig, use_container_width=True)
            fig_export_buttons(fig, "bench_latency_vs_concurrency")

        # ===== table =====
        st.markdown("### Detail Runs")
        st.dataframe(plot, use_container_width=True)

        show_help("bench")
