import streamlit as st, duckdb, pandas as pd, json, re
import os, io
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
from pathlib import Path
from core_db import (
    DB_PATH, get_conn, ensure_db, drop_all, upsert,
    csv_bytes, read_csv_any
)
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
    # -------------------------------
    # COST (Vision)
    # -------------------------------
    st.title("💰 Cost Analytics — STC Vision")

    # --- helper: mapping CSV Vision -> schema standar ---
    def map_csv_cost(df_raw: pd.DataFrame) -> pd.DataFrame:
        m = {
            "Network": "network", "Tx Hash": "tx_hash", "From": "from_address", "To": "to_address",
            "Block": "block_number", "Gas Used": "gas_used", "Gas Price (Gwei)": "gas_price_gwei",
            "Estimated Fee (ETH)": "cost_eth", "Estimated Fee (Rp)": "cost_idr",
            "Contract": "contract", "Function": "function_name", "Timestamp": "timestamp", "Status": "status"
        }
        df = df_raw.rename(columns=m).copy()

        df["project"] = "STC"
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
        else:
            df["timestamp"] = pd.Timestamp.utcnow()

        gwei = pd.to_numeric(df.get("gas_price_gwei", 0), errors="coerce").fillna(0)
        df["gas_price_wei"] = (gwei * 1_000_000_000).round().astype("Int64")

        status_series = df.get("status")
        if status_series is not None:
            df["meta_json"] = status_series.astype(str).apply(lambda s: json.dumps({"status": s}) if s else "{}")
        else:
            df["meta_json"] = "{}"

        df["id"] = df.apply(lambda r: f"{r.get('tx_hash','')}::{(r.get('function_name') or '')}".strip(), axis=1)

        cols = [
            "id", "project", "network", "timestamp", "tx_hash", "contract", "function_name",
            "block_number", "gas_used", "gas_price_wei", "cost_eth", "cost_idr", "meta_json"
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df["block_number"] = pd.to_numeric(df["block_number"], errors="coerce").astype("Int64")
        df["gas_used"] = pd.to_numeric(df["gas_used"], errors="coerce").astype("Int64")
        df["cost_eth"] = pd.to_numeric(df["cost_eth"], errors="coerce")
        df["cost_idr"] = pd.to_numeric(df["cost_idr"], errors="coerce")
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
            "Network", "Tx Hash", "From", "To", "Block", "Gas Used", "Gas Price (Gwei)",
            "Estimated Fee (ETH)", "Estimated Fee (Rp)", "Contract", "Function", "Timestamp", "Status"
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
                if "meta_json" not in d.columns:
                    if "meta" in d.columns:
                        d["meta_json"] = d["meta"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else (x if x else "{}"))
                    else:
                        d["meta_json"] = "{}"

                cols = [
                    "id", "project", "network", "timestamp", "tx_hash", "contract", "function_name",
                    "block_number", "gas_used", "gas_price_wei", "cost_eth", "cost_idr", "meta_json"
                ]
                for c in cols:
                    if c not in d.columns:
                        d[c] = None

                d["project"] = d.get("project").fillna("STC")
                d["timestamp"] = pd.to_datetime(d["timestamp"], errors="coerce").fillna(pd.Timestamp.utcnow())
                for numc in ["block_number", "gas_used", "gas_price_wei", "cost_eth", "cost_idr"]:
                    d[numc] = pd.to_numeric(d[numc], errors="coerce")

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

    # ==== Load & tampilkan data (DI LUAR EXPANDER, masih di halaman) ====
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
                + (f" | Network: **{f_net}**" if f_net != "(All)" else "")
                + (f" | Function: **{f_fn}**" if f_fn != "(All)" else "")
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
                fig = px.line(
                    ts, x="ts", y=y, color="network", markers=not do_smooth,
                    title="Biaya per Transaksi (Rp) vs Waktu",
                    labels={"ts": "Waktu", y: "Biaya (Rp)", "network": "Jaringan"},
                )
                if line_log:
                    fig.update_yaxes(type="log")
                st.plotly_chart(fig, use_container_width=True)

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
                 )
                fig.update_xaxes(categoryorder="total descending")
                st.plotly_chart(fig, use_container_width=True)

        sc = df_plot[(df_plot["gas_used_num"] > 0) & (df_plot["gas_price_num"] > 0)].copy()
        if not sc.empty:
            sc["tx_short"] = sc["tx_hash"].astype(str).map(short_tx)
            sc["cost_str"] = sc["cost_idr_num"].round().astype(int).map(lambda v: f"{v:,}")
            sc["gas_used_str"] = sc["gas_used_num"].round().astype(int).map(lambda v: f"{v:,}")
            sc["gas_price_str"] = sc["gas_price_num"].round().astype(int).map(lambda v: f"{v:,}")
            sc["explorer_url"] = sc.apply(lambda r: explorer_tx_url(r["network"], r["tx_hash"]), axis=1)

            fig = px.scatter(
                sc, x="gas_used_num", y="gas_price_num", size="cost_idr_num", color="network",
                title="Gas Used vs Gas Price (size = Biaya Rp)",
                labels={"gas_used_num": "Gas Used", "gas_price_num": "Gas Price (wei)", "network": "Jaringan"},
                hover_data=None,
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

def render_swc_page():
    # -------------------------------
    # SECURITY (SWC)
    # -------------------------------
    st.title("🛡️ Security Analytics — STC for SWC")

    # --- mapping CSV/NDJSON -> schema + id fallback + dedup ---
    def map_swc(df: pd.DataFrame) -> pd.DataFrame:
        cols = [
            "finding_id","timestamp","network","contract","file","line_start","line_end",
            "swc_id","title","severity","confidence","status","remediation","commit_hash"
        ]
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

    # --- Load data ---
    con = get_conn()
    swc_df = con.execute("SELECT * FROM swc_findings ORDER BY timestamp DESC").df()
    con.close()

    if swc_df.empty:
        st.info("Belum ada data temuan SWC.")
        return

    # base + helpers
    swc_base = swc_df.copy()
    swc_base["ts"] = pd.to_datetime(swc_base["timestamp"], errors="coerce")
    swc_base["sev"] = swc_base["severity"].fillna("(unknown)")
    swc_base["conf_num"] = pd.to_numeric(swc_base.get("confidence", 0), errors="coerce").fillna(0.0)

    # filters (mirip Vision)
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
        if start: swc_plot = swc_plot[swc_plot["ts"] >= pd.Timestamp(start)]
        if end:   swc_plot = swc_plot[swc_plot["ts"] < (pd.Timestamp(end) + pd.Timedelta(days=1))]
    if f_net != "(All)":
        swc_plot = swc_plot[swc_plot["network"] == f_net]
    if f_sev != "(All)":
        swc_plot = swc_plot[swc_plot["sev"] == f_sev]

    # badge + download
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

    # metrics
    total = len(swc_plot)
    high  = (swc_plot["sev"].astype(str).str.lower() == "high").sum()
    uniq  = swc_plot["swc_id"].nunique()
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Findings", f"{total:,}")
    m2.metric("High Severity", f"{high:,}")
    m3.metric("Unique SWC IDs", f"{uniq:,}")

    # heatmap
    pivot = swc_plot.pivot_table(index="swc_id", columns="sev", values="finding_id",
                                 aggfunc="count", fill_value=0)
    if not pivot.empty:
        fig = px.imshow(pivot, text_auto=True, aspect="auto", title="SWC-ID × Severity (count)")
        st.plotly_chart(fig, use_container_width=True)

    # table
    st.markdown("### Detail Temuan")
    detail_cols = ["timestamp","network","contract","file","line_start","swc_id","title",
                   "severity","confidence","status","remediation"]
    st.dataframe(swc_plot[detail_cols], use_container_width=True)
    st.download_button(
        "⬇️ Download tabel di atas (CSV)",
        data=csv_bytes(swc_plot[detail_cols]),
        file_name="swc_table_filtered.csv",
        mime="text/csv",
        use_container_width=True
    )

    # SWC Knowledge
    st.markdown("### 🔎 SWC Knowledge")
    kb = load_swc_kb()
    if not kb:
        st.warning("SWC KB JSON belum ditemukan. Letakkan file **swc_kb.json** atau set env `SWC_KB_PATH`.")
        return

    available_ids = sorted(swc_plot["swc_id"].dropna().astype(str).unique().tolist())
    if not available_ids:
        st.info("Tidak ada SWC-ID pada data saat ini.")
        return

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

def render_bench_page():
    # -------------------------------
    # PERFORMANCE (Bench)
    # -------------------------------
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
                d["mined_at"]     = pd.to_datetime(d["mined_at"], errors="coerce")

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
            st.download_button("⬇️ Template bench_runs.csv", data=csv_bytes(tpl_runs),
                               file_name="bench_runs_template.csv", mime="text/csv",
                               use_container_width=True)
        with dcol2:
            st.download_button("⬇️ Template bench_tx.csv", data=csv_bytes(tpl_tx),
                               file_name="bench_tx_template.csv", mime="text/csv",
                               use_container_width=True)

    # ===== di luar expander =====
    want_load = st.session_state.get("load_existing", False)
    no_new_upload = (st.session_state.get("runs_csv") is None) and (st.session_state.get("tx_csv") is None)
    if no_new_upload and not want_load:
        st.info("Belum ada data benchmark untuk sesi ini. Upload bench_runs/bench_tx atau aktifkan ‘Load existing stored data’.")
        st.stop()

    con = get_conn()
    runs_df = con.execute("SELECT * FROM bench_runs ORDER BY timestamp DESC").df()
    con.close()

    if runs_df.empty:
        st.info("Belum ada data benchmark.")
        return

    # ===== base + helper cols =====
    base = runs_df.copy()
    base["ts"]   = pd.to_datetime(base["timestamp"], errors="coerce")
    base["succ"] = pd.to_numeric(base.get("success_rate", 0), errors="coerce").fillna(0.0)

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
        f_fn  = st.selectbox("Function",
                             ["(All)"] + sorted(base["function_name"].dropna().astype(str).unique().tolist()),
                             index=0)

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
    k1.metric("TPS Peak", f"{plot['tps_peak'].max():,.2f}" if not plot.empty else "0")
    k2.metric("Latency p95 (ms)", f"{plot['p95_ms'].mean():,.0f}" if not plot.empty else "0")
    k3.metric("Success Rate", f"{avg_sr:.1f}%")

    # ===== charts =====
    c1, c2 = st.columns(2)
    with c1:
        fig = px.line(
            plot.sort_values("concurrency"),
            x="concurrency", y="tps_avg", color="scenario",
            markers=True, title="TPS vs Concurrency",
            labels={"concurrency":"Concurrency","tps_avg":"TPS Avg","scenario":"Scenario"}
        )
        st.plotly_chart(fig, use_container_width=True)
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
            labels={"concurrency":"Concurrency","latency_ms":"Latency (ms)","metric":"Metric"}
        )
        st.plotly_chart(fig, use_container_width=True)

    # ===== table =====
    st.markdown("### Detail Runs")
    st.dataframe(plot, use_container_width=True)

    show_help("bench")
