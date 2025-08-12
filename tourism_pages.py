import streamlit as st, duckdb, pandas as pd, json, re
# import juga helpermu: csv_bytes, read_csv_any, get_conn, ensure_db, drop_all, upsert, DB_PATH, plotly.express as px, ...

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
