def render_bench_validation(runs, tx):
    import streamlit as st
    import pandas as pd

    REQ_RUNS = {
        "run_id","timestamp","network","scenario","contract","function_name",
        "concurrency","tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"
    }
    REQ_TX = {
        "run_id","tx_hash","submitted_at","mined_at","latency_ms","status",
        "gas_used","gas_price_wei","block_number","function_name"
    }

    con = get_conn()

    # --- hitung dari TABEL (bukan dari CSV variabel) ---
    rows_runs = con.execute("SELECT COUNT(*) FROM bench_runs").fetchone()[0]
    rows_tx   = con.execute("SELECT COUNT(*) FROM bench_tx"  ).fetchone()[0]
    match_cnt = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT r.run_id
            FROM bench_runs r
            JOIN bench_tx   t ON r.run_id = t.run_id
        )
    """).fetchone()[0]

    # --- tampilkan metrik ---
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows (runs)", rows_runs)
    c2.metric("Rows (tx)",   rows_tx)
    c3.metric("run_id match", match_cnt)

    # --- cek kolom wajib dari schema DB (PRAGMA) ---
    req_runs = [
        "run_id","timestamp","network","scenario","contract","function_name",
        "concurrency","tx_per_user","tps_avg","tps_peak","p50_ms","p95_ms","success_rate"
    ]
    req_tx = [
        "run_id","tx_hash","submitted_at","mined_at","latency_ms","status",
        "gas_used","gas_price_wei","block_number","function_name"
    ]

    have_runs = [row[1] for row in con.execute("PRAGMA table_info('bench_runs')").fetchall()]
    have_tx   = [row[1] for row in con.execute("PRAGMA table_info('bench_tx')"  ).fetchall()]

    missing_runs = [c for c in req_runs if c not in have_runs]
    missing_tx   = [c for c in req_tx   if c not in have_tx]

    if missing_runs:
        st.error(f"Kolom wajib hilang di bench_runs.csv: {missing_runs}")
    if missing_tx:
        st.error(f"Kolom wajib hilang di bench_tx.csv: {missing_tx}")

    # --- debug (opsional) ---
    with st.expander("🔎 Debug run_id (DB view)", expanded=False):
        colA, colB, colC = st.columns(3)
        colA.dataframe(con.execute(
            "SELECT DISTINCT run_id FROM bench_runs ORDER BY run_id LIMIT 25"
        ).fetchdf(), use_container_width=True)
        colB.dataframe(con.execute(
            "SELECT DISTINCT run_id FROM bench_tx ORDER BY run_id LIMIT 25"
        ).fetchdf(), use_container_width=True)
        colC.dataframe(con.execute("""
            SELECT DISTINCT r.run_id
            FROM bench_runs r JOIN bench_tx t ON r.run_id=t.run_id
            ORDER BY r.run_id LIMIT 25
        """).fetchdf(), use_container_width=True)

    con.close()

    def chip(label: str, ok: bool):
        color = "#16a34a" if ok else "#e11d48"
        bg = "rgba(22,163,74,.12)" if ok else "rgba(225,29,72,.12)"
        icon = "✅" if ok else "✖️"
        st.markdown(
            f"""<span style="
                display:inline-block;padding:.35rem .6rem;border-radius:9999px;
                background:{bg};color:{color};font-weight:600;border:1px solid {color}33;
            ">{icon} {label}</span>""",
            unsafe_allow_html=True,
        )

    def normalize_run_id(df: pd.DataFrame, col="run_id") -> pd.DataFrame:
        if df is None or col not in df.columns: 
            return df
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"\s+", "", regex=True)
            .str.strip()
        )
        return df

    def has_required(df, req):
        return df is not None and req.issubset(set(df.columns))

    def read_csv_any(f):
        if f is None:
            return None
        try:
            return pd.read_csv(f)
        except Exception:
            f.seek(0)
            return pd.read_csv(f, sep=";")

    df_runs = read_csv_any(runs)
    df_tx   = read_csv_any(tx)

    ok_runs_cols = has_required(df_runs, REQ_RUNS)
    ok_tx_cols   = has_required(df_tx, REQ_TX)

    join_ok = False
    match_count = 0
    miss_tx_ids = []
    miss_runs_ids = []

    if ok_runs_cols:
        df_runs = normalize_run_id(df_runs, "run_id")
    if ok_tx_cols:
        df_tx   = normalize_run_id(df_tx, "run_id")

    if ok_runs_cols and ok_tx_cols:
        ids_runs = set(df_runs["run_id"].dropna().astype(str))
        ids_tx   = set(df_tx["run_id"].dropna().astype(str))

        match_count   = len(ids_runs & ids_tx)
        miss_tx_ids   = sorted(list(ids_tx - ids_runs))[:8]
        miss_runs_ids = sorted(list(ids_runs - ids_tx))[:8]
        join_ok       = (match_count > 0 and len(ids_tx - ids_runs) == 0)

    c1, c2, c3 = st.columns([1,1,1])
    with c1: chip("bench_runs.csv", ok_runs_cols)
    with c2: chip("bench_tx.csv", ok_tx_cols)
    with c3: chip("JOIN run_id", join_ok)

    m1, m2, m3 = st.columns(3)
    with m1: st.metric("Rows (runs)", 0 if df_runs is None else len(df_runs))
    with m2: st.metric("Rows (tx)",   0 if df_tx   is None else len(df_tx))
    with m3: st.metric("run_id match", match_count)

    if df_runs is None:
        st.info("🗂️ Unggah **bench_runs.csv**.")
    elif not ok_runs_cols:
        missing = sorted(list(REQ_RUNS - set(df_runs.columns)))
        st.error(f"Kolom wajib hilang di **bench_runs.csv**: {missing}")

    if df_tx is None:
        st.info("🗂️ Unggah **bench_tx.csv**.")
    elif not ok_tx_cols:
        missing = sorted(list(REQ_TX - set(df_tx.columns)))
        st.error(f"Kolom wajib hilang di **bench_tx.csv**: {missing}")

    if ok_runs_cols and ok_tx_cols and not join_ok:
        st.warning("Sebagian `run_id` tidak saling cocok antar file.")
        colA, colB = st.columns(2)
        with colA:
            st.caption("Contoh `run_id` ada di TX tapi tidak di RUNS")
            st.code(", ".join(miss_tx_ids) if miss_tx_ids else "—")
        with colB:
            st.caption("Contoh `run_id` ada di RUNS tapi tidak di TX")
            st.code(", ".join(miss_runs_ids) if miss_runs_ids else "—")

    ready = ok_runs_cols and ok_tx_cols and join_ok
    if ready:
        st.success("✅ Siap dianalisis. Grafik & metrik diaktifkan di bawah ini.")
    else:
        st.info("ℹ️ Unggah **kedua file** dengan struktur kolom benar & `run_id` yang saling cocok untuk mengaktifkan analitik.")
