import streamlit as st

def render_bench_validation_db(get_conn_fn):
    con = get_conn_fn()

    # --- hitung dari TABEL (setelah insert) ---
    rows_runs = con.execute("SELECT COUNT(*) FROM bench_runs").fetchone()[0]
    rows_tx   = con.execute("SELECT COUNT(*) FROM bench_tx"  ).fetchone()[0]
    match_cnt = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT r.run_id
            FROM bench_runs r
            JOIN bench_tx   t ON r.run_id = t.run_id
        )
    """).fetchone()[0]

    # --- tampilkan metrik (selaras dengan atas) ---
    c1, c2, c3 = st.columns(3)
    c1.metric("Rows (runs)", rows_runs)
    c2.metric("Rows (tx)",   rows_tx)
    c3.metric("run_id match", match_cnt)

    # --- cek kolom wajib dari schema DB (bukan dari CSV) ---
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

    con.close()
