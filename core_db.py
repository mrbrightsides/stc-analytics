# core_db.py
import duckdb, pandas as pd

DB_PATH = "stc.duckdb"

def get_conn():
    return duckdb.connect(DB_PATH)

def ensure_db():
    con = get_conn()
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
    con.execute("""
        CREATE TABLE IF NOT EXISTS swc_findings (
            finding_id TEXT PRIMARY KEY,
            timestamp TIMESTAMP,
            network TEXT,
            contract TEXT,
            file TEXT,
            line_start BIGINT,
            line_end BIGINT,
            swc_id TEXT,
            title TEXT,
            severity TEXT,
            confidence DOUBLE,
            status TEXT,
            remediation TEXT,
            commit_hash TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bench_runs (
            run_id TEXT PRIMARY KEY,
            timestamp TIMESTAMP,
            network TEXT,
            scenario TEXT,
            contract TEXT,
            function_name TEXT,
            concurrency BIGINT,
            tx_per_user BIGINT,
            tps_avg DOUBLE,
            tps_peak DOUBLE,
            p50_ms DOUBLE,
            p95_ms DOUBLE,
            success_rate DOUBLE
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS bench_tx (
            run_id TEXT,
            tx_hash TEXT,
            submitted_at TIMESTAMP,
            mined_at TIMESTAMP,
            latency_ms DOUBLE,
            status TEXT,
            gas_used BIGINT,
            gas_price_wei BIGINT,
            block_number BIGINT,
            function_name TEXT
        );
    """)
    con.close()

def drop_all():
    con = get_conn()
    for t in ["vision_costs", "swc_findings", "bench_runs", "bench_tx"]:
        con.execute(f"DROP TABLE IF EXISTS {t};")
    con.close()

def upsert(table: str, df: pd.DataFrame, keys: list[str], cols: list[str]) -> int:
    if df.empty:
        return 0
    con = get_conn()
    con.execute(f"CREATE TEMP TABLE stg AS SELECT * FROM {table} WITH NO DATA;")
    con.register("df_stage", df[cols])
    con.execute("INSERT INTO stg SELECT * FROM df_stage;")
    pk = " AND ".join([f"{table}.{k}=d.{k}" for k in keys])
    ksel = ", ".join([f"{k}" for k in keys])
    con.execute(f"""
        DELETE FROM {table} USING (
            SELECT DISTINCT {ksel} FROM stg
        ) d
        WHERE {pk};
    """)
    con.execute(f"INSERT INTO {table} SELECT * FROM stg;")
    n = con.execute("SELECT COUNT(*) FROM stg").fetchone()[0]
    con.close()
    return int(n)

# util CSV lokal
def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def read_csv_any(file) -> pd.DataFrame:
    return pd.read_csv(file)
