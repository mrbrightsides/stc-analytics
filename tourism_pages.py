def map_csv_cost(df_raw: pd.DataFrame) -> pd.DataFrame:
    # mapping CSV Vision -> schema standar
    m = {
        "Network":"network","Tx Hash":"tx_hash","From":"from_address","To":"to_address",
        "Block":"block_number","Gas Used":"gas_used","Gas Price (Gwei)":"gas_price_gwei",
        "Estimated Fee (ETH)":"cost_eth","Estimated Fee (Rp)":"cost_idr",
        "Contract":"contract","Function":"function_name","Timestamp":"timestamp","Status":"status"
    }
    df = df_raw.rename(columns=m).copy()
    n = len(df)

    # helper: safe getter → selalu return Series
    def S(col, default=""):
        ser = df.get(col)
        if ser is None:
            return pd.Series([default] * n, index=df.index)
        return ser

    # field wajib/umum
    df["project"]   = "STC"
    ts = pd.to_datetime(S("timestamp"), errors="coerce")
    df["timestamp"] = ts.fillna(pd.Timestamp.utcnow())

    # numeric-safe
    gwei = pd.to_numeric(S("gas_price_gwei", 0), errors="coerce").fillna(0)
    df["gas_price_wei"] = (gwei * 1_000_000_000).round().astype("Int64")
    df["block_number"]   = pd.to_numeric(S("block_number"), errors="coerce").astype("Int64")
    df["gas_used"]       = pd.to_numeric(S("gas_used"), errors="coerce").astype("Int64")
    df["cost_eth"]       = pd.to_numeric(S("cost_eth"), errors="coerce")
    df["cost_idr"]       = pd.to_numeric(S("cost_idr"), errors="coerce")

    # meta_json aman
    st_ser = S("status").astype(str)
    df["meta_json"] = st_ser.apply(lambda s: json.dumps({"status": s}) if s and s != "nan" else "{}")

    # buat ID stabil: pakai tx_hash + function_name; fallback ke hash isi baris
    tx   = S("tx_hash").astype(str).fillna("")
    func = S("function_name").astype(str).fillna("")
    base = (tx + "::" + func).astype(str)
    df["id"] = base

    is_dummy = tx.eq("") | tx.str.contains(r"\.\.\.")
    if is_dummy.any():
        unique_fallback = (
            df.astype(str).agg("|".join, axis=1)
              .map(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest()[:16])
        )
        df.loc[is_dummy, "id"] = "csv::" + unique_fallback[is_dummy]

    # pastikan semua kolom target ada
    cols = ["id","project","network","timestamp","tx_hash","contract","function_name",
            "block_number","gas_used","gas_price_wei","cost_eth","cost_idr","meta_json"]
    for c in cols:
        if c not in df.columns:
            df[c] = None

    df = df.drop_duplicates(subset=["id"], keep="last")
    return df[cols]
