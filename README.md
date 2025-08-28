
# STC Analytics — Hybrid Dashboard

Satu tempat buat pantau **biaya gas (Vision)**, **temuan keamanan (SWC)**, dan **hasil benchmark (Bench)** Smart Contract Anda — cepat, ringan, dan terstruktur. Dibangun dengan **Streamlit** + **DuckDB**.

[![Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://stc-analytics.streamlit.app)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-yellow)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
[![Keep Alive](https://github.com/mrbrightsides/stc-analytics/actions/workflows/ping.yml/badge.svg)](https://github.com/mrbrightsides/stc-analytics/actions/workflows/ping.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16792530.svg)](https://doi.org/10.5281/zenodo.16792530)
![STC Ecosystem](https://img.shields.io/badge/STC%20Ecosystem-core-blue?logo=ethereum&logoColor=white)
![status: stable](https://img.shields.io/badge/status-stable-brightgreen)

<p align="center">
<img width="757" height="757" alt="stc-logo" src="https://github.com/user-attachments/assets/3a20844c-20c0-4f94-8e6d-0d25ffab49ff" />
</p>

---

## ✨ Fitur
- **Cost (Vision):** unggah CSV/NDJSON dari STC GasVision, lihat metrik & tren biaya gas per fungsi.
- **Security (SWC):** unggah temuan SWC (CSV/NDJSON), filter per network/severity, heatmap _SWC × Severity_, dan **SWC Knowledge** (penjelasan/mitigasi dari `swc_kb.json`).
- **Performance (Bench):** unggah hasil benchmark (`bench_runs.csv` & opsional `bench_tx.csv`), grafik TPS vs concurrency dan latensi p50/p95.
- **Templates & contoh data:** tombol unduh di setiap tab untuk memudahkan format.
- **Export hasil filter:** unduh CSV dari tabel yang sedang ditampilkan.
- **Privasi:** semua data lokal di **DuckDB**; tidak ada pengiriman data ke pihak ketiga.

---

## 🚀 Quick Start (Local)
```bash
# 1) Clone
git clone https://github.com/mrbrightsides/stc-analytics.git
cd stc-analytics

# 2) (Disarankan) Buat & aktifkan virtualenv
python -m venv .venv
# Windows PowerShell
. .venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

# 3) Install dependency
pip install -r requirements_stc.txt

# 4) Jalankan
streamlit run app_stc_analytics.py
```

> **Catatan Windows:** Bila PowerShell memblokir eksekusi, jalankan PS sebagai Admin lalu:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` (setelah itu tutup PS, buka lagi).

---

## 🗂️ Struktur Repo (ringkas)
```
stc-analytics/
├─ app_stc_analytics.py        # Aplikasi Streamlit (UI + logic)
├─ requirements_stc.txt        # Daftar dependency
├─ templates/                  # Template & contoh data
│  ├─ vision_template.csv
│  ├─ vision_sample.ndjson
│  ├─ swc_findings_template.csv
│  ├─ swc_findings_sample.ndjson
│  ├─ bench_runs_template.csv
│  └─ bench_tx_template.csv
├─ swc_kb.json                 # (Opsional) Pengetahuan SWC (judul, deskripsi, mitigasi)
├─ README.md                   # Dokumen ini
└─ .gitignore
```

---

## 📥 Format Input Data

### 1) Vision (Cost)
- **CSV (dari GasVision):** gunakan tombol **Template CSV (Vision)** atau **Contoh NDJSON (Vision)** di tab **Cost**.  
- Kolom standar (contoh header CSV):
  `Network, Tx Hash, From, To, Block, Gas Used, Gas Price (Gwei), Estimated Fee (ETH), Estimated Fee (Rp), Contract, Function, Timestamp, Status`

> **Sumber CSV:** buka **GasVision** → pilih **Network** → (opsional) masukkan Tx Hash yang ingin dicek → **Download CSV** → unggah ke tab Cost.

### 2) Security (SWC)
- **CSV/NDJSON** dengan kolom:
  `finding_id (opsional), timestamp, network, contract, file, line_start, line_end, swc_id, title, severity, confidence, status, remediation, commit_hash`  
- Jika `finding_id` kosong, aplikasi akan mengisi otomatis `contract::swc_id::line_start` dan _de-dup_ per batch.

### 3) Performance (Bench)
- **bench_runs.csv:** `run_id, timestamp, network, scenario, contract, function_name, concurrency, tx_per_user, tps_avg, tps_peak, p50_ms, p95_ms, success_rate`
- **bench_tx.csv (opsional):** `run_id, tx_hash, submitted_at, mined_at, latency_ms, status, gas_used, gas_price_wei, block_number, function_name`

---

## ⚙️ Variabel Lingkungan (opsional)
- `EDA_DB_PATH` — path file DuckDB untuk penyimpanan lokal (default: `stc_analytics.duckdb`).
- `SWC_KB_PATH` — path ke file pengetahuan SWC (default: `swc_kb.json`).

---

## 🧭 Alur Pakai (singkat)
1. **Upload** data (CSV/NDJSON) di tab yang sesuai.
2. (Opsional) aktifkan **Load existing stored data** untuk memuat data lokal yang sudah ada di DuckDB.
3. Gunakan **filter & date range** untuk eksplorasi.
4. **Export** hasil filter via tombol **Download CSV**.
5. Untuk **SWC Knowledge**, pastikan `swc_kb.json` tersedia (format _list_ atau _dict_ berindeks SWC-ID).

---

## 🧩 Troubleshooting
- **Data tidak tampil:** pastikan format kolom sesuai template; periksa encoding UTF-8; cek log saat upload.
- **PK/duplikasi:** untuk SWC, `finding_id` unik. Kosong? Aplikasi membuat fallback `contract::swc_id::line_start`.
- **DuckDB terkunci:** tutup sesi Streamlit lain yang masih mengakses file DB, lalu jalankan ulang.
- **Performa lambat:** bagi file besar menjadi beberapa berkas; kurangi jumlah kolom non-esensial saat eksplorasi.

---

## 🗺️ Roadmap (ringkas)
- Tambah date range picker untuk Vision (berbasis sumber data).
- Ringkasan otomatis temuan SWC per kontrak.
- Penyimpanan konfigurasi user (dark/light, default network).

---

## 📄 Lisensi
MIT — silakan gunakan dan sesuaikan sesuai kebutuhan.

---

## 🙌 Kontribusi & Kontak
- Laporkan bug/fitur baru: **Issues** pada repo.
- Hubungi: https://github.com/mrbrightsides
