import streamlit as st

def sidebar_nav():
    st.sidebar.markdown("## 📌 STC Analytics (core)")
    st.sidebar.divider()

    st.sidebar.markdown("### 🧩 Modules")
    module = st.sidebar.radio(
        "Pilih module",
        [
            "Tourism",
            "Finance (DeFi)",
            "NFT/Token",
            "Supply Chain",
            "Custom Monitor"
        ],
        key="module_choice"
    )

    st.sidebar.markdown("### 🛠 Tools")
    tool = st.sidebar.radio(
        "Pilih tool",
        ["Scan", "Test", "Contract"],
        key="tool_choice"
    )

    return module, tool
