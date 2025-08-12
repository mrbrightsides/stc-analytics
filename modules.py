import streamlit as st

def module_tourism(render_cost, render_swc, render_bench):
    st.markdown("## 🌍 Module: Tourism (SmartTourismChain)")
    tab1, tab2, tab3 = st.tabs(["Cost (Vision)", "Security (SWC)", "Performance (Bench)"])
    with tab1: render_cost()
    with tab2: render_swc()
    with tab3: render_bench()

def module_finance():
    st.markdown("## 💹 Module: Finance (DeFi)")
    st.markdown("<h1 style='text-align: center; color: gray;'>COMING SOON</h1>", unsafe_allow_html=True)

def module_nft():
    st.markdown("## 🎨 Module: NFT/Token")
    st.markdown("<h1 style='text-align: center; color: gray;'>COMING SOON</h1>", unsafe_allow_html=True)

def module_supplychain():
    st.markdown("## 🚚 Module: Supply Chain")
    st.markdown("<h1 style='text-align: center; color: gray;'>COMING SOON</h1>", unsafe_allow_html=True)

def module_custom():
    st.markdown("## 🛠 Module: Custom Monitor")
    st.markdown("<h1 style='text-align: center; color: gray;'>COMING SOON</h1>", unsafe_allow_html=True)
