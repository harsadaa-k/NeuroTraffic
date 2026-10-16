"""
NeuroTraffic dashboard — entry point
=====================================
Run:  streamlit run app.py

This file only defines the menu (the pages, their order and icons) and the
"Read data from HDFS" switch shown on every page. Each page lives in pages/.
"""

import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config
import hdfs_helper

if "use_hdfs" not in st.session_state:
    st.session_state.use_hdfs = config.USE_HDFS

pages = [
    st.Page("views/0_home.py",         title="Home",         icon="🏠", url_path="home", default=True),
    st.Page("views/1_live_monitor.py", title="Live Monitor", icon="📡", url_path="live_monitor"),
    st.Page("views/5_network_map.py",  title="Network Map",  icon="🗺", url_path="network_map"),
    st.Page("views/3_explorer.py",     title="Explorer",     icon="📊", url_path="explorer"),
    st.Page("views/2_simulator.py",    title="Simulator",    icon="⚡", url_path="simulator"),
]
page = st.navigation(pages)

with st.sidebar:
    st.toggle("Read data from HDFS", key="use_hdfs",
              help="On: read every file from HDFS through WebHDFS. Off: read the local copies in 08_dashboard/.")
    if st.session_state.use_hdfs:
        st.caption("● HDFS connected" if hdfs_helper.is_hdfs_available() else "○ HDFS not reachable — using local files")
    else:
        st.caption("Using local files")
    st.divider()

page.run()
