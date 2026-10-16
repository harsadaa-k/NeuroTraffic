"""
NeuroTraffic — Home
====================
What is happening right now, what the historical analysis found,
where to go next, how the system works, and whether every data file is present.
"""

import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config
import hdfs_helper
import ui

NYC = ZoneInfo("America/New_York")

ui.setup_page("NeuroTraffic", "🚊",
              "Multi-modal transport disruption detection for New York City. Taxi, subway, Citi Bike, weather, "
              "311 complaints and events are processed with Hadoop and Spark; every taxi zone gets an anomaly "
              "score between 0 (normal) and 1 (disruption).")
use_hdfs = ui.use_hdfs()


@st.cache_data(ttl=10)
def load_live(use_hdfs):
    latest = hdfs_helper.load_dataframe(config.LATEST_CSV, config.HDFS_LATEST_CSV, use_hdfs=use_hdfs)
    status = hdfs_helper.load_json(config.SOURCE_STATUS_JSON, config.HDFS_SOURCE_STATUS_JSON, use_hdfs=use_hdfs)
    return latest, status or {}


# ── Right now ─────────────────────────────────────────────────────────
latest, status = load_live(use_hdfs)
ui.section("Right now", "Latest snapshot from the live pipeline (fetcher every 10 s → Spark every 15 s).", "live")
if latest is None or len(latest) == 0:
    st.warning("No live data yet. Start `07_streaming/realtime_fetcher.py` and `07_streaming/live_pipeline.scala`.")
else:
    snapshot = pd.to_datetime(latest["timestamp"].max())
    age_min = (datetime.now(NYC) - snapshot.tz_localize(NYC)).total_seconds() / 60
    levels = latest["anomaly_score"].apply(lambda s: ui.level(s)[0])
    alerted = [l["line"] for l in status.get("subway_lines", []) if l.get("alert_severity", 0) > 0]
    if age_min > 10:
        st.warning(f"The newest snapshot is {age_min:.0f} minutes old — the live pipeline is not running.")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Zones in disruption", int((levels == "Disruption").sum()), help="Anomaly score ≥ 0.80")
    c2.metric("Zones with an anomaly", int((levels == "Anomaly").sum()), help="Anomaly score 0.50 – 0.80")
    c3.metric("Subway lines with an alert", len(alerted), help=", ".join(alerted) or "none")
    c4.metric("Snapshot (NYC time)", f"{snapshot:%H:%M:%S}", help=f"{age_min:.1f} minutes ago")

# ── Pages ─────────────────────────────────────────────────────────────
ui.section("Pages")
PAGES = [
    ("views/1_live_monitor.py", "📡 Live Monitor", "Every zone's live anomaly score on a map, and how fresh each data source is."),
    ("views/5_network_map.py",  "🗺 Network Map",  "Subway lines, stations, Citi Bike docks and the links between them, live."),
    ("views/3_explorer.py",     "📊 Explorer",     "The last 24 hours of live scores, and three years of history by day and zone."),
    ("views/2_simulator.py",    "⚡ Simulator",    "What-if: pick a subway line and a time, see how a disruption could spread."),
]
cols = st.columns(len(PAGES))
for col, (path, label, desc) in zip(cols, PAGES):
    with col:
        st.page_link(path, label=label)
        st.caption(desc)

# ── How the system works ──────────────────────────────────────────────
ui.section("How the system works")
st.markdown("""
<div class='nt-card' style='font-size:0.85rem;line-height:1.9;color:#cbd5e1;'>
<b>Batch (2022 – 2024)</b><br>
1. <b>Ingest</b> — download taxi, subway, Citi Bike, weather, 311 and event data; upload to HDFS.<br>
2. <b>Clean</b> — Spark (Scala) removes bad rows and standardises each source.<br>
3. <b>Join</b> — one table with a row per taxi zone per hour: 263 zones × 26,304 hours = 6.9 million rows.<br>
4. <b>Graph</b> — Spark GraphX: PageRank (how central a zone is) and communities.<br>
5. <b>Detect</b> — z-scores + KMeans label unusual hours; a GBT classifier learns to recognise them.<br>
<b>Live</b><br>
6. <b>Fetch</b> — every 10 s: weather, MTA subway alerts and trains, road speeds, Citi Bike, 311, events → HDFS.<br>
7. <b>Score</b> — Spark Structured Streaming scores every zone every 15 s and keeps a 24-hour history.<br>
8. <b>Show</b> — this dashboard reads the results from HDFS.
</div>""", unsafe_allow_html=True)


# ── Data files ────────────────────────────────────────────────────────
ui.section("Data files", "Every file the dashboard reads, where it was found and when it was last updated.")
FILES = [
    ("latest.csv",                     config.LATEST_CSV,              config.HDFS_LATEST_CSV,              "Live score per zone"),
    ("live_history.csv",               os.path.join(config.STREAMING_OUTPUT_DIR, "live_history.csv"),
                                                                       config.HDFS_LIVE_HISTORY_CSV,        "Last 24 h of live scores"),
    ("source_status.json",             config.SOURCE_STATUS_JSON,      config.HDFS_SOURCE_STATUS_JSON,      "Freshness of each live source"),
    ("bike_docks.json",                config.BIKE_DOCKS_JSON,         config.HDFS_BIKE_DOCKS_JSON,         "Live Citi Bike docks"),
    ("graph_nodes.csv",                config.GRAPH_NODES_CSV,         config.HDFS_GRAPH_NODES_CSV,         "Zones + graph features"),
    ("historical_predictions.parquet", config.HISTORICAL_PARQUET,      config.HDFS_HISTORICAL_PARQUET,      "2022 – 2024 history"),
    ("evaluation_metrics.json",        config.EVALUATION_METRICS_JSON, config.HDFS_EVALUATION_METRICS_JSON, "Model metrics"),
    ("feature_importance.csv",         config.FEATURE_IMPORTANCE_CSV,  config.HDFS_FEATURE_IMPORTANCE_CSV,  "Model feature weights"),
    ("confusion_matrix.csv",           config.CONFUSION_MATRIX_CSV,    config.HDFS_CONFUSION_MATRIX_CSV,    "Model errors"),
]

def file_info(local_path, hdfs_path):
    """(where, size in bytes, last modified) — HDFS first if switched on, else the local copy."""
    if use_hdfs:
        try:
            r = requests.get(f"{config.HDFS_WEB_PREFIX}{hdfs_path}?op=GETFILESTATUS", timeout=2)
            if r.status_code == 200:
                fs = r.json()["FileStatus"]
                return "HDFS", fs["length"], datetime.fromtimestamp(fs["modificationTime"] / 1000)
        except requests.RequestException:
            pass
    if os.path.exists(local_path):
        return "Local", os.path.getsize(local_path), datetime.fromtimestamp(os.path.getmtime(local_path))
    return None, 0, None

rows = []
for name, local_path, hdfs_path, purpose in FILES:
    where, size, modified = file_info(local_path, hdfs_path)
    rows.append({"": "✅" if where else "❌", "File": name, "Purpose": purpose, "Found in": where or "missing",
                 "Size": f"{size / 1048576:.1f} MB" if size > 1048576 else f"{size / 1024:.1f} KB",
                 "Last updated (your time)": f"{modified:%d %b %H:%M:%S}" if modified else "—"})
st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
