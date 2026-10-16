# NeuroTraffic — Complete Code Guide

A file-by-file explanation of every coding file in the project: what it does, what it reads, what it writes, the logic step by step, and the libraries it uses.

Read it top to bottom the first time. Part 1 gives you the big picture and the concepts, Part 2 explains each file, Part 3 has the run order and the honest notes you should know before presenting.

---

## Contents

**Part 1 — Big picture**
1. [What the project does](#1-what-the-project-does)
2. [The pipeline in one diagram](#2-the-pipeline-in-one-diagram)
3. [Core concepts you must know](#3-core-concepts-you-must-know)
4. [Libraries used and what each one does](#4-libraries-used-and-what-each-one-does)
5. [Where the data lives (HDFS layout)](#5-where-the-data-lives-hdfs-layout)

**Part 2 — Every file explained**
- [Infrastructure files](#infrastructure-files)
- [01_ingestion — downloading the data](#01_ingestion--downloading-the-data)
- [02_cleaning — cleaning each dataset](#02_cleaning--cleaning-each-dataset)
- [03_integration — joining everything](#03_integration--joining-everything-into-one-table)
- [04_mapreduce — MapReduce example](#04_mapreduce--mapreduce-example)
- [05_graph — graph analytics](#05_graph--graph-analytics-graphx)
- [06_ml — anomaly detection and ML](#06_ml--anomaly-detection-and-machine-learning)
- [07_streaming — live data pipeline](#07_streaming--live-data-pipeline)
- [08_dashboard — the web UI](#08_dashboard--the-web-ui-streamlit)

**Part 3 — Putting it together**
- [Full run order](#full-run-order)
- [Honest notes: what is real, what is synthetic, known quirks](#honest-notes-what-is-real-what-is-synthetic-known-quirks)
- [Glossary](#glossary)

---

# Part 1 — Big picture

## 1. What the project does

NeuroTraffic is a **multi-modal transport intelligence system for New York City**. "Multi-modal" means it combines several ways of moving around the city:

| Mode / source | Dataset |
|---|---|
| Taxi | NYC TLC Yellow, Green and HVFHV (Uber/Lyft) trip records |
| Subway | MTA hourly ridership per station |
| Bike | Citi Bike trip records |
| Weather | Hourly weather (Open-Meteo / NOAA) |
| Complaints | NYC 311 service requests (transport-related types) |
| Events | NYC event permits + film permits |

Years covered: **2022–2024**. About 350 GB raw, 3+ billion records.

The goal: **detect transport disruptions** (for example a subway line failing) by spotting the *cross-modal signature*: subway ridership drops while taxi and bike usage jump in the same zone, because people switch modes.

It has two halves:
- **Batch (historical):** clean and join 3 years of data → graph analytics → train an ML model → dashboard charts.
- **Live (streaming):** fetch current NYC data every 10 seconds → score every zone with Spark Structured Streaming → live dashboard map.

The city is divided into **263 taxi zones** (official TLC zones). Almost everything in the project is organised as **zone × hour**.

---

## 2. The pipeline in one diagram

```
                         ┌──────────────── BATCH (historical, 2022-2024) ────────────────┐
 Public websites         │                                                               │
 (TLC, MTA, Citi Bike,   │  01_ingestion        02_cleaning        03_integration        │
  Open-Meteo, NYC 311,   │  download_datasets → clean_*.scala  →  join_all.scala  ──┐    │
  NYC events)  ──────────┼► .py (raw files)     (cleaned/ in      → combined        │    │
                         │   hdfs dfs -put       Parquet)          add_bike → v2     │    │
                         │   → HDFS raw/                           fix_311 → v3 ◄────┘    │
                         │                                              │                 │
                         │        ┌─────────────────────────────────────┼──────────┐      │
                         │        ▼                                     ▼          ▼      │
                         │  04_mapreduce          05_graph            06_ml             │
                         │  pickups per           PageRank,           z-scores, KMeans, │
                         │  zone per day          communities,        GBT classifier    │
                         │                        degrees                 │             │
                         │                            └───────┬───────────┘             │
                         │                                    ▼                         │
                         │                     08_dashboard/generate_dashboard_data     │
                         │                     → graph_nodes.csv, historical_predictions│
                         └───────────────────────────────────────┬───────────────────────┘
                                                                 │
 ┌──────────────── LIVE (every 10 s) ───────────────┐            │
 │ NYC live APIs → realtime_fetcher.py → HDFS input/│            │
 │   → live_pipeline.scala (Spark Streaming)        │            │
 │   → HDFS output/latest.csv + live_history.csv ───┼────────────┤
 └──────────────────────────────────────────────────┘            ▼
                                                    08_dashboard (Streamlit)
                                                    Home · Live Monitor · Network Map
                                                    · Explorer · Simulator
```

Everything big runs **inside Docker** (Hadoop + Spark). The dashboard and the live fetcher run **on Windows** and talk to HDFS over HTTP (WebHDFS).

---

## 3. Core concepts you must know

### 3.1 Docker and docker-compose
Docker runs programs in isolated "containers". `docker-compose.yml` describes several containers that start together. Here: 1 HDFS namenode, 3 HDFS datanodes, 1 Spark master, 2 Spark workers. Your project folders are **mounted** into the containers (for example `D:\Big_Data_Project\06_ml` appears inside the Spark container as `/project/06_ml`), so Spark can run your `.scala` files directly.

### 3.2 HDFS (Hadoop Distributed File System)
A file system spread across machines.
- **NameNode** = the index: knows which file is made of which blocks and where they are. Web UI: http://localhost:9870
- **DataNodes** = store the actual blocks. Here 3 of them (ports 9864/9865/9866).
- **Block size** = 128 MB. A big file is cut into 128 MB blocks.
- **Replication = 3**: every block is stored on 3 datanodes, so losing one doesn't lose data.
- Paths look like `hdfs://namenode:9000/data/neurotraffic/...`.

### 3.3 WebHDFS
An HTTP (REST) interface to HDFS on port 9870. Windows programs (the dashboard, the fetcher) use it because they don't have Hadoop installed. Reading a file is a **two-step** process:
1. Ask the namenode: `GET http://localhost:9870/webhdfs/v1/<path>?op=OPEN`
2. It replies **307 Redirect** to a datanode, e.g. `http://datanode2:9864/...`
3. Windows doesn't know the name `datanode2`, so the code rewrites it to `localhost:9865` (the port Docker exposes for datanode2) and fetches the data.

You will see this `fix_redirect` / `_fix_redirect` trick in many Python files.

### 3.4 Apache Spark
A distributed data-processing engine. Key ideas:
- **Driver** = the program you run (spark-shell). **Executors** = workers doing the computation in parallel. **Master** = hands out workers. Master UI: http://localhost:8080, job UI: http://localhost:4040
- **DataFrame** = a distributed table with named columns (like a pandas DataFrame, but split across machines).
- **Lazy evaluation**: `filter`, `select`, `join`, `groupBy` only *plan* the work. Nothing runs until an **action** like `count()`, `show()`, `write`, `collect()`.
- **Partitions**: a DataFrame is split into chunks processed in parallel. `repartition(n)` / `coalesce(n)` change the number of chunks. `coalesce(1)` = write a single output file.
- **Shuffle**: moving data between workers (needed for `groupBy` and `join`). `spark.sql.shuffle.partitions` (default 200) controls how many pieces a shuffle makes.
- **cache()** keeps a DataFrame in memory because it is reused.
- **broadcast(df)** sends a small table to every worker so a join doesn't need a shuffle.

### 3.5 spark-shell and `-i`
`spark-shell` is an interactive Scala console with a ready-made `spark` (SparkSession) object. `spark-shell -i file.scala` runs a script file line by line. That's why most `.scala` files here are **scripts** (no `object`/`main`) and end with `System.exit(0)`.

Typical command:
```bash
docker exec -it spark-master /spark/bin/spark-shell -i /project/02_cleaning/clean_taxi.scala
```

### 3.6 Parquet vs CSV
- **CSV**: plain text, slow, no types.
- **Parquet**: binary **columnar** format, compressed, keeps types. Spark only reads the columns you need. All intermediate tables are Parquet.
- **partitionBy("year","month")** writes folders like `year=2023/month=5/`. Filtering by year/month then skips whole folders ("partition pruning").

### 3.7 Scala in 60 seconds (what you'll see)
```scala
val x = 5                 // constant (can't change)
var df = spark.read...    // variable (can be reassigned)
def f(a: Int): Int = a+1  // function
col("taxi_pickups")       // reference a column
when(cond, a).otherwise(b) // SQL CASE WHEN
s"$HDFS/raw"              // string with a variable inside
f"$x%.3f"                 // formatted string
Seq(1,2,3) / Array(...)   // lists
case class Foo(a: Int)    // simple data record
_.getPath                 // shorthand lambda: x => x.getPath
```

### 3.8 Z-score (used everywhere for anomalies)
`z = (value − mean) / std_dev`. It says how unusual a value is compared to normal: z = 0 is perfectly normal, z = 3 is very unusual. The project computes mean/std **per zone, per hour of day, per weekday/weekend**, so normal rush hour isn't flagged as an anomaly.

### 3.9 Spark Structured Streaming
Treats a folder that keeps receiving new files as an endless table. Every *trigger* (e.g. 15 s) Spark picks up new files as a **micro-batch** and runs your code on it (`foreachBatch`). A **checkpoint** folder remembers which files are already processed, so a restart doesn't redo them.

### 3.10 Streamlit
A Python library that turns a script into a web page. Every time you click something, Streamlit **re-runs the whole script** top to bottom. `@st.cache_data(ttl=60)` remembers a function's result for 60 seconds so data isn't reloaded on every click. This project lists its pages with `st.navigation` in `app.py` (the page files are in `views/`).

---

## 4. Libraries used and what each one does

### Scala / Spark (run inside Docker)

| Library / import | What it does in this project |
|---|---|
| `org.apache.spark.sql.functions._` | All column functions: `col`, `when`, `sum`, `avg`, `count`, `date_trunc`, `hour`, `dayofweek`, `explode`, `sequence`, `greatest`, `least`, `coalesce`… |
| `org.apache.spark.sql.types._` | Data types for casting and schemas: `IntegerType`, `DoubleType`, `StringType`, `StructType`… |
| `org.apache.spark.sql.expressions.Window` | Window functions: forward-fill weather gaps, "nearest zone" ranking with `row_number()` |
| `org.apache.spark.ml.feature.VectorAssembler` | Packs many feature columns into one vector column (ML models need a single vector) |
| `org.apache.spark.ml.feature.StandardScaler` | Rescales features so they have similar ranges (std = 1, optionally mean = 0) |
| `org.apache.spark.ml.classification.GBTClassifier` | Gradient-Boosted Trees classifier (the main ML model) |
| `org.apache.spark.ml.clustering.KMeans` | Groups similar rows into k clusters; tiny clusters = unusual patterns |
| `org.apache.spark.ml.evaluation.*Evaluator` | Computes AUC-ROC, accuracy, precision, recall, F1 |
| `org.apache.spark.ml.Pipeline` / `PipelineModel` | Chains assembler → scaler → model into one object you can save/load |
| `org.apache.spark.graphx._` | Graph library: `Graph`, `Edge`, `pageRank`, `inDegrees`, `outDegrees`, `connectedComponents` |
| `org.apache.spark.graphx.lib.LabelPropagation` | Community detection algorithm |
| `org.apache.spark.sql.streaming.Trigger` | How often a streaming query runs (`ProcessingTime("15 seconds")`) |
| `org.apache.hadoop.fs.{FileSystem, Path}` | Direct HDFS file operations: list, rename, delete, create, exists |
| `java.time.*` | Current New York time and formatting in the live pipeline |
| sbt + `sbt-assembly` (`05_graph/build.sbt`) | Builds the GraphX app into one "fat JAR" for `spark-submit` |

### Python (run on Windows)

| Library | What it does in this project |
|---|---|
| `requests` | HTTP calls: download datasets, call live APIs, talk to WebHDFS |
| `tqdm` | Progress bars while downloading |
| `pandas` | Tables in Python: read CSV/Parquet, merge, group, filter in the dashboard and fetcher |
| `numpy` | Math and random numbers (simulator) |
| `pyarrow` | Engine pandas uses to read Parquet |
| `pyshp` (`import shapefile`) | Reads the taxi-zone **shapefile** (zone polygons) |
| `shapely` | Geometry: polygons, points, "is this point inside this zone?", centroids, `STRtree` spatial index |
| `pyproj` | Converts coordinates between GPS (lat/lon, EPSG:4326) and NY State Plane feet (EPSG:2263) used by the shapefile |
| `gtfs-realtime-bindings` (`google.transit.gtfs_realtime_pb2`) | Decodes MTA real-time train feeds (Protocol Buffers format) |
| `zoneinfo` / `pytz` | Time zones (America/New_York) |
| `streamlit` | The web dashboard framework |
| `plotly` (`express`, `graph_objects`) | Interactive charts and maps (`Scattermap` with `carto-darkmatter` tiles) |
| `scikit-learn` + `joblib` | Only for the simulator: loads `gbt_model.pkl` (a scikit-learn model) |
| `matplotlib` | Listed in requirements; not needed by the current dashboard |
| `pyspark` | Listed in requirements; only needed by the old `spark_live_processor.py` (not used now) |

---

## 5. Where the data lives (HDFS layout)

```
/data/neurotraffic/
├── raw/                    ← uploaded raw downloads (taxi/, citibike/, subway/, weather/, 311/, events/)
├── cleaned/                ← output of 02_cleaning (Parquet, partitioned by year/month)
│   ├── taxi/{yellow,green,hvfhv}
│   ├── citibike, subway, weather, 311, events
├── combined                ← join_all.scala       (zone × hour grid)
├── combined_v2             ← + bike columns
├── combined_v3             ← + fixed 311 join   ★ the main table, 6,917,952 rows
├── mapreduce_input/output  ← 04_mapreduce
├── disruptions             ← detect_anomalies.scala (labels)
├── ml/training.parquet     ← prepare_ml_training.scala
├── models/                 ← saved Spark ML models
├── evaluation/             ← metrics JSON, feature importance, confusion matrix
├── lookups/                ← zone_centroids, subway_station_zones, zone_hour_profile.csv
└── dashboard/              ← graph_nodes.csv, historical_predictions.parquet, metrics files
/data/graph/
├── nodes/{taxi_zones,subway_stations,bike_docks}.parquet
├── combined/trips.parquet  ← graph edges
└── features/features.parquet ← PageRank, community, degrees
/data/streaming/
├── input/                  ← live JSON batches from the fetcher (deleted after processing)
├── output/latest.csv       ← newest score per zone
├── output/live_history.csv ← last 24 hours, one snapshot per minute
├── output/source_status.json ← freshness of each live source
└── checkpoint/             ← Structured Streaming bookkeeping
```

6,917,952 = 263 zones × 26,304 hours (3 years × ~8,760 h).

> **Note:** after the cluster was rebuilt, HDFS only contains the `dashboard/`, `lookups/` and `streaming/` parts. The raw, cleaned and combined tables from the batch pipeline would have to be uploaded and recomputed to rerun those stages.

---

# Part 2 — Every file explained

Each file uses the same layout: **Purpose · Input → Output · How to run · Logic · Key ideas**.

---

## Infrastructure files

### `docker-compose.yml`
**Purpose:** starts the whole Big Data cluster with one command (`docker compose up -d`).

| Service | Image | Ports (Windows → container) | Role |
|---|---|---|---|
| `namenode` | bde2020/hadoop-namenode (Hadoop 3.2.1, Java 8) | 9870 (web UI + WebHDFS), 9000 (HDFS RPC) | HDFS index |
| `datanode1/2/3` | bde2020/hadoop-datanode | 9864, 9865, 9866 → 9864 | store HDFS blocks |
| `spark-master` | bde2020/spark-master 3.3.0 | 8080 (UI), 7077 (cluster), 4040 (job UI) | runs your spark-shell scripts |
| `spark-worker-1/2` | bde2020/spark-worker | – | 2 cores + 4 GB each, do the computation |

**Logic worth knowing:**
- `volumes:` named volumes (`namenode_data`, `datanode1_data`…) keep HDFS data when containers are removed. `docker compose down` keeps them; `docker compose down -v` deletes them.
- Folder mounts: `./neurotraffic_data → /data/local` (raw files visible inside), `./0X_folder → /project/0X_folder` (your code).
- `SERVICE_PRECONDITION=namenode:9870` makes datanodes wait until the namenode is up.
- All containers share the network `hadoop_net`, so they find each other by name (`namenode`, `spark-master`).

### `hadoop.env`
**Purpose:** Hadoop settings injected into the containers as environment variables (the bde2020 images turn `HDFS_CONF_dfs_replication=3` into `dfs.replication=3` in `hdfs-site.xml`).

Important lines:
- `CORE_CONF_fs_defaultFS=hdfs://namenode:9000` — the default file system address.
- `HDFS_CONF_dfs_webhdfs_enabled=true` — turns on WebHDFS (needed by the dashboard).
- `HDFS_CONF_dfs_permissions_enabled=false` — no permission checks (simpler for a project).
- `HDFS_CONF_dfs_replication=3`, `dfs_blocksize=134217728` (128 MB).
- `YARN_CONF_*`, `MAPRED_CONF_*` — settings for YARN/MapReduce. There are **no YARN containers in the current compose file**, so these are unused leftovers.

### `requirements.txt`
Python packages for the Windows side (see the library table above). Install with `pip install -r requirements.txt`.

### `.claude/launch.json`, `.vscode/settings.json`
Editor helpers. `launch.json` describes how to start the dashboard (note: it still points to an old `F:\` path). `settings.json` stops VS Code watching the `target/` build folders.

---

## 01_ingestion — downloading the data

### `01_ingestion/download_datasets.py` (Python)
**Purpose:** download all 6 raw datasets for 2022–2024 into `neurotraffic_data/` (≈50–65 GB).

**How to run:**
```bash
python 01_ingestion/download_datasets.py               # everything
python 01_ingestion/download_datasets.py --only taxi   # one dataset
```
Then upload to HDFS with `hdfs dfs -put`.

**Logic:**
1. `create_dirs()` makes the folder tree (`taxi/yellow`, `citibike`, `subway`, …).
2. `download_file(url, dest)` — the workhorse:
   - skips files that already exist;
   - downloads into a `.tmp` file in 1 MB chunks with a progress bar;
   - **resumes** an interrupted download using the HTTP `Range: bytes=N-` header (status 206 = partial content, 416 = already complete);
   - retries up to 5 times with increasing waits;
   - renames `.tmp` → final name only when complete (so you never get half files).
3. One function per dataset:
   - `download_taxi()` — monthly Parquet files from the TLC CloudFront server, plus `taxi_zone_lookup.csv` and the `taxi_zones.zip` shapefile.
   - `download_bike()` — monthly Citi Bike zip files from Amazon S3.
   - `download_subway()` — MTA hourly ridership from the **Socrata API** (`data.ny.gov`). Socrata returns max 50,000 rows per request, so it **paginates** with `$limit` and `$offset`, year by year, and sleeps 1 s between calls.
   - `download_weather()` — NOAA (needs a token) or `_download_weather_openmeteo()` fallback (free, one CSV per year, hourly).
   - `download_311()` — 311 requests via Socrata, filtered to transport complaint types.
   - `download_events()` — event permits + film permits via Socrata.
4. `print_summary()` shows file counts and sizes; `main()` parses `--only` and `--noaa-token` with `argparse`.

**Key ideas:** pagination, resumable downloads, retry with back-off.

---

## 02_cleaning — cleaning each dataset

All files in this folder follow the same pattern:

> **read raw from HDFS → select & rename columns → cast types → drop nulls/bad rows → filter dates 2022-01-01…2025-01-01 → filter NYC bounding box → add derived columns → write Parquet partitioned by year/month to `cleaned/`**

The NYC bounding box used everywhere: latitude 40.4774–40.9176, longitude −74.2591 to −73.7004.

### `clean_taxi.scala`
**Input:** `raw/taxi/{yellow,green,hvfhv}/*.parquet` → **Output:** `cleaned/taxi/{yellow,green,hvfhv}`

**Logic:**
1. `readFilesIndividually()` reads **each monthly file separately** and unions them, because the files have slightly different column types across years (reading all at once would fail on schema conflicts). Bad files are skipped with a message.
2. Each taxi type has different column names (`tpep_pickup_datetime` for yellow, `lpep_` for green, `pickup_datetime` for HVFHV); they are renamed to one common schema.
3. `applyFilters()` removes: missing times/locations, passenger_count = 0, distance ≤ 0, fare ≤ 0, zone IDs outside 1–263, trips shorter than 30 s or longer than 12 h, dates outside range. Adds `trip_duration_sec`, `pickup_hour` (`date_trunc("hour")`), `year`, `month`.
4. HVFHV (Uber/Lyft, ~684 M rows) is processed **in batches of 6 files**, each appended to a staging folder, to avoid running out of memory. HVFHV has no passenger count, so that check is skipped.
5. Prints raw → cleaned counts per type.

### `clean_subway.scala`
**Input:** `raw/subway` CSV → **Output:** `cleaned/subway`

**Logic:** casts columns; drops null/negative ridership; the raw data has **one row per payment method**, so it **groups by station + hour** and sums ridership and transfers; removes outliers above **mean + 5 × std** per station.

### `clean_bike.scala`
**Input:** `raw/citibike` CSV → **Output:** `cleaned/citibike`

**Logic:** standardises names (`initcap`, `trim`); removes duplicate `ride_id`s; keeps trips inside NYC; trip duration 60 s – 24 h; removes "dock errors" (same start and end station in < 2 min); adds `start_hour`, `year`, `month`.

### `clean_weather.scala`
**Input:** `raw/weather` (Open-Meteo CSV) → **Output:** `cleaned/weather`

**Logic:**
1. The Open-Meteo CSV has metadata lines on top, so it is read with a forced **11-column string schema** and only rows whose first column looks like a timestamp (regex `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}`) are kept.
2. Casts to numbers; drops duplicate hours.
3. **Forward-fill:** a `Window` over the previous 6 hours fills missing values with the last known value (`last(..., ignoreNulls = true)`).
4. Adds flags: `is_rain` (> 0.5 mm), `is_heavy_rain` (> 7.6 mm), `is_snow`, `is_extreme_cold` (< −10 °C), `is_extreme_heat` (> 35 °C), `is_low_visibility` (< 1 km), `is_high_wind` (> 50 km/h).

### `clean_311.scala`
**Input:** `raw/311` → **Output:** `cleaned/311`

**Logic:** keeps only 12 transport-related complaint types (street condition, traffic signal, illegal parking…); valid dates and coordinates; adds a hand-made **`severity_score`** (water main break = 5, traffic signal = 4, street condition/sewer = 3, …, other = 1); adds `resolution_hours` and `complaint_hour`.

### `clean_events.scala`
**Input:** `raw/events/event_permits…csv` + `film_permits…csv` → **Output:** `cleaned/events`

**Logic:**
1. Reads both files, renames to a common schema, `unionByName`.
2. **Counts affected streets with a regex** on the street description (patterns like "5TH AVE", "42ND ST") — `regexp_extract` + `split` + `size`.
3. Event duration in hours (0 < duration < 30 days).
4. `disruption_severity` 1–5 from number of streets and duration.

### `extract_bike_stations.scala`
**Purpose:** build a list of unique Citi Bike stations. Takes start and end stations from cleaned trips, unions them, averages their lat/lng per station. Writes one CSV to `/tmp/bike_stations_out` (inside the container).

### `map_bike_stations_to_zones.scala`
**Purpose:** assign each bike station to a taxi zone.

**Logic:** **cross-joins** every station with every zone centroid, computes a simple straight-line distance on lat/lng, and keeps the nearest zone per station using a `Window` + `row_number()` (rank 1). Writes `bike_station_zones.csv`.

> This is an approximation (nearest centroid). The newer `07_streaming/build_live_lookups.py` does the exact **point-in-polygon** test with the shapefile.

---

## 03_integration — joining everything into one table

### `join_all.scala` ★ the most important batch file
**Input:** all `cleaned/*` tables + `taxi_zone_lookup.csv` → **Output:** `combined` (partitioned by year/month)

**Logic:**
1. **Build the grid:** every zone (1–263) × every hour from 2022-01-01 00:00 to 2024-12-31 23:00. It uses `spark.range` for zones, `sequence(start, end, interval 1 hour)` + `explode` for hours, and a `crossJoin`. This guarantees a row for every zone-hour, even if nothing happened.
2. Adds time features: `year`, `month`, `day_of_week` (1 = Sunday … 7 = Saturday in Spark), `hour_of_day`, `is_rush_hour` (7–9 and 17–19), `is_weekend` (day 1 or 7). Joins zone name and borough.
3. Aggregates each source to the join level:
   - **Taxi — per zone per hour:** pickups (count by pickup zone), dropoffs (count by dropoff zone), avg fare, avg distance, avg duration.
   - **Weather — per hour (whole city).**
   - **Subway — per borough per hour** (sum of ridership, transfers, active stations).
   - **311 — per borough per hour** (count, max severity).
   - **Events — per borough per hour** (count, max severity, streets affected).
4. **Left-joins** everything onto the grid (taxi on zone+hour, weather on hour, the others on borough+hour), then fills missing values with 0.

> **Important to understand:** subway, 311 and events are joined **by borough**, so every zone in Manhattan gets the same Manhattan-wide subway number for that hour. Only taxi (and later bike) are truly per-zone.

### `add_bike_to_combined.scala`
**Input:** `combined` + `cleaned/citibike` + `bike_station_zones.csv` → **Output:** `combined_v2` (partitioned by year)

**Logic:** maps each trip's start and end station to a zone using the station→zone table; counts `bike_starts` per start zone per hour and `bike_ends` per end zone per hour; joins onto `combined`; fills missing with 0.

### `fix_311_join.scala`
**Input:** `combined_v2` + `cleaned/311` → **Output:** `combined_v3` ★

**Why it exists:** 311 boroughs were in UPPER CASE ("MANHATTAN") but zone boroughs were "Manhattan", so the first join matched nothing. This script drops the broken columns, re-aggregates 311 with `initcap(borough)`, and joins again. (The current `join_all.scala` already contains the fix too.)

`combined_v3` is the table used by everything after this point.

### `analysis_export.scala`
**Input:** `combined_v3` → **Output:** CSV summaries in `analysis/`

Creates 6 summary tables for charts (hourly citywide totals, daily per borough, zone heatmap, weather impact, rush vs off-peak, monthly trends) and prints a **correlation matrix** (`df.stat.corr`) between taxi, subway, bike, complaints, temperature, rain and wind.

---

## 04_mapreduce — MapReduce example

These files demonstrate the classic **MapReduce** model: total taxi pickups per zone per day.

### `export_for_mapreduce.scala`
Writes `combined_v3` as tab-separated text to `mapreduce_input/`, because Hadoop Streaming works with text lines.

### `mapper.sh` (Bash)
**Map step.** Reads each line, skips the header, takes `zone_id`, the date part of the timestamp (`${timestamp:0:10}`) and `pickups`, and outputs `zone_id <TAB> date <TAB> pickups` for non-zero rows.

### `reducer.sh` (Bash)
**Reduce step.** Hadoop sorts the mapper output by key before the reducer, so all lines for the same `zone_id + date` arrive together. The reducer keeps a running sum and prints `key <TAB> total` whenever the key changes (and once more at the end).

How you would run them with Hadoop Streaming (needs YARN, which this compose file doesn't start):
```bash
hadoop jar hadoop-streaming.jar -mapper mapper.sh -reducer reducer.sh -input /data/neurotraffic/mapreduce_input -output /data/neurotraffic/mapreduce_output
```

### `mapreduce_spark.scala`
The **same computation in Spark**: `filter(pickups > 0)` → `to_date` → `groupBy(zone_id, date).agg(sum)`. Shows how one Spark line replaces a mapper + reducer. Writes `mapreduce_output/`.

---

## 05_graph — graph analytics (GraphX)

**Idea:** turn NYC transport into a **network**. Nodes are taxi zones, subway stations and bike docks; edges are trips between them (and walking links between nearby nodes). Then ask network questions: which nodes are most important (PageRank)? Which nodes form clusters (communities)?

### `build.sbt`
Build file for the compiled GraphX app: Scala 2.12, Spark 3.5.1 libraries marked `provided` (already on the cluster), `sbt-assembly` to make `neurotraffic-graphx.jar`.

### `src/main/scala/neurotraffic/GraphSchema.scala`
Data definitions:
- `NodeType` enumeration: `TaxiZone`, `SubwayStation`, `BikeDock`.
- `NodeProperties(nodeId, nodeType, name, lat, lon, capacity, borough)`.
- `EdgeProperties(tripCount, avgTravelTime, hourOfDay, dayType)`.
- `GraphFeatures(pageRank, communityId, inDegree, outDegree)`.

### `src/main/scala/neurotraffic/GraphBuilder.scala`
Builds the graph.
- **ID offsets** so IDs never collide: taxi zones 1–263, subway = id + 1000, bike = id + 5000.
- `loadNodes()` reads the three node Parquet files and turns rows into `(vertexId, NodeProperties)`.
- `loadEdges()` reads `trips.parquet` and creates `Edge(src, dst, EdgeProperties)`.
- `buildGraph()` = `Graph(vertices, edges, defaultNode)`.
- `buildTimeWindowedGraph(hour, dayType)` keeps only edges for, e.g., 8 AM weekdays (`subgraph`).

### `src/main/scala/neurotraffic/ProximityEdges.scala`
Adds **walking links** between different transport modes that are close:
- taxi zone ↔ subway station within 1.5 km, taxi ↔ bike within 1.2 km, subway ↔ bike within 0.8 km.
- Distance with the **haversine** formula (real distance on Earth's surface).
- Edge weight = `baseDemand × (1 − dist/maxDist)²` — closer means stronger (quadratic decay).
- Edges go both ways. Travel time = distance × 12 min/km (walking).
- `buildConnectedGraph()` = trip edges + proximity edges.

### `src/main/scala/neurotraffic/GraphAnalytics.scala`
- `runPageRank` — weights edges by trip count, runs `pageRank(tol = 0.001, resetProb = 0.15)`.
- `runLabelPropagation` — community detection (10 iterations).
- `computeDegrees` — in-degree and out-degree.
- `extractAllFeatures` — joins all of them per node.
- `saveFeatures` — writes `graph_features/features.parquet`.

### `src/main/scala/neurotraffic/NeuroTrafficApp.scala`
The `main` entry point for `spark-submit`. Modes: `full` (build + analytics + save + print top 20), `pagerank`, `timewindow` (compares 8 AM, 5 PM, noon, 2 AM weekend), `stats` (counts by type/borough, edge weight stats, connected components). `--data <path>` overrides the base path.

### `build_graph_inputs.scala` (script)
Creates the input files the graph code expects, from real data:
1. **Taxi zone nodes** from `zone_centroids.csv`; capacity = total historical pickups.
2. **Subway station nodes** from `subway_station_zones.csv`; capacity ≈ zone ridership / 10.
3. **Bike dock nodes** from `bike_station_zones.csv`; capacity fixed at 30.
4. **Edges** from raw yellow + green taxi trips: group by origin zone, destination zone, hour, weekday/weekend → `trip_count`, avg travel time; keep pairs with ≥ 5 trips.

### `build_edges_only.scala`
Rebuilds only step 4 (edges), reading each taxi file individually to avoid the `VendorID` int/bigint schema conflict.

### `run_graph_pipeline.scala` ★ the one actually used
Does what `NeuroTrafficApp full` does, but as a spark-shell script (no sbt build needed):
1. Loads all nodes and normalises them to one schema.
2. Sums edges over all hours into one weight per pair.
3. `Graph(vertexRDD, edgeRDD)` → `pageRank(0.001)`, `LabelPropagation.run(g, 5)`, `inDegrees`, `outDegrees`.
4. Joins results with node info and writes `/data/graph/features/features.parquet`; prints the top 10 taxi zones by PageRank.

**What the results mean:**
- **PageRank** — a node is important if important nodes send it lots of traffic (the same idea Google used for web pages). Busy hubs like Midtown get high PageRank.
- **Label Propagation** — every node starts with its own label and repeatedly adopts the most common label among its neighbours; groups that trade lots of trips end up with the same label = a "community".
- **In/out degree** — how many different places send trips to / receive trips from this node.

These numbers become ML features and are shown in the dashboard (PageRank drives the simulator and the GBT features).

---

## 06_ml — anomaly detection and machine learning

There are several overlapping scripts here (the project evolved from Python/scikit-learn to Scala/Spark MLlib). The **main path** is:

```
combined_v3 → detect_anomalies.scala → disruptions (labels)
            → prepare_ml_training.scala → ml/training.parquet
            → train_gbt.scala → model + metrics + feature importance + confusion matrix
```

### `detect_anomalies.scala` ★ creates the labels
**Input:** `combined_v3` → **Output:** `disruptions`

**Logic:**
1. **Z-scores per zone** for 12 features (taxi, fare, distance, subway, bike, complaints, events, rain, wind, temperature): mean and std per zone, then `(x − mean)/std`.
2. `max_abs_z` (most unusual feature) and `avg_abs_z` (average unusualness).
3. **KMeans (k = 10)** on the standardised features; clusters with < 50,000 rows are "small" = rare patterns.
4. **Disruption score** = 0.4·avg_abs_z + 0.3·max_abs_z + 2·(in small cluster) + small bonuses for complaints, events and bad weather.
5. `is_disruption = 1` if score > 3.0.
6. Saves the result and prints summaries by borough, zone, hour and weekday.

### `prepare_ml_training.scala`
**Input:** `disruptions` + graph features → **Output:** `ml/training.parquet`

Joins graph features, creates **cross-modal ratio features** (`taxi_to_subway_ratio`, `bike_to_subway_ratio`, `total_surface_transport`, `complaint_density`), keeps **all positives** and only **5 % of negatives** (undersampling — 6.9 M rows → a few hundred thousand), writes one Parquet file.

### `train_gbt.scala` ★ trains the model
**Input:** `ml/training.parquet` → **Output:** `models/gbt_trained`, `evaluation/*.json/csv`, copies to `08_dashboard/data/`

**Logic:**
1. Casts the 23 feature columns to Double, replaces null/NaN with 0.
2. **Class weights:** disruptions are rare, so each positive row gets weight = negatives/positives. Without this the model would just predict "normal" always.
3. 80/20 random train/test split (seed 42).
4. `Pipeline(VectorAssembler → StandardScaler → GBTClassifier(maxIter=150, maxDepth=4, stepSize=0.1, subsampling=0.8))`.
5. Evaluates on test data: AUC-ROC, accuracy, precision, recall, F1, and the confusion matrix (TP, FP, FN, TN).
6. Feature importance from the trained trees.
7. Writes `evaluation_metrics.json`, `feature_importance.csv`, `confusion_matrix.csv` to HDFS and copies them to the dashboard's `data/` folder.

**GBT in one paragraph:** Gradient-Boosted Trees build many small decision trees one after another; each new tree tries to fix the mistakes of the trees before it. The final prediction combines all trees. `maxIter` = number of trees, `maxDepth` = how many questions each tree can ask, `stepSize` = how much each tree is trusted.

### `disruption_detector.scala` (alternative all-in-one version)
Loads `combined_v3` + graph features, adds the ratio features, **injects synthetic disruptions** into 3.5 % of rows (subway × 0.05–0.2, taxi × 2–4, bike × 1.8–3.3, more complaints), computes z-scores per zone × hour × weekend, defines `is_anomaly` = subway z < −1.5 **and** (taxi z > 1.5 **or** bike z > 1.5), trains a GBT (100 trees, depth 5), and saves the model to `models/gbt_cascade`, predictions and metrics.

### `tune_contamination.scala`
Tries z-score thresholds 1.0 … 5.0 for the rule "subway down + taxi or bike up". For each threshold it measures how strong the signature is (subway ratio below 1, taxi/bike ratio above 1) and keeps the best one in `models/anomaly_config.json`. Also runs KMeans (k = 10) and lists small clusters. ("Contamination" is the name of the similar setting in scikit-learn's Isolation Forest, which this replaced.)

### `scoring_module.scala`
A reusable class `NeuroTrafficScorer` meant for streaming: loads a saved model, computes z-scores and an anomaly level (≥ 0.80 yellow, ≥ 0.90 red), and runs the GBT only on red rows. Load it with `:load /project/06_ml/scoring_module.scala`. **Not used by the current live pipeline.**

### `prepare_data.scala`
A **quick-start / demo data generator**: reads only a local taxi file and fills subway, bike, weather, complaints and graph features with **synthetic (hash- or random-based) values**, then writes `combined_v3` and `graph_features`.
> ⚠ Running it **overwrites** the real `combined_v3`. Only use it for testing without the real data.

### `run_pipeline.scala`
Doesn't compute anything: it prints the recommended order and commands for the ML scripts.

---

## 07_streaming — live data pipeline

**Currently running:** `realtime_fetcher.py` (Python, on Windows) → HDFS `streaming/input/` → `live_pipeline.scala` (Scala, Spark cluster) → HDFS `streaming/output/`.

### `build_live_lookups.py` (Python) — run once
**Purpose:** creates two lookup tables the fetcher needs, from the real shapefile.

1. `load_zone_polygons()` reads `taxi_zones.shp` with **pyshp**, turns shapes into **shapely** polygons, merges multi-part zones (`unary_union`).
2. `build_zone_centroids()` — centroid of each polygon, converted from NY State Plane feet to GPS with **pyproj** → `zone_centroids.csv` (zone_id, name, borough, lat, lon).
3. `build_station_zones()` — reads subway stations from `subway_hourly_2024.csv`, extracts the lines from names like "Times Sq (1,2,3,7,N,Q,R,W,S)" with a regex, and finds the zone **containing** each station using an `STRtree` spatial index + `polygon.contains(point)` (nearest zone if none) → `subway_station_zones.csv`.
4. With `--upload` it pushes both to HDFS `lookups/` and replaces placeholder coordinates in `graph_nodes.csv` with the real centroids.

### `build_live_profiles.scala` — run once
Averages `combined_v3` per **zone × hour of day × weekday/weekend** for taxi, subway, bike, complaints and events → `lookups/zone_hour_profile.csv` (263 × 24 × 2 = 12,624 rows). This is the "normal level" used by the live system.

### `build_network_lookups.py` (Python) — run once
**Purpose:** builds the two static layers of the dashboard's Network Map.
1. `taxi_zones.geojson` — every taxi zone's outline: polygons from the shapefile, simplified (100 ft tolerance) and converted to GPS with pyproj, saved as GeoJSON for the map.
2. `subway_segments.csv` — the track of each line. The MTA station list has no stop order, so each line's stations are joined with a **minimum spanning tree** (Prim's algorithm: keep adding the closest unconnected station). This follows the real track closely, including express runs and branches. The "S" line is really three separate shuttles, so links longer than 3 km are not drawn for it.

### `realtime_fetcher.py` ★ live data collector (Python)
**Run:** `python 07_streaming/realtime_fetcher.py --interval 10`

**Logic:**
1. Waits for HDFS, creates the streaming folders, loads zones, the profile and the line→zones map.
2. Creates one **source object per live feed**. Each has `fetch()` (with its own refresh interval so APIs aren't hammered) and `status()`:

| Class | Source | What it computes |
|---|---|---|
| `LiveWeather` (every 5 min) | Open-Meteo current weather | temperature, rain, snow, wind, visibility + the same flags as cleaning |
| `LiveMTA` (60 s) | MTA subway alerts (JSON) | keeps **unplanned, currently active** alerts; maps route IDs to lines (`route_to_line`); severity from the alert type (`alert_severity`: suspension 0.8, delays 0.6, other 0.4); spreads each alert to every zone with a station on that line |
| `LiveSubwayTrains` | MTA GTFS-RT train feeds (Protocol Buffers) | number of trains currently reporting per line (shown as status only) |
| `LiveTrafficSpeeds` | NYC DOT traffic speeds | average road speed per zone; marked **stale** if older than 30 min |
| `LiveCitiBike` (60 s) | Citi Bike GBFS | compares bike counts per station between snapshots; changes over the last 15 min ≈ bike trips per hour per zone (`starts_per_hour`, a lower bound) |
| `Live311` | NYC Open Data 311 | recent transport complaints (published with delay, context only) |
| `LiveEvents` | NYC permitted events | active event permits per borough |

3. Every `--interval` seconds:
   - fetches all sources **in parallel** (`ThreadPoolExecutor`);
   - `build_batch()` creates one JSON record per zone (263). **Live:** weather, MTA alert line + severity, road speed, events, and Citi Bike where the zone has stations. **Estimated from the profile:** taxi pickups/dropoffs (× a weather factor: rain 1.15, heavy rain 1.3, snow × 1.4), subway ridership, complaints, and bikes where there are no stations (× 0.6 in rain);
   - writes the batch to HDFS `streaming/input/live_<time>_<id>.json` (one JSON object per line);
   - writes `bike_docks.json`: every Citi Bike dock with its live bikes, free docks and bike changes in the last 15 min (for the Network Map);
   - writes `source_status.json` (age and error of each source) for the dashboard;
   - prints one status line.

### `live_pipeline.scala` ★ live processor (Scala, Spark Structured Streaming)
**Run:**
```bash
docker exec -it spark-master /spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.ui.showConsoleProgress=false -i /project/07_streaming/live_pipeline.scala
```
**Logic (7 numbered sections in the file):**
1. **Profile** — reads `zone_hour_profile.csv` from HDFS (avg taxi, subway, bike per zone × hour × weekend) and caches it.
2. **Schema** — the fields used from each JSON record (streaming needs a fixed schema).
3. **Score** — joins each record with its profile row and computes:
   - `z = |live − avg| / max(avg × 0.3, 1)` for taxi, subway and bike (std approximated as 30 % of the average);
   - `anomaly_score = min(1, 0.04·maxZ + 0.03·avgZ + weather + alert)`, where weather = 0.15 if precipitation > 5 mm, 0.05 if > 0, and alert = 0.45 + 0.45 × severity when the zone's line has an active MTA alert;
   - `is_disruption = 1` if score ≥ 0.8.
4. **writeCsv** — Spark always writes a *folder* of part-files; this writes with `coalesce(1)` into a temp folder, then renames the single part-file to the real name (e.g. `latest.csv`).
5. **saveHistory** — reads the existing history, keeps only rows from the **last 24 hours** (New York time), adds the new snapshot, rewrites the file. Anything older is deleted automatically.
6. **process** (runs every micro-batch) — keeps only the newest timestamp in the batch (one row per zone), scores it, writes `latest.csv` every batch and history **once per minute**, prints one line.
7. **Start** — `readStream.json(input/)` with `cleanSource = delete` (processed input files are removed), checkpoint at `streaming/checkpoint`, trigger every 15 s, `foreachBatch(process _)`.

Also sets `spark.sql.shuffle.partitions = 4` because each batch is tiny (263 rows).

### `streaming_pipeline.scala` (older Scala version)
Same idea as `live_pipeline.scala`, but computes baselines from the full `combined_v3` table (real mean and std per zone × hour × weekend), uses more weather flags and a complaint signal, and writes **only** `latest.csv` (no history). Needs `combined_v3` in HDFS, which the rebuilt cluster no longer has.

### `spark_live_processor.py` (older PySpark version — not used)
Python + PySpark version of the processor: polls HDFS for new files, scores with Spark in `local[2]` mode, writes `latest.csv` to HDFS and **appends history only to a local Windows file** (30-day retention). Replaced because it needs PySpark + Java 17/21 on Windows.

### `live_processor.py` (pandas version — not used)
The same scoring with plain pandas instead of Spark. Lightweight fallback.

### `streaming_simulator.py` (synthetic data generator — alternative to the fetcher)
Generates **fake but realistic** batches instead of calling live APIs:
- simulated clock that runs faster than real time (`--time-scale 120` = 1 real minute = 2 simulated hours);
- `rush_hour_factor(hour)`, weekend factor, rain penalty;
- `WeatherState` — weather that drifts randomly over time;
- `DisruptionManager` — each batch has an 8 % chance of starting a disruption on a random line (severity 0.5–1.0, lasts 3–12 h, fades 5 % per hour);
- writes JSON batches to HDFS and also its own `latest.csv`.
Useful for demos when there are no live disruptions.

### `spark_demo.scala`
Small Spark demo: reads the film-permits CSV from HDFS, prints the schema, top boroughs and categories, then waits 10 minutes so you can look at the Spark UI on port 4040.

---

## 08_dashboard — the web UI (Streamlit)

**Run:**
```bash
cd D:\Big_Data_Project\08_dashboard
```
```bash
$env:NEUROTRAFFIC_USE_HDFS = 'true'
```
```bash
streamlit run app.py
```
Opens at http://localhost:8501.

**Layout:** `app.py` (menu) · `ui.py` (shared look) · `config.py` (paths, colours) · `hdfs_helper.py` (HDFS client) · `views/` (one file per page).

### `config.py`
All settings in one place:
- `USE_HDFS` (from the environment variable `NEUROTRAFFIC_USE_HDFS`, default true), `HDFS_HOST`.
- Every HDFS path (`HDFS_*`) and its local twin (`*_CSV`, `*_JSON`, `*_PARQUET` under `08_dashboard/data` and `streaming/output`).
- Network Map lookups (`ZONES_GEOJSON`, `SUBWAY_STATIONS_CSV`, `SUBWAY_SEGMENTS_CSV` in `07_streaming/lookups`).
- Map centre/zoom, official MTA colour per line (`LINE_COLORS`), line groups (`SUBWAY_LINES`), borough colours.

### `hdfs_helper.py`
The dashboard's HDFS client over WebHDFS:
- `is_hdfs_available()` — quick ping.
- `_fix_redirect()` — rewrites `datanodeN:9864` → `localhost:986x` (see concept 3.3).
- `read_hdfs_csv` / `read_hdfs_parquet` / `read_hdfs_json`, `write_hdfs_csv`, `make_hdfs_directory`.
- `load_dataframe(local_path, hdfs_path, use_hdfs)` and `load_json(...)` — **try HDFS first, fall back to the local file**. Every page loads data through these.

### `app.py` — entry point and menu
Defines the menu with `st.navigation`: Home, Live Monitor, Network Map, Explorer, Simulator (each an `st.Page` with an icon and a URL such as `/live_monitor`). It also shows the **Read data from HDFS** switch, with the connection status, at the top of every page's sidebar, then runs the chosen page. The page files live in `views/`, not `pages/`, because Streamlit would otherwise also build its own automatic menu from a `pages/` folder.

### `ui.py` — shared look and helpers
Used by every page so that everything looks and reads the same:
- `LEVELS` and `level(score)` — the four anomaly levels used everywhere: **Normal < 0.25** (green), **Elevated 0.25–0.50** (yellow), **Anomaly 0.50–0.80** (orange), **Disruption ≥ 0.80** (red).
- `setup_page(title, icon, subtitle)` — page config, the dark theme CSS and the page header with a one-line explanation.
- `section(title, caption, kind)` — section title with a short explanation and a **LIVE / ESTIMATED / HISTORICAL** tag.
- `level_legend()`, `style_chart(fig)` (common Plotly style; a page's own settings override the defaults), `zone_map(fig)` (dark NYC basemap), `use_hdfs()`.

### `views/0_home.py` — Home
1. **Right now** (live) — zones in disruption and anomaly, subway lines with an alert, snapshot time; warns if the snapshot is over 10 minutes old.
2. **Historical results 2022 – 2024** — zones, days, disrupted zone-days and their share.
3. **Pages** — one link and one sentence per page.
4. **How the system works** — the 8 steps from download to dashboard.
5. **Data files** — every file the dashboard reads: found in HDFS or locally, size and last update (WebHDFS `GETFILESTATUS`).

### `views/1_live_monitor.py` — Live Monitor
- Loads `latest.csv` and `source_status.json` (cache 5 s) and `graph_nodes.csv` (coordinates, names).
- Snapshot time and age, and counts per level.
- **Zone map**: one dot per zone in its level colour; bigger dot = higher score.
- **Zones needing attention**: every zone ≥ 0.50, highest first, with the subway line that has an alert. Next to it, **zones per level by borough** (stacked bars).
- **Data sources** table (LIVE / ESTIMATED / STALE / DELAYED, age, errors) and the active MTA alerts.
- **Auto-refresh** (sidebar, default every 15 s): `time.sleep(interval)` → clear the live cache → `st.rerun()`.

### `views/2_simulator.py` — Disruption Simulator
A **what-if tool**, not a forecast; the page and its "How the simulator works" box say so. The same inputs always give the same result (the random seed comes from `zlib.crc32` of line, hour and day).
1. **Line → candidate zones:** zones whose `all_lines` (from `graph_nodes.csv`) include the chosen line; fallback to the line's boroughs.
2. **Time → multiplier:** weekday rush 2.2, weekend rush 1.4, midday 1.3/1.0, evening 0.7, night 0.5.
3. **Epicenters (red):** weighted-random pick among candidate zones, weighted by PageRank + noise (more noise at night). Count ≈ slider × multiplier / 1.3.
4. **Cascade (orange):** the zones *nearest by distance* to the epicenters' average position (not along the line).
5. **Volumes:** generated per zone from PageRank × multiplier; epicenters get taxi × 2.5–4, subway × 0.05–0.25, bike × 1.8–3; cascades a milder version.
6. **Score:** the scikit-learn GBT (`models/gbt_model.pkl`, loaded once with joblib) gives a probability *p*, but the role dominates: epicenter = 0.70 + 0.08·mult + 0.15·p, cascade = 0.25 + 0.10·mult + 0.30·p, normal = small.
7. Taxi surge %, bike surge %, subway drop % (capped at 95%) and minutes to peak are calculated from the score with fixed factors.

### `views/3_explorer.py` — Explorer
Two tabs:
- **Last 24 hours (live)** — from `live_history.csv` (cache 60 s): choose 1, 6 or 24 hours; the cutoff uses **New York time** because the data is in NY time; 1- or 5-minute averages of the average and highest zone score, plus the number of zones ≥ 0.50. Periods when the pipeline was not running show as gaps.
- **History 2022 – 2024** — from `historical_predictions.parquet` (cache 10 min): date range and borough filter over the whole data; disrupted zones per day with a 7-day average; most disrupted zones (quiet zones such as parks and islands rank high because small changes in tiny traffic give large z-scores); share disrupted by weekday; the 15 most disrupted days; **CSV download** of the selection.

The Analytics page (model metrics, feature importance, graph view) was removed from the dashboard; the model's results are presented in the review deck instead. The metric files it read (`evaluation_metrics.json`, `feature_importance.csv`) are still produced by `06_ml/train_gbt.scala`.

### `views/5_network_map.py` — Network Map
One live map of every transport mode, drawn with **pydeck** (deck.gl):
- **Taxi zones** — shaded by the live anomaly level; normal zones are almost transparent so problem zones stand out.
- **Subway lines** — `subway_segments.csv` in official MTA colours; a line is drawn thicker when it has an active MTA alert (from `source_status.json`). Hovering shows the number of trains reporting on it right now.
- **Subway stations** — white, or yellow/orange/red by the worst alert on any of their lines.
- **Citi Bike docks** — from `bike_docks.json`: blue = plenty of bikes, amber = running low, red = almost empty; a bigger dot means more bikes taken or returned in the last 15 min.
- **Station ↔ dock links** — every station and dock less than 0.8 km apart. A link turns **orange (active)** when the station's line has an alert **and** the dock had at least 3 bike changes in 15 min: the "riders switch from subway to bike" signal.

The sidebar holds the layer switches, link mode, a **Focus on** menu (an alerted line or a borough) and **Refresh now**. Auto-refresh is off by default because every refresh resets the map's zoom. A legend under the map marks each layer LIVE or ESTIMATED.

### `generate_dashboard_data.scala` (Spark)
Builds the dashboard's files from the real pipeline outputs:
1. **`graph_nodes.csv`** — zone centroids + real PageRank/community/degrees + the zone's primary subway line and `all_lines` (from `subway_station_zones.csv`) + capacity.
2. **`historical_predictions.parquet`** — per zone per day from `disruptions`: `anomaly_score` = max disruption score ÷ overall max, `is_anomaly`, prediction label (CRITICAL > 0.8, ANOMALY > 0.5), affected line, predicted surge.
3. **`latest.csv`** — a placeholder snapshot, only if the live pipeline isn't producing one.

---

# Part 3 — Putting it together

## Full run order

**One-time batch pipeline** (each Scala line: `docker exec -it spark-master /spark/bin/spark-shell -i <file>`):

| Step | Command / file | Produces |
|---|---|---|
| 1 | `python 01_ingestion/download_datasets.py` then `hdfs dfs -put` | `raw/` |
| 2 | `02_cleaning/clean_*.scala` (taxi, subway, bike, weather, 311, events) | `cleaned/` |
| 3 | `extract_bike_stations.scala` → `map_bike_stations_to_zones.scala` | station → zone table |
| 4 | `03_integration/join_all.scala` → `add_bike_to_combined.scala` → `fix_311_join.scala` | `combined_v3` |
| 5 | `03_integration/analysis_export.scala`, `04_mapreduce/*` (optional) | summaries |
| 6 | `python 07_streaming/build_live_lookups.py --upload` | centroids, station zones |
| 7 | `05_graph/build_graph_inputs.scala` → `run_graph_pipeline.scala` | graph features |
| 8 | `06_ml/detect_anomalies.scala` → `prepare_ml_training.scala` → `train_gbt.scala` | labels, model, metrics |
| 9 | `08_dashboard/generate_dashboard_data.scala` | dashboard files |
| 10 | `07_streaming/build_live_profiles.scala` | `zone_hour_profile.csv` |

**Every session (live system):**
```bash
cd D:\Big_Data_Project
```
```bash
docker compose up -d
```
```bash
& C:\Users\Harsada\AppData\Local\Programs\Python\Python313\python.exe 07_streaming\realtime_fetcher.py --interval 10
```
```bash
docker exec -it spark-master /spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.ui.showConsoleProgress=false -i /project/07_streaming/live_pipeline.scala
```
Then start the dashboard (see 08_dashboard). Pages: Home, Live Monitor, Network Map, Explorer, Simulator. Run the fetcher and the pipeline in separate terminals.

**Web UIs:** NameNode http://localhost:9870 · DataNodes :9864 / :9865 / :9866 · Spark Master http://localhost:8080 · Spark job :4040 · Dashboard http://localhost:8501

---

## Honest notes: what is real, what is synthetic, known quirks

Know these before a demo or viva — examiners often ask.

**Real data**
- All historical source data (taxi, subway, bike, weather, 311, events 2022–2024).
- Graph features (PageRank etc.) are computed from real taxi trips.
- Live: weather, MTA alerts, train counts, road speeds, Citi Bike, 311, events.

**Estimated / synthetic**
- **Live taxi and subway volumes** are the historical profile (× a weather factor), because NYC publishes no live feed for them. So in the live score the taxi/subway z-scores are close to 0; the score is mainly driven by **MTA alerts, Citi Bike and rain**.
- **Training labels** come from rules (z-scores + KMeans in `detect_anomalies.scala`), not from a list of real incidents. The GBT therefore learns to reproduce those rules, which is why accuracy/AUC are very high. Say this openly: it's *rule-labelled* anomaly detection.
- `disruption_detector.scala` **injects synthetic disruptions** (3.5 % of rows).
- `prepare_data.scala` creates synthetic columns (and overwrites `combined_v3` if run).
- The **simulator** page is a scenario illustration (role-based scores, distance-based cascade).

**Granularity**
- Subway, 311 and events are **borough-level** per hour in `combined_v3`; only taxi and bike are zone-level.

**Known quirks**
- The simulator uses `gbt_model.pkl` from the old Python `train_gbt.py`, which isn't in the project any more. The Scala `train_gbt.scala` saves a Spark model instead.
- `evaluation_metrics.json` also contains an `isolation_forest` block that is an exact copy of the GBT numbers; the dashboard ignores it.
- `hadoop.env` has YARN/MapReduce settings, but no YARN containers run; the bash MapReduce job would need a YARN setup.
- `.claude/launch.json` still points to an old `F:\` path.
- Unused older versions kept for reference: `streaming_pipeline.scala`, `spark_live_processor.py`, `live_processor.py`, `scoring_module.scala`.

---

## Glossary

| Term | Meaning |
|---|---|
| **Zone** | One of 263 NYC TLC taxi zones (neighbourhood-sized areas) |
| **Zone-hour** | One row = one zone in one hour |
| **HDFS** | Hadoop's distributed file system |
| **NameNode / DataNode** | HDFS index server / storage servers |
| **Replication** | Number of copies of each block (3 here) |
| **WebHDFS** | HTTP interface to HDFS (port 9870) |
| **Spark driver / executor** | The program you run / the workers doing the work |
| **DataFrame** | Distributed table in Spark |
| **Action vs transformation** | Action runs the job (`count`, `write`); transformation only plans it (`filter`, `join`) |
| **Shuffle** | Moving data between workers for `groupBy`/`join` |
| **Partition** | One chunk of a DataFrame processed in parallel |
| **Parquet** | Compressed columnar file format |
| **Micro-batch** | The small chunk of new data Structured Streaming processes per trigger |
| **Checkpoint** | Folder where streaming remembers its progress |
| **Z-score** | (value − mean) / std; how unusual a value is |
| **KMeans** | Clustering into k groups; tiny groups = rare patterns |
| **GBT** | Gradient-Boosted Trees: many small trees, each fixing the previous ones' errors |
| **VectorAssembler** | Combines feature columns into one vector |
| **StandardScaler** | Rescales features to comparable ranges |
| **Class weight** | Makes rare positive examples count more in training |
| **Undersampling** | Keeping only a fraction of the common class |
| **AUC-ROC** | How well the model ranks positives above negatives (1.0 = perfect, 0.5 = random) |
| **Precision / Recall / F1** | Of predicted disruptions, how many were real / of real disruptions, how many were caught / balance of the two |
| **Confusion matrix** | Table of TP, FP, FN, TN |
| **PageRank** | Importance of a node in a network |
| **Label Propagation** | Community detection by spreading labels between neighbours |
| **In/Out degree** | Number of incoming/outgoing connections |
| **Haversine** | Distance between two GPS points on Earth |
| **Centroid** | Geometric centre of a zone polygon |
| **Point-in-polygon** | Test whether a GPS point lies inside a zone shape |
| **Shapefile** | GIS file format storing zone polygons |
| **GTFS-RT** | Standard real-time transit feed format (Protocol Buffers) |
| **GBFS** | Standard bike-share feed format (JSON) |
| **Socrata** | API platform behind NYC/NY open data (`$limit`, `$offset`, `$where`) |
| **Streamlit rerun** | Streamlit re-executes the page script on every interaction |
| **`@st.cache_data(ttl=N)`** | Remember a function's result for N seconds |
