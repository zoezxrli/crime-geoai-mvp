# Toronto Crime GeoAI — Near-Repeat & Emerging Hotspots (MVP)

**Live demo:** https://zoezxrli.github.io/crime-geoai-mvp/  
**Stack:** Python (Polars/Pandas), H3, scikit-learn, GeoPandas, Mapbox GL JS

> Planner-first crime analytics: 90-day heat, near-repeat “aftershock” risk, and emerging hotspots — in one lightweight web map.

---

## Elevator Pitch

We turn Toronto Police Service **Major Crime Indicators (MCI)** into **actionable, prevention-oriented** maps:

- **Heat (last 90 days):** where incidents cluster recently (soft background).
- **Near-Repeat (Knox):** short-term risk within **250 m / 14 days**, estimated via permutation (global **p-value**).
- **Emerging Hotspots (simple):** **New / Intensifying / Persistent** by comparing a **4-week recent** vs **12-week baseline**.

**Use-cases:** lighting and sightlines, access control, hours/management tweaks, neighborhood engagement.  
**Non-goals:** enforcement targeting of individuals.

---

## What’s Inside

- Reproducible pipeline: **CSV → H3 hex–week Parquet**.
- **Knox near-repeat** with Monte-Carlo permutations (**R=500** by default).
- **Emerging** labels with lightweight, explainable rules.
- Single-page **Mapbox** app with toggles, filters, hover, permalink, offence dropdown.

---

## Methods (one-pagers)

### Near-Repeat (Knox)
Use a **90-day lookback** window ending at the dataset’s anchor date.  
Count pairs of incidents with **distance ≤ 250 m** and **|Δt| ≤ 14 days** (observed).  
Build a null by **randomly permuting timestamps** (R=500). If observed ≫ null mean, clustering is significant (report **global p** and **z**).  
Map shows **k=1 H3 rings** around incidents in the **most recent 14 days**, with per-cell **`coverage`**.

### Emerging (simple)
Per H3 cell, compare **Recent (4w)** vs **Baseline (12w)**:
- **New:** baseline≈0 & recent ≥ 2
- **Intensifying:** recent ≥ baseline + _z·σ_ (default _z_=1)
- **Persistent:** baseline in top quartile and recent remains high

Windows are **anchored to the dataset’s latest week**.

### Heat (90d)
Sum weekly counts over the last 90 days and render as a **low-alpha background**.  
Color ramp auto-adapts via **percentiles**, robust to low-variance data.

---

## Parameters (defaults)

- H3 resolution: **r=9** (~0.105 km²; edge ~170–200 m)  
- Near-Repeat thresholds: **distance ≤ 250 m**, **time ≤ 14 days**  
- Permutations: **R = 500**  
- Emerging windows: **baseline 12w**, **recent 4w**, **z=1.0**, **min recent=2**  
- Heat window: **90 days**  
- All “recent” windows are **anchored to the data’s latest timestamp** (not “today”).

---

## Data

- **Toronto Police Service — Major Crime Indicators (MCI)**  
  Download the CSV into `data/raw/`. Columns include lat/lon (`LAT_WGS84`, `LONG_WGS84`), occurrence date/time (`OCC_DATE`, `OCC_HOUR`), and category (`MCI_CATEGORY`).  
  ⚠️ Locations are **offset to nearest intersections** by TPS for privacy; we aggregate to H3 cells.

---

## Quickstart

```bash
# (Optional) Create a clean conda env
conda create -n crime-geoai python=3.10 -y
conda activate crime-geoai
conda install -c conda-forge polars pandas geopandas shapely h3-py scikit-learn pyarrow -y

# 1) CSV → H3 hex-week parquet
python src/preprocess_h3_week.py \
  --csv data/raw/Major_Crime_Indicators.csv \
  --out data/processed/hex_week.parquet \
  --h3_res 9 \
  --latcol LAT_WGS84 --loncol LONG_WGS84 \
  --datecol OCC_DATE --timecol OCC_HOUR \
  --offencecol MCI_CATEGORY

# 2) Heat (90d)
python src/export_heat_90d_geojson.py \
  --hex_week data/processed/hex_week.parquet \
  --out geojson/heat_90d.geojson \
  --days 90

# 3) Near-Repeat (Knox)
python src/near_repeat_knox.py \
  --csv data/raw/Major_Crime_Indicators.csv \
  --out geojson/near_repeat.geojson \
  --h3_res 9 --distance_m 250 --time_days 14 \
  --lookback_days 90 --recent_days 14 --k 1 --permutations 500 \
  --latcol LAT_WGS84 --loncol LONG_WGS84 \
  --datecol OCC_DATE --timecol OCC_HOUR \
  --offencecol MCI_CATEGORY

# 4) Emerging (simple)
python src/emerging_simple.py \
  --hex_week data/processed/hex_week.parquet \
  --out geojson/emerging.geojson \
  --baseline_weeks 12 --recent_weeks 4 \
  --z_thresh 1.0 --new_min_recent 2
