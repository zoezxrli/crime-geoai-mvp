# -*- coding: utf-8 -*-
"""
Near-repeat (Knox) analysis + Monte Carlo permutation.
- Reads TPS MCI CSV
- Filters to last N days (lookback)
- Counts space<=d & time<=t pairs (observed)
- Permutes timestamps R times -> null distribution
- Writes a near-repeat attention layer as GeoJSON by union of k-ring cells around incidents in the recent window (recent-days).
Output: geojson/near_repeat.geojson

Usage (for your CSV columns):
python src/near_repeat_knox.py \ 
  --csv data/raw/Major_Crime_Indicators.csv \ 
  --latcol LAT_WGS84  --loncol LONG_WGS84\
  --datecol OCC_DATE. --timecol OCC_HOUR\ 
  --offencecol MCI_CATEGORY  --offence_filter "Auto Theft"\
  --out geojson/near_repeat.geojson
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd

# spatial + geo
try:
    import h3
except ImportError:
    raise SystemExit("Need h3. Try: conda install -c conda-forge h3-py")
try:
    from sklearn.neighbors import BallTree
except Exception:
    raise SystemExit("Need scikit-learn. Try: conda install -c conda-forge scikit-learn")
try:
    import geopandas as gpd
    from shapely.geometry import Polygon
except Exception:
    raise SystemExit("Need geopandas+shapely. Try: conda install -c conda-forge geopandas shapely")

EARTH_RADIUS_M = 6_371_000.0

# ---------- H3 v3/v4 compat ----------
def h3_cell(lat, lon, res):
    if hasattr(h3, "geo_to_h3"):                    # v3
        return h3.geo_to_h3(lat, lon, res)
    return h3. latlng_to_cell(lat, lon, res)        # v4

def h3_k_ring(h, k):
    if hasattr(h3, "k_ring"):                       # v3
        return set(h3.k_ring(h, k))
    return set(h3.grid_disk(h, k))                  # v4

def h3_boundary_lonlat(h):
    if hasattr(h3, "h3_to_geo_boundary"):            # v3
        return h3.h3_to_geo_boundary(h, geo_json=True)
    pts = h3.cell_to_boundary(h)                     # v4 -> (lat,lon)
    return [(lon, lat) for (lat, lon) in pts]

# ---------- column guessing ----------
CANDS = {
    "lat": ["lat_wgs84","latitude","lat","y"],
    "lon": ["long_wgs84","longitude","long","lon","x"],
    "date": ["occ_date","occurrencedate","occurrence_date","report_date","date"],
    "time": ["occ_hour","occurrencetime","occurrence_time","report_hour","time","hour"],
    "off":  ["mci_category","offence","offense","mci"]
}
def pick(colnames, pool):
    low = {c.lower(): c for c in colnames}
    for k in pool:
        if k in low: return low[k]
    return None

def to_toronto_dt(df, c_date, c_time):
    # combine date/time to tz-aware Toronto timestamp
    if c_time is None:
        dt = pd.to_datetime(df[c_date], errors="coerce", utc=False)
        dt = dt.dt.tz_localize("America/Toronto", nonexistent="shift_forward", ambiguous="NaT")
        return dt.dt.floor("T")
    if pd.api.types.is_numeric_dtype(df[c_time]):
        base = pd.to_datetime(df[c_date], errors="coerce", utc=False).dt.floor("D")
        base = base.dt.tz_localize("America/Toronto", nonexistent="shift_forward", ambiguous="NaT")
        hour = pd.to_numeric(df[c_time], errors="coerce").fillna(0).clip(0,23).astype(int)
        return base + pd.to_timedelta(hour, unit="h")
    comb = df[c_date].astype(str) + " " + df[c_time].astype(str)
    dt = pd.to_datetime(comb, errors="coerce", utc=False)
    if getattr(dt.dtype, "tz", None) is None:
        dt = dt.dt.tz_localize("America/Toronto", nonexistent="shift_forward", ambiguous="NaT")
    else:
        dt = dt.dt.tz_convert("America/Toronto")
    return dt

# ---------- knox core ----------
def knox_observed_pairs(lat_rad, lon_rad, ts_ns, d_m, t_days):
    """
    Count pairs with distance <= d_m and |delta_t| <= t_days.
    Use BallTree(haversine) to precompute spatial neighbors, then filter by time.
    Returns: observed_count, pair_index ndarray shape (P,2) with i<j
    """
    coords = np.c_[lat_rad, lon_rad]
    tree = BallTree(coords, metric="haversine")
    r = d_m / EARTH_RADIUS_M  # meters -> radians
    # neighbors for each point, includes self
    inds = tree.query_radius(coords, r=r, return_distance=False)

    pairs = []
    n = len(coords)
    for i in range(n):
        neigh = inds[i]
        if len(neigh) == 0: continue
        # keep j>i to avoid double count
        js = neigh[neigh > i]
        if js.size:
            pairs.extend([(i, int(j)) for j in js])
    if not pairs:
        return 0, np.empty((0,2), dtype=int)

    pairs = np.asarray(pairs, dtype=int)
    # time filter
    tdiff = np.abs(ts_ns[pairs[:,0]] - ts_ns[pairs[:,1]])
    within = tdiff <= (t_days * 24 * 3600 * 1e9)  # days -> ns
    obs = int(np.sum(within))
    return obs, pairs[within]

def knox_permute_counts(ts_ns, pairs, t_days, R=500, seed=42):
    """
    Fix spatial pairs; permute timestamps R times; count time-within pairs.
    Returns: np.array shape (R,)
    """
    rng = np.random.default_rng(seed)
    n = len(ts_ns)
    counts = np.empty(R, dtype=int)
    thresh = (t_days * 24 * 3600 * 1e9)
    for r in range(R):
        perm = rng.permutation(n)
        tperm = ts_ns[perm]
        td = np.abs(tperm[pairs[:,0]] - tperm[pairs[:,1]])
        counts[r] = int(np.sum(td <= thresh))
    return counts

# ---------- main pipeline ----------
def main(csv, out_geojson, h3_res=9, distance_m=250, time_days=14,
         lookback_days=90, recent_days=14, k=1, permutations=500,
         latcol=None, loncol=None, datecol=None, timecol=None,
         offencecol=None, offence_filter=None):
    df = pd.read_csv(csv, low_memory=False)
    cols = list(df.columns)

    c_lat = latcol or pick(cols, CANDS["lat"])
    c_lon = loncol or pick(cols, CANDS["lon"])
    c_date = datecol or pick(cols, CANDS["date"])
    c_time = timecol or pick(cols, CANDS["time"])
    c_off  = offencecol or pick(cols, CANDS["off"])
    if any(c is None for c in [c_lat, c_lon, c_date]):
        raise SystemExit(f"Required columns missing. Saw: {cols}\n"
                         "Use --latcol/--loncol/--datecol/--timecol to specify.")

    df["lat"] = pd.to_numeric(df[c_lat], errors="coerce")
    df["lon"] = pd.to_numeric(df[c_lon], errors="coerce")
    df = df.dropna(subset=["lat","lon"])

    if offence_filter and c_off:
        df = df[df[c_off].astype(str).str.contains(offence_filter, case=False, na=False)]

    # datetime and filters
    df["dt"] = to_toronto_dt(df, c_date, c_time)
    print("data window:", df["dt"].min().date(), "→", df["dt"].max().date())

    df = df.dropna(subset=["dt"])
    # now = pd.Timestamp.now(tz="America/Toronto")
    # df = df[df["dt"] >= (now - pd.Timedelta(days=lookback_days))].copy()
    anchor = df["dt"].max()
    cut_lo = anchor - pd.Timedelta(days=lookback_days)
    df = df[(df["dt"] >= cut_lo) & (df["dt"] <= anchor)].copy()
    print("anchor date:", anchor.date())
    if df.empty:
        raise SystemExit("No incidents in lookback window.")

    # arrays
    lat_rad = np.deg2rad(df["lat"].to_numpy())
    lon_rad = np.deg2rad(df["lon"].to_numpy())
    # ts_ns   = df["dt"].view("int64").to_numpy()  # ns since epoch (tz-aware ok in pandas)
    ts_ns = df["dt"].dt.tz_convert("UTC").astype("int64").to_numpy()

    # Knox observed + spatial pairs
    obs, pairs = knox_observed_pairs(lat_rad, lon_rad, ts_ns, distance_m, time_days)

    if pairs.size == 0:
        print("[INFO] No space<=d candidates; try larger distance or longer lookback.")
        sim = np.zeros(permutations, dtype=int)
        pval = 1.0
        z = np.nan
    else:
        # Permutation
        sim = knox_permute_counts(ts_ns, pairs, time_days, R=permutations, seed=42)
        mu, sd = float(sim.mean()), float(sim.std(ddof=1)) if permutations>1 else (sim.mean(), 0.0)
        # Monte Carlo p-value (right-tailed)
        pval = (1 + np.sum(sim >= obs)) / (permutations + 1)
        z = (obs - mu) / sd if sd > 0 else np.inf

    print("=== Knox near-repeat ===")
    print(f"n incidents: {len(df):,} (lookback={lookback_days}d, filter={offence_filter or 'None'})")
    print(f"thresholds: distance≤{distance_m} m, time≤{time_days} d")
    if pairs.size:
        print(f"observed pairs: {obs:,}")
        print(f"null mean±sd: {sim.mean():.1f} ± {sim.std(ddof=1):.1f} (R={permutations})")
        print(f"Monte-Carlo p-value: {pval:.5f} | z≈{z:.2f}")
    else:
        print("observed pairs: 0")

    # ----- build attention layer (recent incidents -> k-ring coverage) -----
    # recent = df[df["dt"] >= (now - pd.Timedelta(days=recent_days))].copy()
    recent = df[df["dt"] >= (anchor - pd.Timedelta(days=recent_days))].copy()
    if recent.empty:
        print(f"[WARN] No incidents in recent {recent_days} days; attention layer will be empty.")
        gpd.GeoDataFrame({"h3":[], "coverage":[]}, geometry=[], crs="EPSG:4326") \
            .to_file(out_geojson, driver="GeoJSON")
        return

    # count coverage of k-ring cells
    cells = {}
    for lat, lon in zip(recent["lat"].to_numpy(), recent["lon"].to_numpy()):
        c = h3_cell(lat, lon, h3_res)
        ring = h3_k_ring(c, k)
        for h in ring:
            cells[h] = cells.get(h, 0) + 1

    # polygons
    rows = []
    polys = []
    for h, cov in cells.items():
        try:
            poly = Polygon(h3_boundary_lonlat(h))
            polys.append(poly)
            rows.append({"h3": h, "coverage": int(cov),
                         "window_days": int(recent_days),
                         "k": int(k),
                         "p_value": float(pval)})
        except Exception:
            continue

    gdf = gpd.GeoDataFrame(rows, geometry=polys, crs="EPSG:4326").sort_values("coverage", ascending=False)
    Path(out_geojson).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_geojson, driver="GeoJSON")
    print(f"[OK] wrote {out_geojson} — cells: {len(gdf):,} (recent_days={recent_days}, k={k})")
    print("Properties per cell: coverage (#recent incidents whose k-ring includes cell), window_days, k, p_value")
    print("Tip: symbolize by coverage, and add a side note showing global p-value.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", default="geojson/near_repeat.geojson")
    ap.add_argument("--h3_res", type=int, default=9)
    ap.add_argument("--distance_m", type=float, default=250)
    ap.add_argument("--time_days", type=int, default=14)
    ap.add_argument("--lookback_days", type=int, default=90)
    ap.add_argument("--recent_days", type=int, default=14)
    ap.add_argument("--k", type=int, default=1)
    ap.add_argument("--permutations", type=int, default=500)
    ap.add_argument("--latcol", default=None)
    ap.add_argument("--loncol", default=None)
    ap.add_argument("--datecol", default=None)
    ap.add_argument("--timecol", default=None)
    ap.add_argument("--offencecol", default=None)
    ap.add_argument("--offence_filter", default=None)
    args = ap.parse_args()

    # ✅ 显式把 --out 映射到 out_geojson
    main(
        csv=args.csv,
        out_geojson=args.out,
        h3_res=args.h3_res,
        distance_m=args.distance_m,
        time_days=args.time_days,
        lookback_days=args.lookback_days,
        recent_days=args.recent_days,
        k=args.k,
        permutations=args.permutations,
        latcol=args.latcol,
        loncol=args.loncol,
        datecol=args.datecol,
        timecol=args.timecol,
        offencecol=args.offencecol,
        offence_filter=args.offence_filter,
    )

"""
Results:
    data window: 1964-09-01 → 2025-06-30
    anchor date: 2025-06-30
    === Knox near-repeat ===
    n incidents: 9,954 (lookback=90d, filter=None)
    thresholds: distance≤250.0 m, time≤14 d
    observed pairs: 27,362
    null mean±sd: 7912.4 ± 87.9 (R=500)
    Monte-Carlo p-value: 0.00200 | z≈221.17
    [OK] wrote geojson/near_repeat.geojson — cells: 3,455 (recent_days=14, k=1)
    Properties per cell: coverage (#recent incidents whose k-ring includes cell), window_days, k, p_value
    Tip: symbolize by coverage, and add a side note showing global p-value.
    在本数据的最近 90 天里，同类案件在“250 m / 14 天”内成对聚集的程度，是随机世界的 ~3.5 倍，这就是“余震效应”的统计证据。
    地图层（near_repeat.geojson）把最近 14 天内的“最可能出现余震的邻域”高亮出来，并按 coverage 强弱排序，
    指导“案后窗口期”的照明/门禁巡检、物业提醒等预防动作。
"""