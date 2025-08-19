# -*- coding: utf-8 -*-
"""
Read hex_week.{parquet|csv} -> filter last N days -> sum by H3 cell -> write GeoJSON.
Output: geojson/heat_90d.geojson (fields: h3, count_90d, geometry EPSG:4326)

Usage:
  python src/export_heat_90d_geojson.py \
    --hex_week data/processed/hex_week.parquet \
    --out geojson/heat_90d.geojson \
    --days 90 --min_count 1
"""

import argparse
from pathlib import Path
import pandas as pd

# deps: geopandas, shapely, h3 (v3 or v4), pyarrow/fastparquet (if parquet)
try:
    import geopandas as gpd
    from shapely.geometry import Polygon
except Exception as e:
    raise SystemExit("Need geopandas + shapely. Try: conda install -c conda-forge geopandas shapely")

try:
    import h3
except ImportError:
    raise SystemExit("Need h3. Try: conda install -c conda-forge h3-py")

# --------- H3 v3/v4 compatibility helpers ---------
def h3_boundary_lonlat(h):
    """
    Return list of (lon, lat) tuples forming the H3 cell boundary.
    v3: h3_to_geo_boundary(..., geo_json=True) already lon/lat
    v4: cell_to_boundary returns [(lat,lon)], need flip
    """
    if hasattr(h3, "h3_to_geo_boundary"):  # v3
        return h3.h3_to_geo_boundary(h, geo_json=True)
    else:  # v4
        pts = h3.cell_to_boundary(h)  # [(lat, lon), ...]
        return [(lon, lat) for (lat, lon) in pts]

# --------- IO helpers ---------
def read_hex_week(path: str) -> pd.DataFrame:
    path = str(path)
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path)
    elif path.lower().endswith(".csv"):
        df = pd.read_csv(path, low_memory=False)
    else:
        raise ValueError("hex_week file must be .parquet or .csv")

    # minimal schema: h3, week_start, count
    lower = {c.lower(): c for c in df.columns}
    c_h3 = lower.get("h3")
    c_week = lower.get("week_start")
    c_count = lower.get("count")
    if not (c_h3 and c_week and c_count):
        raise ValueError(f"Expected columns h3, week_start, count. Saw: {list(df.columns)}")

    # ensure tz-aware Toronto
    wk = pd.to_datetime(df[c_week], errors="coerce", utc=True)
    try:
        wk = wk.dt.tz_convert("America/Toronto")
    except Exception:
        # if naive, localize
        wk = wk.dt.tz_localize("America/Toronto")
    df = df.rename(columns={c_h3: "h3", c_count: "count"}).assign(week_start=wk)
    return df[["h3", "week_start", "count"]].dropna()

# --------- main ---------
def main(hex_week_path: str, out_geojson: str, days: int = 90, min_count: int = 1):
    df = read_hex_week(hex_week_path)

    cutoff = pd.Timestamp.now(tz="America/Toronto") - pd.Timedelta(days=days)
    recent = df[df["week_start"] >= cutoff]

    if recent.empty:
        raise SystemExit(f"No rows within last {days} days. Check your data time range.")

    counts = (recent.groupby("h3", as_index=False)["count"]
                    .sum()
                    .rename(columns={"count": "count_90d"}))

    if min_count > 0:
        counts = counts[counts["count_90d"] >= min_count]

    # build polygons
    polys = []
    for h in counts["h3"]:
        try:
            poly = Polygon(h3_boundary_lonlat(h))
            polys.append(poly)
        except Exception:
            polys.append(None)

    gdf = gpd.GeoDataFrame(counts, geometry=polys, crs="EPSG:4326").dropna(subset=["geometry"])

    Path(out_geojson).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_geojson, driver="GeoJSON")
    print(f"[OK] wrote {out_geojson} — cells: {len(gdf):,} (days={days}, min_count={min_count})")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hex_week", required=True, help="data/processed/hex_week.parquet or .csv")
    ap.add_argument("--out", default="geojson/heat_90d.geojson")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--min_count", type=int, default=1)
    args = ap.parse_args()
    main(args.hex_week, args.out, args.days, args.min_count)
