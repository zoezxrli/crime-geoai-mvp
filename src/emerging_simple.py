# -*- coding: utf-8 -*-
"""
Emerging hotspot (simple): compare recent vs baseline windows on H3 hex weekly counts.

Input
-----
`data/processed/hex_week.{parquet|csv}` with columns:
    - h3          : H3 cell id
    - week_start  : week start (datetime)
    - count       : weekly incident count

Output
------
`geojson/emerging.geojson` (each feature has a `label` ∈ {"New","Intensifying","Persistent"} and metrics)

Usage
-----
python src/emerging_simple.py \
  --hex_week data/processed/hex_week.parquet \
  --out geojson/emerging.geojson \
  --baseline_weeks 12 --recent_weeks 4 \
  --z_thresh 1.0 --new_min_recent 2 --include_unlabeled 0
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd

try:
    import geopandas as gpd
    from shapely.geometry import Polygon
except Exception:
    raise SystemExit("geopandas + shapely required. Install via conda-forge.")
try:
    import h3
except ImportError:
    raise SystemExit("h3-py (v3 or v4) required.")

# --- H3 v3/v4 boundary helper (returns list of (lon, lat)) ---
def h3_boundary_lonlat(h):
    if hasattr(h3, "h3_to_geo_boundary"):  # v3
        return h3.h3_to_geo_boundary(h, geo_json=True)
    pts = h3.cell_to_boundary(h)          # v4: (lat, lon)
    return [(lon, lat) for (lat, lon) in pts]

def read_hex_week(path: str) -> pd.DataFrame:
    """Read parquet/csv; normalize column names & convert week_start to Toronto tz."""
    path = str(path)
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path)
    elif path.lower().endswith(".csv"):
        df = pd.read_csv(path, low_memory=False)
    else:
        raise ValueError("hex_week must be .parquet or .csv")

    lower = {c.lower(): c for c in df.columns}
    c_h3 = lower.get("h3")
    c_week = lower.get("week_start")
    c_count = lower.get("count")
    if not (c_h3 and c_week and c_count):
        raise ValueError(f"Expected columns h3, week_start, count. Saw: {list(df.columns)}")

    wk = pd.to_datetime(df[c_week], errors="coerce", utc=True)
    try:
        wk = wk.dt.tz_convert("America/Toronto")
    except Exception:
        wk = wk.dt.tz_localize("America/Toronto")
    df = df.rename(columns={c_h3: "h3", c_count: "count"}).assign(week_start=wk)
    return df[["h3", "week_start", "count"]].dropna()

def main(hex_week_path: str, out_geojson: str,
         baseline_weeks: int = 12, recent_weeks: int = 4,
         z_thresh: float = 1.0, new_min_recent: int = 2,
         include_unlabeled: int = 0):

    df = read_hex_week(hex_week_path)
    if df.empty:
        raise SystemExit("hex_week is empty")

    anchor = df["week_start"].max()

    # Time windows (aligned to weeks; inclusive bounds)
    recent_lo = anchor - pd.Timedelta(days=7 * recent_weeks) + pd.Timedelta(seconds=1)
    base_lo   = anchor - pd.Timedelta(days=7 * (recent_weeks + baseline_weeks)) + pd.Timedelta(seconds=1)
    base_hi   = anchor - pd.Timedelta(days=7 * recent_weeks)

    # Slice windows
    recent = df[(df["week_start"] > recent_lo) & (df["week_start"] <= anchor)].copy()
    base   = df[(df["week_start"] > base_lo)   & (df["week_start"] <= base_hi)].copy()

    if recent.empty or base.empty:
        raise SystemExit(
            f"Window is empty. recent_weeks={recent_weeks}, baseline_weeks={baseline_weeks}, anchor={anchor.date()}"
        )

    # --- Baseline metrics per h3 (missing weeks treated as 0) ---
    # Sum/mean/variance over the baseline period. Start with per-week sums:
    base_grp = base.groupby(["h3", "week_start"], as_index=False)["count"].sum()

    # Sum & sum of squares over *present* weeks
    g_sum = base_grp.groupby("h3")["count"].sum().rename("base_sum")
    g_sumsq = base_grp.groupby("h3")["count"].apply(lambda s: (s ** 2).sum()).rename("base_sumsq")
    g_weeks_present = base_grp.groupby("h3")["week_start"].nunique().rename("base_weeks_present")

    base_weeks_total = baseline_weeks
    base_df = pd.concat([g_sum, g_sumsq, g_weeks_present], axis=1).fillna(0)
    base_df["base_weeks_total"] = base_weeks_total
    base_df["base_mean"] = base_df["base_sum"] / base_weeks_total

    # Population variance proxy including zeros:
    # E[X^2] over all weeks ≈ (sum of squares + zeros) / W = base_sumsq / W
    base_df["ex2_all"] = base_df["base_sumsq"] / base_weeks_total
    base_df["base_std"] = np.sqrt(np.maximum(base_df["ex2_all"] - base_df["base_mean"] ** 2, 0.0))

    # --- Recent metrics per h3 (missing weeks treated as 0) ---
    recent_grp = recent.groupby(["h3", "week_start"], as_index=False)["count"].sum()
    r_sum = recent_grp.groupby("h3")["count"].sum().rename("recent_sum")
    r_weeks_present = recent_grp.groupby("h3")["week_start"].nunique().rename("recent_weeks_present")

    recent_df = pd.concat([r_sum, r_weeks_present], axis=1).fillna(0)
    recent_df["recent_weeks_total"] = recent_weeks
    recent_df["recent_mean"] = recent_df["recent_sum"] / recent_weeks

    # Join baseline & recent on the union of all H3 cells
    idx = pd.Index(sorted(set(df["h3"])))
    out = pd.DataFrame(index=idx).join([base_df, recent_df]).fillna(0)

    # z-like score
    eps = 1e-6
    out["delta"] = out["recent_mean"] - out["base_mean"]
    out["z"] = out["delta"] / (out["base_std"] + eps)

    # Citywide high-baseline threshold (Q75 of base_mean)
    city_q75 = float(np.quantile(out["base_mean"].to_numpy(), 0.75)) if len(out) else 0.0

    # Labeling rules
    def label_row(r):
        # New: baseline ~ 0 and recent has at least `new_min_recent`
        if (r["base_sum"] <= 0.5) and (r["recent_sum"] >= new_min_recent):
            return "New"
        # Intensifying: recent significantly above baseline
        if (r["recent_sum"] >= new_min_recent) and (r["z"] >= z_thresh):
            return "Intensifying"
        # Persistent: baseline high (≥ Q75) and recent also high
        if (r["base_mean"] >= city_q75) and (r["recent_mean"] >= max(city_q75, r["base_mean"] * 0.9)):
            return "Persistent"
        return "None"

    out["label"] = out.apply(label_row, axis=1)

    # Keep labeled only unless include_unlabeled=1
    if not include_unlabeled:
        out = out[out["label"] != "None"].copy()

    if out.empty:
        print("[WARN] No cells met labeling rules. Try lowering thresholds (e.g., --z_thresh 0.5, --new_min_recent 1).")

    # Build polygons
    rows, polys = [], []
    for h, r in out.iterrows():
        try:
            poly = Polygon(h3_boundary_lonlat(h))
            polys.append(poly)
            rows.append({
                "h3": h,
                "label": r["label"],
                "baseline_mean": round(float(r["base_mean"]), 4),
                "recent_mean":   round(float(r["recent_mean"]), 4),
                "delta":         round(float(r["delta"]), 4),
                "z":             round(float(r["z"]), 3),
                "baseline_std":  round(float(r["base_std"]), 4),
                "baseline_sum":  int(r["base_sum"]),
                "recent_sum":    int(r["recent_sum"]),
                "baseline_weeks": int(baseline_weeks),
                "recent_weeks":   int(recent_weeks),
                "anchor_date":    str(anchor.date())
            })
        except Exception:
            continue

    gdf = gpd.GeoDataFrame(rows, geometry=polys, crs="EPSG:4326")
    Path(out_geojson).parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_geojson, driver="GeoJSON")
    print(f"[OK] wrote {out_geojson} — cells: {len(gdf):,}")
    print(f"anchor={anchor.date()} | baseline_weeks={baseline_weeks} | recent_weeks={recent_weeks}")
    print("labels:", dict(zip(*np.unique(gdf['label'], return_counts=True))))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--hex_week", required=True)
    ap.add_argument("--out", default="geojson/emerging.geojson")
    ap.add_argument("--baseline_weeks", type=int, default=12)
    ap.add_argument("--recent_weeks", type=int, default=4)
    ap.add_argument("--z_thresh", type=float, default=1.0)
    ap.add_argument("--new_min_recent", type=int, default=2)
    ap.add_argument("--include_unlabeled", type=int, default=0)
    args = ap.parse_args()

    # Explicitly map CLI args to main() keyword parameters
    main(
        hex_week_path=args.hex_week,
        out_geojson=args.out,
        baseline_weeks=args.baseline_weeks,
        recent_weeks=args.recent_weeks,
        z_thresh=args.z_thresh,
        new_min_recent=args.new_min_recent,
        include_unlabeled=args.include_unlabeled,
    )
