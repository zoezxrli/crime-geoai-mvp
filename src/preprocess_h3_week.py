# -*- coding: utf-8 -*-
"""
TPS MCI CSV -> H3(res=9) -> weekly aggregation.
Outputs: data/processed/hex_week.parquet
"""

import argparse
from pathlib import Path
import pandas as pd

try:
    import h3  # conda install -c conda-forge h3-py
except ImportError:
    raise SystemExit("Missing h3-py. Install with: conda install -c conda-forge h3-py")

# ---- candidate name pools (全部小写比较) ----
CANDS = {
    "lat": [
        "lat", "latitude", "y", "lat_wgs84", "latitude_wgs84"
    ],
    "lon": [
        "lon", "long", "longitude", "x", "long_wgs84", "longitude_wgs84"
    ],
    "date": [
        "occurrencedate", "occurrence_date", "occ_date", "report_date", "date"
    ],
    "time": [
        "occurrencetime", "occurrence_time", "occ_time", "occurrencehour",
        "occ_hour", "report_time", "report_hour", "time", "hour"
    ],
    "offence": [
        "mci", "offence", "offense", "mci_category"
    ],
}

# --- H3 v3/v4 兼容 ---
def h3_index(lat, lon, res):
    # v3: geo_to_h3; v4: latlng_to_cell
    if hasattr(h3, "geo_to_h3"):
        return h3.geo_to_h3(lat, lon, res)
    else:
        return h3.latlng_to_cell(lat, lon, res)

def pick(colnames, pool):
    low = {c.lower(): c for c in colnames}
    for k in pool:
        if k in low:
            return low[k]
    return None

def to_tz_toronto(dt):
    # 尽量转为 Toronto 本地有时区的时间戳
    s = pd.to_datetime(dt, errors="coerce", utc=False)
    # 如果已经有时间部分 OK；没有时间就当 00:00
    if getattr(s.dtype, "tz", None) is None:
        s = s.dt.tz_localize("America/Toronto", nonexistent="shift_forward", ambiguous="NaT")
    else:
        s = s.dt.tz_convert("America/Toronto")
    return s

def combine_date_time(df, c_date, c_time):
    if c_time is None:
        dt = to_tz_toronto(df[c_date])
        dt = dt.dt.floor("D")  # 没时间列就当 00:00
        return dt
    # 可能 time 是小时（int）或 "HH:MM:SS"
    if pd.api.types.is_numeric_dtype(df[c_time]):
        hour = pd.to_numeric(df[c_time], errors="coerce").fillna(0).clip(0, 23).astype(int)
        base = to_tz_toronto(df[c_date]).dt.floor("D")
        dt = base + pd.to_timedelta(hour, unit="h")
        return dt
    else:
        comb = df[c_date].astype(str) + " " + df[c_time].astype(str)
        return to_tz_toronto(comb)

def week_start_monday(ts):
    # tz-aware -> 先去 tz 再回填
    naive = ts.dt.tz_convert("America/Toronto").dt.tz_localize(None)
    wk = naive.dt.to_period("W-MON").dt.start_time
    return wk.dt.tz_localize("America/Toronto")

def main(csv_path, out_parquet, h3_res=9, offence_filter=None,
         latcol=None, loncol=None, datecol=None, timecol=None, offencecol=None):
    df = pd.read_csv(csv_path, low_memory=False)
    cols = list(df.columns)

    # 允许命令行手动指定；否则自动猜
    c_lat = latcol or pick(cols, CANDS["lat"])
    c_lon = loncol or pick(cols, CANDS["lon"])
    c_date = datecol or pick(cols, CANDS["date"])
    c_time = timecol or pick(cols, CANDS["time"])   # 可为 None
    c_off  = offencecol or pick(cols, CANDS["offence"])

    if any(x is None for x in [c_lat, c_lon, c_date]):
        raise ValueError(
            f"Required cols not found.\nSaw: {cols}\n"
            f"Need latitude/longitude/occurrence date at minimum. "
            f"Try --latcol LAT_WGS84 --loncol LONG_WGS84 --datecol OCC_DATE --timecol OCC_HOUR"
        )

    # 只留有用列
    keep = [c for c in [c_lat, c_lon, c_date, c_time, c_off] if c]
    df = df[keep].copy()

    # 经纬度
    df["lat"] = pd.to_numeric(df[c_lat], errors="coerce")
    df["lon"] = pd.to_numeric(df[c_lon], errors="coerce")
    df = df.dropna(subset=["lat", "lon"])

    # 过滤 offence（可选）
    if offence_filter and c_off:
        df = df[df[c_off].astype(str).str.contains(offence_filter, case=False, na=False)]

    # 组合成带时区的 datetime
    df["dt"] = combine_date_time(df, c_date, c_time)
    df = df.dropna(subset=["dt"])

    # H3
    df["h3"] = [h3_index(lat, lon, h3_res) for lat, lon in zip(df["lat"], df["lon"])]

    # 周起点（周一）
    df["week_start"] = week_start_monday(df["dt"])

    # 周 x hex 聚合
    agg = (df.groupby(["h3", "week_start"], as_index=False)
             .size()
             .rename(columns={"size": "count"}))

    Path(out_parquet).parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(out_parquet, index=False)
    print(f"[OK] wrote {out_parquet} — rows: {len(agg):,}")
    print("Picked columns ->",
          {"lat": c_lat, "lon": c_lon, "date": c_date, "time": c_time, "offence": c_off})

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--out", default="data/processed/hex_week.parquet")
    ap.add_argument("--h3_res", type=int, default=9)
    ap.add_argument("--offence_filter", default=None)
    ap.add_argument("--latcol", default=None)
    ap.add_argument("--loncol", default=None)
    ap.add_argument("--datecol", default=None)
    ap.add_argument("--timecol", default=None)
    ap.add_argument("--offencecol", default=None)
    args = ap.parse_args()

    main(
        csv_path=args.csv,
        out_parquet=args.out,
        h3_res=args.h3_res,
        offence_filter=args.offence_filter,
        latcol=args.latcol,
        loncol=args.loncol,
        datecol=args.datecol,
        timecol=args.timecol,
        offencecol=args.offencecol,
    )
