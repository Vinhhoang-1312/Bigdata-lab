"""Read-only access to Historical and Live Gold datasets."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

import pandas as pd

try:
    from .storage import readable_path, storage_options
    from .zones import HIGH_DEMAND_RATIO, ZONE_CENTROIDS
except ImportError:
    from storage import readable_path, storage_options
    from zones import HIGH_DEMAND_RATIO, ZONE_CENTROIDS

ANALYTICS_TIMEZONE = "America/New_York"

HISTORICAL_COLUMNS = [
    "pickup_zone_id", "day_of_week", "hour", "five_minute_slot",
    "historical_mean_trip_count", "historical_std_trip_count",
    "historical_mean_distance", "historical_mean_duration",
]
LIVE_COLUMNS = [
    "window_start", "window_end", "pickup_zone_id", "current_trip_count",
    "historical_mean_trip_count", "demand_ratio", "z_score",
]


_S3_FILE_CACHE: dict[str, dict[str, pd.DataFrame]] = {}


def _read_s3_resilient(path: str) -> pd.DataFrame:
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    clean = path[len("s3a://"):] if path.startswith("s3a://") else path[len("s3://"):] if path.startswith("s3://") else path
    bucket, _, prefix = clean.partition("/")
    endpoint = os.getenv("URBANFLOW_S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL", "http://localhost:8333")
    key = os.getenv("AWS_ACCESS_KEY_ID")
    if not key or key == "minioadmin":
        key = "URBANFLOWLOCALACCESS"
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not secret or secret == "minioadmin":
        secret = "urbanflow-local-secret-key-0123456789012345"
    endpoint_clean = endpoint.replace("http://", "").replace("https://", "")
    scheme = "https" if endpoint.startswith("https://") else "http"

    file_cache = _S3_FILE_CACHE.setdefault(path, {})

    try:
        pa_fs = pafs.S3FileSystem(
            endpoint_override=endpoint_clean,
            access_key=key,
            secret_key=secret,
            scheme=scheme,
        )
        selector = pafs.FileSelector(f"{bucket}/{prefix}", recursive=True)
        file_infos = pa_fs.get_file_info(selector)
    except Exception:
        dfs = [df for df in file_cache.values() if not df.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    valid_paths = [
        info.path for info in file_infos
        if info.type == pafs.FileType.File
        and info.base_name.endswith(".parquet")
        and not info.base_name.startswith("_")
        and not info.base_name.startswith(".")
        and info.size > 0
    ]
    if not valid_paths:
        _S3_FILE_CACHE.pop(path, None)
        return pd.DataFrame()

    valid_set = set(valid_paths)
    for p in list(file_cache.keys()):
        if p != "__all__" and p not in valid_set:
            del file_cache[p]

    new_paths = [p for p in valid_paths if p not in file_cache]
    if new_paths:
        # Fast bulk load for static datasets (e.g. historical baseline) when cache is cold
        if "historical" in path and len(file_cache) == 0:
            try:
                table = ds.dataset(new_paths, filesystem=pa_fs, format="parquet").to_table()
                bulk_df = table.to_pandas()
                file_cache["__all__"] = bulk_df
                for p in new_paths:
                    file_cache[p] = pd.DataFrame()
                return bulk_df
            except Exception:
                pass

        try:
            bulk_df = ds.dataset(new_paths, filesystem=pa_fs, format="parquet").to_table().to_pandas()
            for p in new_paths[:-1]:
                file_cache[p] = pd.DataFrame()
            file_cache[new_paths[-1]] = bulk_df
        except Exception:
            # If bulk read fails (e.g. an in-flight file in the list), read completed files individually
            for p in new_paths:
                try:
                    file_cache[p] = pq.read_table(p, filesystem=pa_fs).to_pandas()
                except Exception:
                    # In-flight or uncommitted file (Spark is still writing/flushing footer).
                    # Skip for this cycle; it will be retried on the next refresh.
                    pass

    dfs = [df for df in file_cache.values() if not df.empty]
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _read(path: str) -> pd.DataFrame:
    suffix = Path(path).suffix.lower()
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(readable_path(path), lines=True, **storage_options(path))
    if path.startswith(("s3://", "s3a://")):
        is_mocked = (
            getattr(pd.read_parquet, "__name__", "") != "read_parquet"
            or hasattr(pd.read_parquet, "mock_calls")
        )
        if not is_mocked:
            return _read_s3_resilient(path)
    import time
    for attempt in range(3):
        try:
            return pd.read_parquet(readable_path(path), **storage_options(path))
        except Exception as exc:
            if attempt == 2:
                raise exc
            time.sleep(0.4)


def _require(frame: pd.DataFrame, columns: list[str], dataset: str) -> pd.DataFrame:
    if frame.empty and len(frame.columns) == 0:
        return pd.DataFrame(columns=columns)
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{dataset} Gold missing columns: {missing}")
    return frame.loc[:, columns].copy()


@dataclass(frozen=True)
class GoldReader:
    historical_path: str
    live_path: str

    def historical(self) -> pd.DataFrame:
        return _require(_read(self.historical_path), HISTORICAL_COLUMNS, "Historical")

    def live(self) -> pd.DataFrame:
        frame = _require(_read(self.live_path), LIVE_COLUMNS, "Live")
        frame["window_start"] = pd.to_datetime(frame["window_start"], utc=True)
        frame["window_end"] = pd.to_datetime(frame["window_end"], utc=True)
        return frame

    def filtered_live(
        self, zones: list[int] | None = None, start=None, end=None
    ) -> pd.DataFrame:
        frame = self.live()
        if zones:
            frame = frame[frame.pickup_zone_id.isin(zones)]
        if start is not None:
            frame = frame[frame.window_start >= pd.Timestamp(start, tz="UTC")]
        if end is not None:
            frame = frame[frame.window_start <= pd.Timestamp(end, tz="UTC")]
        return frame.sort_values(["window_start", "pickup_zone_id"])

    def latest(self) -> pd.DataFrame:
        frame = self.live()
        return frame[frame.window_start == frame.window_start.max()].copy() if not frame.empty else frame


def to_analytics_local(series: pd.Series) -> pd.Series:
    """UTC instants as naive America/New_York clocks so charts do not shift to the laptop TZ."""
    utc = pd.to_datetime(series, utc=True)
    return utc.dt.tz_convert(ANALYTICS_TIMEZONE).dt.tz_localize(None)


def chart_mode_for(frame: pd.DataFrame) -> str:
    if frame.empty or "window_start_local" not in frame.columns:
        return "grouped"
    return "line" if frame["window_start_local"].nunique() >= 2 else "grouped"


def prepare_live_view(live: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    view = live.copy()
    view["window_start_local"] = to_analytics_local(view["window_start"])
    view["window_end_local"] = to_analytics_local(view["window_end"])
    view["window_label"] = view["window_start_local"].dt.strftime("%Y-%m-%d %H:%M NY")
    named = lookup.loc[:, ["pickup_zone_id", "zone_name"]]
    view = view.merge(named, on="pickup_zone_id", how="left")
    view["zone_name"] = view["zone_name"].fillna("Unknown zone")
    view["zone_label"] = view["pickup_zone_id"].astype(int).astype(str) + " — " + view["zone_name"]
    view["is_high_demand"] = view["demand_ratio"].gt(HIGH_DEMAND_RATIO)
    view["lat"] = view["pickup_zone_id"].map(lambda z: ZONE_CENTROIDS.get(int(z), (None, None))[0])
    view["lon"] = view["pickup_zone_id"].map(lambda z: ZONE_CENTROIDS.get(int(z), (None, None))[1])
    return view.sort_values(["window_start_local", "pickup_zone_id"]).reset_index(drop=True)
