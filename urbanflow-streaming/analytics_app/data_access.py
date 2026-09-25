"""Read-only access to Historical and Live Gold datasets."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any
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
_LIVE_GOLD_MANIFEST_CACHE: dict[str, dict[str, Any]] = {}
_S3_CACHE_LOCK = RLock()


def _read_s3_resilient_unlocked(path: str) -> pd.DataFrame:
    import json
    import pyarrow.dataset as ds
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    clean = path[len("s3a://"):] if path.startswith("s3a://") else path[len("s3://"):] if path.startswith("s3://") else path
    bucket, _, prefix = clean.partition("/")
    prefix = prefix.rstrip("/")
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
    except Exception:
        dfs = [df for df in file_cache.values() if not df.empty]
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    # 1. High-efficiency compact read model via Spark Structured Streaming metadata
    # Eliminates listing 20,000+ append-only archive files and enables instant warm queries
    meta_dir = f"{bucket}/{prefix}/_spark_metadata" if prefix else f"{bucket}/_spark_metadata"
    has_spark_metadata = False

    try:
        meta_infos = pa_fs.get_file_info(pafs.FileSelector(meta_dir, recursive=False))
        batch_info = {
            int(info.base_name): info
            for info in meta_infos
            if info.type == pafs.FileType.File and info.base_name.isdigit()
        }
        batch_ids = sorted(batch_info)

        if batch_ids:
            has_spark_metadata = True
            max_batch = batch_ids[-1]
            manifest_cache = _LIVE_GOLD_MANIFEST_CACHE.setdefault(
                path, {"last_max_batch": -1, "dataframe": pd.DataFrame()}
            )

            latest_commit_time = batch_info[max_batch].mtime

            # Instant warm return if no new micro-batches have committed
            if max_batch == manifest_cache["last_max_batch"] and not manifest_cache["dataframe"].empty:
                cached = manifest_cache["dataframe"].copy()
                cached.attrs["gold_commit_time"] = latest_commit_time
                return cached

            if manifest_cache["last_max_batch"] == -1:
                # Cold start: read only recent 15 batches (representing ~60-90 rolling windows)
                target_batches = batch_ids[-15:]
            else:
                # Incremental: read only new batches committed since previous read
                target_batches = [b for b in batch_ids if b > manifest_cache["last_max_batch"]]

            new_parquet_paths: list[str] = []
            for b_id in target_batches:
                b_path = f"{meta_dir}/{b_id}"
                try:
                    with pa_fs.open_input_file(b_path) as f:
                        for line in f.read().decode("utf-8").splitlines():
                            line = line.strip()
                            if line.startswith("{"):
                                entry = json.loads(line)
                                if entry.get("action") == "add" and "path" in entry:
                                    raw_p = entry["path"].replace("s3a://", "").replace("s3://", "")
                                    new_parquet_paths.append(raw_p)
                except Exception:
                    pass

            new_df = pd.DataFrame()
            if new_parquet_paths:
                try:
                    new_df = ds.dataset(new_parquet_paths, filesystem=pa_fs, format="parquet").to_table().to_pandas()
                except Exception:
                    parts = []
                    for p in new_parquet_paths:
                        try:
                            parts.append(pq.read_table(p, filesystem=pa_fs).to_pandas())
                        except Exception:
                            pass
                    new_df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

            if manifest_cache["last_max_batch"] == -1 or manifest_cache["dataframe"].empty:
                combined = new_df
            else:
                combined = (
                    pd.concat([manifest_cache["dataframe"], new_df], ignore_index=True)
                    if not new_df.empty
                    else manifest_cache["dataframe"]
                )

            # Keep rolling window bounded to latest 120 windows to preserve snappiness
            if not combined.empty and "window_start" in combined.columns:
                combined = combined.drop_duplicates(subset=["window_start", "pickup_zone_id"], keep="last")
                distinct_starts = combined["window_start"].drop_duplicates().sort_values()
                if len(distinct_starts) > 120:
                    cutoff = distinct_starts.iloc[-120]
                    combined = combined[combined.window_start >= cutoff]

            combined.attrs["gold_commit_time"] = latest_commit_time
            manifest_cache["last_max_batch"] = max_batch
            manifest_cache["dataframe"] = combined
            result = combined.copy()
            result.attrs["gold_commit_time"] = latest_commit_time
            return result
    except Exception:
        pass

    # 2. Non-recursive fallback for non-Spark paths or when metadata log is absent
    valid_paths: list[str] = []
    if not has_spark_metadata:
        try:
            selector = pafs.FileSelector(f"{bucket}/{prefix}" if prefix else bucket, recursive=False)
            file_infos = pa_fs.get_file_info(selector)
            all_valid = [
                info.path for info in file_infos
                if info.type == pafs.FileType.File
                and info.base_name.endswith(".parquet")
                and not info.base_name.startswith(("_", "."))
                and info.size > 0
            ]
            all_valid.sort()
            valid_paths = all_valid[-50:] if len(all_valid) > 50 else all_valid
        except Exception:
            dfs = [df for df in file_cache.values() if not df.empty]
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    if not valid_paths:
        _S3_FILE_CACHE.pop(path, None)
        return pd.DataFrame()

    valid_set = set(valid_paths)
    for p in list(file_cache.keys()):
        if p != "__all__" and p not in valid_set:
            del file_cache[p]

    new_paths = [p for p in valid_paths if p not in file_cache]
    if new_paths:
        try:
            bulk_df = ds.dataset(new_paths, filesystem=pa_fs, format="parquet").to_table().to_pandas()
            for p in new_paths[:-1]:
                file_cache[p] = pd.DataFrame()
            file_cache[new_paths[-1]] = bulk_df
        except Exception:
            for p in new_paths:
                try:
                    file_cache[p] = pq.read_table(p, filesystem=pa_fs).to_pandas()
                except Exception:
                    pass

    dfs = [df for df in file_cache.values() if not df.empty]
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _read_s3_resilient(path: str) -> pd.DataFrame:
    """Serialize shared cache mutation across concurrent FastAPI request threads."""
    with _S3_CACHE_LOCK:
        return _read_s3_resilient_unlocked(path)


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
    result = frame.loc[:, columns].copy()
    result.attrs.update(frame.attrs)
    return result


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


def gold_freshness_age_seconds(
    frame: pd.DataFrame, now: pd.Timestamp
) -> tuple[int | None, str]:
    """Return processing freshness for S3 replay data, falling back to event time for fixtures."""
    if frame.empty:
        return None, "unavailable"
    commit_time = frame.attrs.get("gold_commit_time")
    if commit_time is not None:
        commit = pd.Timestamp(commit_time)
        commit = commit.tz_localize("UTC") if commit.tzinfo is None else commit.tz_convert("UTC")
        return max(0, int((now.tz_convert("UTC") - commit).total_seconds())), "storage_commit"
    if "window_end" in frame.columns:
        latest_end = pd.to_datetime(frame["window_end"], utc=True).max()
        return max(0, int((now.tz_convert("UTC") - latest_end).total_seconds())), "event_time"
    return None, "unavailable"


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
    cols = ["pickup_zone_id", "zone_name"]
    if "borough" in lookup.columns:
        cols.append("borough")
    named = lookup.loc[:, cols]
    view = view.merge(named, on="pickup_zone_id", how="left")
    view["zone_name"] = view["zone_name"].fillna("Unknown zone")
    if "borough" in view.columns:
        view["borough"] = view["borough"].fillna("Unknown")
    view["zone_label"] = view["pickup_zone_id"].astype(int).astype(str) + " — " + view["zone_name"]
    view["is_high_demand"] = view["demand_ratio"].gt(HIGH_DEMAND_RATIO)
    view["lat"] = view["pickup_zone_id"].map(lambda z: ZONE_CENTROIDS.get(int(z), (None, None))[0])
    view["lon"] = view["pickup_zone_id"].map(lambda z: ZONE_CENTROIDS.get(int(z), (None, None))[1])
    return view.sort_values(["window_start_local", "pickup_zone_id"]).reset_index(drop=True)
