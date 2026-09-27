import pytest


def test_fixture_data_access(foundation):
    from analytics_app.data_access import GoldReader
    root = foundation / "fixtures/gold"
    reader = GoldReader(str(root / "historical_zone_baseline.jsonl"),
                        str(root / "expected_live_zone_metrics.jsonl"))
    assert len(reader.historical()) == 2
    assert reader.latest().current_trip_count.sum() == 7
    assert list(reader.filtered_live(zones=[161]).pickup_zone_id) == [161]


def test_missing_columns_are_rejected(tmp_path):
    from analytics_app.data_access import GoldReader
    broken = tmp_path / "broken.jsonl"
    broken.write_text('{"pickup_zone_id": 1}\n')
    with pytest.raises(ValueError, match="missing columns"):
        GoldReader(str(broken), str(broken)).live()


def test_s3a_paths_are_read_with_seaweedfs_storage_options(monkeypatch):
    import pandas as pd
    from analytics_app import data_access

    calls = []

    def fake_read_parquet(path, **kwargs):
        calls.append((path, kwargs))
        return pd.DataFrame({
            "window_start": ["2023-02-01T00:00:00Z"],
            "window_end": ["2023-02-01T00:05:00Z"],
            "pickup_zone_id": [230],
            "current_trip_count": [1],
            "historical_mean_trip_count": [0.5],
            "demand_ratio": [2.0],
            "z_score": [1.0],
        })

    monkeypatch.setenv("URBANFLOW_S3_ENDPOINT", "http://localhost:8333")
    monkeypatch.setattr(data_access.pd, "read_parquet", fake_read_parquet)

    frame = data_access.GoldReader(
        "unused.jsonl",
        "s3a://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003/",
    ).live()

    assert len(frame) == 1
    assert calls == [
        (
            "s3://urbanflow/gold/live_zone_metrics/task6/task6_20260914_003/",
            {
                "storage_options": {
                    "client_kwargs": {"endpoint_url": "http://localhost:8333"},
                    "key": "minioadmin",
                    "secret": "minioadmin",
                    "use_ssl": False,
                }
            },
        )
    ]


def test_s3a_zone_lookup_uses_seaweedfs_storage_options(monkeypatch):
    import pandas as pd
    from analytics_app import zones

    calls = []

    def fake_read_csv(path, **kwargs):
        calls.append((path, kwargs))
        return pd.DataFrame({"LocationID": [4], "Zone": ["Alphabet City"]})

    monkeypatch.setenv("URBANFLOW_S3_ENDPOINT", "http://seaweedfs:8333")
    monkeypatch.setattr(zones.pd, "read_csv", fake_read_csv)

    lookup = zones.load_zone_lookup("s3a://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv")

    assert lookup.to_dict("records") == [{"pickup_zone_id": 4, "zone_name": "Alphabet City"}]
    assert calls == [
        (
            "s3://urbanflow/bronze/zone_lookup/taxi_zone_lookup.csv",
            {
                "storage_options": {
                    "client_kwargs": {"endpoint_url": "http://seaweedfs:8333"},
                    "key": "minioadmin",
                    "secret": "minioadmin",
                    "use_ssl": False,
                }
            },
        )
    ]


def test_prepare_live_view_uses_new_york_time_and_zone_names(foundation):
    from analytics_app.data_access import GoldReader, prepare_live_view
    from analytics_app.zones import load_zone_lookup

    root = foundation / "fixtures/gold"
    live = GoldReader(
        str(root / "historical_zone_baseline.jsonl"),
        str(root / "expected_live_zone_metrics.jsonl"),
    ).live()
    lookup = load_zone_lookup(foundation / "fixtures/bronze/zone_lookup.csv")
    view = prepare_live_view(live, lookup)

    midtown = view[view.pickup_zone_id == 161].iloc[0]
    assert midtown.zone_label == "161 — Midtown Center"
    assert str(midtown.window_start_local) == "2025-01-06 08:00:00"
    assert midtown.window_label == "2025-01-06 08:00 NY"
    assert bool(midtown.is_high_demand) is True
    assert midtown.lat == pytest.approx(40.7547, abs=0.02)
    assert midtown.lon == pytest.approx(-73.9812, abs=0.02)


def test_single_window_chart_is_grouped_not_line():
    from analytics_app.data_access import chart_mode_for

    import pandas as pd
    one_window = pd.DataFrame({"window_start_local": [pd.Timestamp("2025-01-06 08:00")] * 2})
    two_windows = pd.DataFrame({
        "window_start_local": [
            pd.Timestamp("2025-01-06 08:00"),
            pd.Timestamp("2025-01-06 08:05"),
        ]
    })
    assert chart_mode_for(one_window) == "grouped"
    assert chart_mode_for(two_windows) == "line"


def test_default_zone_ids_uses_latest_window_and_caps_by_current_activity():
    from analytics_app.data_access import default_zone_ids

    import pandas as pd

    view = pd.DataFrame({
        "window_start_local": pd.to_datetime([
            "2025-01-06 07:55",
            "2025-01-06 08:00",
            "2025-01-06 08:00",
            "2025-01-06 08:00",
        ]),
        "pickup_zone_id": [99, 4, 7, 2],
        "current_trip_count": [100, 1, 9, 5],
    })

    assert default_zone_ids(view, limit=2) == [7, 2]


def test_historical_sample_filters_selected_zones_and_caps_rows():
    from analytics_app.data_access import prepare_historical_sample

    import pandas as pd

    historical = pd.DataFrame({
        "pickup_zone_id": [1, 2, 2, 2],
        "hour": [0, 0, 1, 2],
    })
    lookup = pd.DataFrame({
        "pickup_zone_id": [1, 2],
        "zone_name": ["Newark Airport", "Jamaica"],
    })

    sample, total_rows = prepare_historical_sample(
        historical,
        lookup,
        selected_ids=[2],
        limit=2,
    )

    assert total_rows == 3
    assert sample.to_dict("records") == [
        {"pickup_zone_id": 2, "hour": 0, "zone_name": "Jamaica"},
        {"pickup_zone_id": 2, "hour": 1, "zone_name": "Jamaica"},
    ]


def test_choropleth_paints_live_zones_and_leaves_others_grey(foundation):
    from analytics_app.data_access import GoldReader, HIGH_DEMAND_RATIO, prepare_live_view
    from analytics_app.zones import attach_demand_to_geojson, load_taxi_zone_geojson, load_zone_lookup, ratio_fill_color

    red = ratio_fill_color(2.5)
    grey = ratio_fill_color(None)
    assert red[0] > red[1] and red[0] > red[2]
    assert grey[0] == grey[1] == grey[2]
    assert 2.5 > HIGH_DEMAND_RATIO

    live = GoldReader(
        str(foundation / "fixtures/gold/historical_zone_baseline.jsonl"),
        str(foundation / "fixtures/gold/expected_live_zone_metrics.jsonl"),
    ).live()
    view = prepare_live_view(live, load_zone_lookup(foundation / "fixtures/bronze/zone_lookup.csv"))
    geo = attach_demand_to_geojson(load_taxi_zone_geojson(), view)
    by_id = {f["properties"]["location_id"]: f["properties"] for f in geo["features"]}
    assert len(geo["features"]) >= 200
    assert by_id[161]["demand_ratio"] == 2.5
    assert by_id[161]["fill_color"] == list(red)
    assert by_id[236]["demand_ratio"] == 2.0
    assert by_id[1]["demand_ratio"] is None
    assert by_id[1]["fill_color"] == list(grey)
    ring = geo["features"][next(i for i, f in enumerate(geo["features"]) if f["properties"]["location_id"] == 161)]["geometry"]["coordinates"][0]
    lons, lats = zip(*ring)
    assert -74.05 < min(lons) < max(lons) < -73.9
    assert 40.70 < min(lats) < max(lats) < 40.80
