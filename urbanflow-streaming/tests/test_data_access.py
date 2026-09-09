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
