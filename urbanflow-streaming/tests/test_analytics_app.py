from pathlib import Path

from streamlit.testing.v1 import AppTest


def test_historical_table_is_loaded_only_on_request():
    app_path = Path(__file__).parents[1] / "analytics_app" / "app.py"
    app = AppTest.from_file(str(app_path)).run(timeout=30)

    assert not app.exception
    assert [checkbox.label for checkbox in app.checkbox] == ["Load historical sample"]
    assert len(app.dataframe) == 4

    app.checkbox[0].check().run(timeout=30)

    assert not app.exception
    assert len(app.dataframe) == 5
