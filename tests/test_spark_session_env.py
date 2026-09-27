from pathlib import Path

from scripts.lib.spark_session import candidate_java_home


def test_candidate_java_home_accepts_existing_java(tmp_path: Path):
    home = tmp_path / "jbr"
    bin_dir = home / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "java.exe").write_text("", encoding="utf-8")
    assert candidate_java_home([home]) == str(home)


def test_candidate_java_home_returns_none_when_missing(tmp_path: Path):
    assert candidate_java_home([tmp_path / "missing"]) is None
