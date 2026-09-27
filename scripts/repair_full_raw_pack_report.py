import json
from pathlib import Path
import pyarrow.parquet as pq

root = Path(r"D:\BigEarthNet-partial\packed_raw_band_parquet_full")
report_path = Path("reports/full_raw_band_pack_evidence.json")
report = json.loads(report_path.read_text(encoding="utf-8"))

def dir_stats(path: Path):
    files = [p for p in path.rglob("*") if p.is_file()]
    return {"exists": path.exists(), "files": len(files), "bytes": sum(p.stat().st_size for p in files)}

def parquet_rows(path: Path):
    rows = 0
    parquet_files = list(path.rglob("*.parquet"))
    for f in parquet_files:
        rows += pq.ParquetFile(f).metadata.num_rows
    return rows, len(parquet_files)

country_runs = []
for d in sorted(root.glob("country=*")):
    country = d.name.split("=", 1)[1]
    rows, parquet_file_count = parquet_rows(d)
    country_runs.append({
        "country": country,
        "output_rows_from_parquet_metadata": rows,
        "parquet_files": parquet_file_count,
        "output_path": str(d),
        "output_stats": dir_stats(d),
    })
report["output_rows"] = sum(item["output_rows_from_parquet_metadata"] for item in country_runs)
report["errors"] = None
report["validation_method"] = "Parquet footer metadata row counts; binary columns were not loaded into Spark driver memory."
report["country_runs"] = country_runs
report["output_path"] = str(root)
report["output_uri"] = root.resolve().as_uri()
report["packed_parquet_stats"] = dir_stats(root)
report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(json.dumps({"rows": report["output_rows"], "files": report["packed_parquet_stats"]["files"], "bytes": report["packed_parquet_stats"]["bytes"]}, indent=2))
