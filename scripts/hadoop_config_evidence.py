"""Capture Hadoop configuration values visible to Spark's Hadoop Configuration."""
from __future__ import annotations

import json
from pathlib import Path

from scripts.lib.spark_session import get_spark


def _get(conf, key: str):
    val = conf.get(key)
    return val if val not in (None, "") else None


def main() -> None:
    spark = get_spark("BEN-Hadoop-Config-Evidence")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    block = _get(conf, "dfs.blocksize") or _get(conf, "dfs.block.size")
    repl = _get(conf, "dfs.replication")
    fs_default = _get(conf, "fs.defaultFS")
    payload = {
        "source": "Spark Hadoop Configuration, not hdfs CLI",
        "fs_defaultFS": fs_default,
        "dfs_blocksize_raw": block,
        "dfs_replication_raw": repl,
        "block_size_bytes": int(block) if block and str(block).isdigit() else None,
        "block_size_mib": round(int(block) / 1024 / 1024, 3) if block and str(block).isdigit() else None,
        "replication_factor": int(repl) if repl and str(repl).isdigit() else None,
        "interpretation": "These are Hadoop configuration defaults visible to Spark. They are not proof that a real HDFS cluster was running unless fs.defaultFS points to hdfs:// and hdfs CLI/NameNode evidence is available.",
    }
    Path("reports/hadoop_config_via_spark.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
