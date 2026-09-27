from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

bench = json.loads(Path("reports/step2_partition_benchmark.json").read_text(encoding="utf-8-sig"))
rows = []
for run in bench["runs"]:
    total_jobs = len(run.get("spark_jobs", []))
    stage_count = sum(len(job.get("stages", [])) for job in run.get("spark_jobs", []))
    task_count = sum(stage.get("num_tasks", 0) for job in run.get("spark_jobs", []) for stage in job.get("stages", []))
    failed_tasks = sum(stage.get("num_failed_tasks", 0) for job in run.get("spark_jobs", []) for stage in job.get("stages", []))
    rows.append([str(run["partitions"]), f'{run["elapsed_seconds"]:.3f}s', str(run["output_rows"]), str(run["errors"]), str(total_jobs), str(stage_count), str(task_count), str(failed_tasks)])

fig, ax = plt.subplots(figsize=(12, 6))
ax.axis("off")
ax.set_title("Spark Step 2 Benchmark Evidence (statusTracker)", fontsize=16, weight="bold", pad=18)
columns = ["Partitions", "Runtime", "Rows", "Errors", "Jobs", "Stages", "Tasks", "Failed tasks"]
table = ax.table(cellText=rows, colLabels=columns, loc="center", cellLoc="center")
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1, 1.8)
for (r, c), cell in table.get_celld().items():
    if r == 0:
        cell.set_text_props(weight="bold", color="white")
        cell.set_facecolor("#2F5597")
    else:
        cell.set_facecolor("#EAF2F8" if r % 2 else "#FFFFFF")
fig.text(0.05, 0.08, "Source: reports/step2_partition_benchmark.json. This is not a browser screenshot; it is a rendered evidence panel from real Spark job/stage/task status.", fontsize=9)
out = Path("reports/figures/spark_ui_step2.png")
out.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(out, dpi=180, bbox_inches="tight")
print(out)
