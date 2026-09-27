#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt

root = Path(__file__).resolve().parents[1]
report = json.loads((root / "reports" / "a3_step5_scalability_deployment.json").read_text(encoding="utf-8"))
rows = report["scaling"]["rows"]
fig, ax1 = plt.subplots(figsize=(8, 5))
for partitions in sorted({r["partitions"] for r in rows}):
    sub = sorted([r for r in rows if r["partitions"] == partitions], key=lambda r: r["train_fraction"])
    ax1.plot([r["train_fraction"] for r in sub], [r["training_time_sec"] for r in sub], marker="o", label=f"time p={partitions}")
ax1.set_xlabel("Training fraction")
ax1.set_ylabel("Training time (seconds)")
ax2 = ax1.twinx()
for partitions in sorted({r["partitions"] for r in rows}):
    sub = sorted([r for r in rows if r["partitions"] == partitions], key=lambda r: r["train_fraction"])
    ax2.plot([r["train_fraction"] for r in sub], [r["test_macro_f1"] for r in sub], marker="x", linestyle="--", label=f"macro-F1 p={partitions}")
ax2.set_ylabel("Test macro-F1")
handles1, labels1 = ax1.get_legend_handles_labels()
handles2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(handles1 + handles2, labels1 + labels2, loc="best", fontsize=8)
ax1.set_title("Assignment 3 scaling: time and macro-F1")
fig.tight_layout()
out = root / "reports" / "figures" / "a3_scalability.png"
fig.savefig(out, dpi=160)
print(out)
