#!/usr/bin/env python3
"""Build the submission-ready Assignment 3 PDF from verified JSON evidence."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from xml.sax.saxutils import escape

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pypdf import PdfReader
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import BaseDocTemplate, Frame, Image, PageBreak, PageTemplate, Paragraph, Spacer, Table, TableStyle

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
FIGURES = REPORTS / "figures"
OUT = REPORTS / "a3_final_report.pdf"
MD_OUT = REPORTS / "a3_final_report.md"
NAVY = colors.HexColor("#17365D")
TEAL = colors.HexColor("#1F6D78")
PALE = colors.HexColor("#EEF5FA")
GREY = colors.HexColor("#D6DCE4")
DARK = colors.HexColor("#3E4A59")


def load_data() -> dict[str, dict]:
    names = {1: "features_v2_validation", 2: "multiclass", 3: "multilabel", 4: "unsupervised", 5: "scalability_deployment"}
    return {f"s{i}": json.loads((REPORTS / f"a3_step{i}_{name}.json").read_text(encoding="utf-8")) for i, name in names.items()}


def f3(value: float) -> str:
    return f"{value:.3f}"


def make_styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()
    return {
        "top": ParagraphStyle("top", parent=base["Title"], fontName="Helvetica-Bold", fontSize=12, leading=14, alignment=TA_CENTER, textColor=NAVY),
        "title": ParagraphStyle("title2", parent=base["Title"], fontName="Helvetica-Bold", fontSize=20, leading=23, alignment=TA_CENTER, textColor=NAVY),
        "sub": ParagraphStyle("sub", parent=base["Normal"], fontName="Helvetica-Bold", fontSize=11, leading=14, alignment=TA_CENTER, textColor=TEAL),
        "meta": ParagraphStyle("meta", parent=base["Normal"], fontSize=9, leading=12, alignment=TA_CENTER, textColor=DARK),
        "h1": ParagraphStyle("h1x", parent=base["Heading1"], fontName="Helvetica-Bold", fontSize=13, leading=15, textColor=NAVY, spaceBefore=3, spaceAfter=5),
        "h2": ParagraphStyle("h2x", parent=base["Heading2"], fontName="Helvetica-Bold", fontSize=10.5, leading=12, textColor=TEAL, spaceBefore=4, spaceAfter=3),
        "body": ParagraphStyle("bodyx", parent=base["BodyText"], fontName="Helvetica", fontSize=8.35, leading=11.1, textColor=colors.HexColor("#202833"), spaceAfter=4),
        "small": ParagraphStyle("smallx", parent=base["BodyText"], fontName="Helvetica", fontSize=7.1, leading=9.1, textColor=colors.HexColor("#202833"), spaceAfter=2),
        "tiny": ParagraphStyle("tinyx", parent=base["BodyText"], fontName="Helvetica", fontSize=6.0, leading=7.1, textColor=colors.HexColor("#202833")),
        "head": ParagraphStyle("headx", parent=base["BodyText"], fontName="Helvetica-Bold", fontSize=6.8, leading=8.1, textColor=colors.white, alignment=TA_CENTER),
        "caption": ParagraphStyle("capx", parent=base["BodyText"], fontName="Helvetica-Oblique", fontSize=7, leading=8.4, alignment=TA_CENTER, textColor=DARK, spaceBefore=2, spaceAfter=3),
        "code": ParagraphStyle("codex", parent=base["Code"], fontName="Courier", fontSize=6.4, leading=7.8, leftIndent=5, rightIndent=5),
    }


def para(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(text, style)


def report_table(rows: list[list[object]], widths: list[float], st: dict[str, ParagraphStyle], *, small: bool = False, right: tuple[int, ...] = ()) -> Table:
    body_style = st["tiny"] if small else st["small"]
    cooked = [[para(escape(str(value)), st["head"] if row_no == 0 else body_style) for value in row] for row_no, row in enumerate(rows)]
    result = Table(cooked, colWidths=widths, repeatRows=1, hAlign="LEFT")
    commands = [("BACKGROUND", (0, 0), (-1, 0), NAVY), ("GRID", (0, 0), (-1, -1), 0.35, GREY),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4), ("TOPPADDING", (0, 0), (-1, -1), 2.6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2.6)]
    for row_no in range(2, len(rows), 2):
        commands.append(("BACKGROUND", (0, row_no), (-1, row_no), PALE))
    for column in right:
        commands.append(("ALIGN", (column, 1), (column, -1), "RIGHT"))
    result.setStyle(TableStyle(commands))
    return result


def scaled_image(path: Path, max_width: float, max_height: float) -> Image:
    image = Image(str(path)); ratio = min(max_width / image.imageWidth, max_height / image.imageHeight)
    image.drawWidth = image.imageWidth * ratio; image.drawHeight = image.imageHeight * ratio; image.hAlign = "CENTER"
    return image


def header_footer(canvas, doc) -> None:
    canvas.saveState(); width, height = A4; canvas.setStrokeColor(GREY); canvas.setLineWidth(0.45)
    canvas.line(1.6*cm, height-1.05*cm, width-1.6*cm, height-1.05*cm); canvas.setFont("Helvetica", 6.5)
    canvas.setFillColor(colors.HexColor("#657386")); canvas.drawString(1.6*cm, height-.82*cm, "BDA501 | Individual Assignment 3")
    canvas.drawRightString(width-1.6*cm, height-.82*cm, "Land-Cover Classification with Spark MLlib")
    canvas.line(1.6*cm, 1*cm, width-1.6*cm, 1*cm); canvas.drawCentredString(width/2, .68*cm, f"Page {doc.page}"); canvas.restoreState()


def make_charts(data: dict[str, dict], folder: Path) -> dict[str, Path]:
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 8, "axes.grid": True, "grid.color": "#DDE3EA", "axes.axisbelow": True})
    s2, s3, s4, s5 = data["s2"], data["s3"], data["s4"], data["s5"]; output = {}
    path = folder / "grid.png"; fig, ax = plt.subplots(figsize=(7.2, 2.8))
    marks = {"lr": "o", "rf": "s", "gbt_ovr": "^"}; labels = {"lr": "Logistic Regression", "rf": "Random Forest", "gbt_ovr": "GBT One-vs-Rest"}
    for group in s2["grid_results"]:
        rows = group["grid_results"]; ax.plot([r["config_id"]+1 for r in rows], [r["validation_macro_f1"] for r in rows], marker=marks[group["model_name"]], linewidth=1.8, label=labels[group["model_name"]])
    ax.set(xlabel="Configuration number within model grid", ylabel="Validation macro-F1", ylim=(.55,.9)); ax.legend(frameon=False, ncol=3, loc="lower center", bbox_to_anchor=(.5,-.38))
    fig.tight_layout(); fig.savefig(path, dpi=180, bbox_inches="tight"); plt.close(fig); output["grid"] = path
    path = folder / "importance.png"; top = list(reversed(s2["interpretation"]["rf_feature_importances"][:10])); fig, ax = plt.subplots(figsize=(7.2,2.9))
    ax.barh([r["feature"] for r in top], [r["importance"] for r in top], color="#4F81BD"); ax.set(xlabel="Random Forest importance", title="Top ten features")
    fig.tight_layout(); fig.savefig(path, dpi=180, bbox_inches="tight"); plt.close(fig); output["importance"] = path
    path = folder / "multilabel.png"; rows = sorted(s3["per_class"], key=lambda r:r["test"]["f1"])
    short = {"Industrial or commercial units":"Industrial / commercial", "Land principally occupied by agriculture, with significant areas of natural vegetation":"Agriculture + natural vegetation", "Natural grassland and sparsely vegetated areas":"Natural grassland / sparse vegetation", "Moors, heathland and sclerophyllous vegetation":"Moors / heathland", "Transitional woodland, shrub":"Transitional woodland"}
    fig, ax = plt.subplots(figsize=(7.2,4.5)); values = [r["test"]["f1"] for r in rows]
    ax.barh([short.get(r["class"],r["class"]) for r in rows], values, color=["#4F81BD" if v>0 else "#B8C2CC" for v in values]); ax.axvline(s3["summary"]["macro_f1"], color="#C0504D", linestyle="--", label="Macro-F1")
    ax.set(xlabel="Test F1", xlim=(0,1), title="Per-class multi-label performance"); ax.legend(frameon=False, loc="lower right")
    fig.tight_layout(); fig.savefig(path, dpi=180, bbox_inches="tight"); plt.close(fig); output["multilabel"] = path
    path = folder / "kmeans.png"; rows = s4["kmeans"]["k_grid"]; best = s4["kmeans"]["best_k"]; fig, ax = plt.subplots(figsize=(5.8,2.6))
    ax.plot([r["k"] for r in rows], [r["silhouette"] for r in rows], marker="o", color="#1F6D78"); ax.scatter([best["k"]],[best["silhouette"]], color="#C0504D", s=55, label=f"Best k={best['k']}")
    ax.set(xticks=range(3,11), xlabel="k", ylabel="Silhouette score", title="KMeans model selection"); ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(path, dpi=180, bbox_inches="tight"); plt.close(fig); output["kmeans"] = path
    path = folder / "scaling.png"; rows = s5["scaling"]["rows"]; fig, axes = plt.subplots(1,2,figsize=(7.2,2.8))
    for partitions, color in ((4,"#4F81BD"),(8,"#C0504D")):
        subset=[r for r in rows if r["partitions"]==partitions]; x=[int(r["train_fraction"]*100) for r in subset]
        axes[0].plot(x,[r["training_time_sec"] for r in subset],marker="o",color=color,label=f"{partitions} partitions"); axes[1].plot(x,[r["test_macro_f1"] for r in subset],marker="o",color=color,label=f"{partitions} partitions")
    axes[0].set(title="Training time",xlabel="Training fraction (%)",ylabel="Seconds"); axes[1].set(title="Test macro-F1",xlabel="Training fraction (%)",ylabel="Macro-F1",ylim=(.45,.66)); axes[1].legend(frameon=False,fontsize=7)
    fig.tight_layout(); fig.savefig(path,dpi=180,bbox_inches="tight"); plt.close(fig); output["scaling"] = path
    return output


def write_markdown(data: dict[str, dict]) -> None:
    s1,s2,s3,s4,s5=(data[f"s{i}"] for i in range(1,6)); v=s1["validation"]; tm=s2["test_metrics"]
    lines=["# BDA501 - Big Data Analytics","","## Individual Assignment 3","","**Land-Cover Classification at Scale with Apache Spark MLlib**","","**Student:** Tran Phan Vinh Hoang","","**Date:** 28 September 2026","","## Abstract","",
           f"This report processes {v['rows']:,} BigEarthNet-S2 patches from {len(v['countries'])} countries with the official split. Random Forest reached test accuracy {f3(tm['accuracy'])}, weighted-F1 {f3(tm['weighted_f1'])}, and macro-F1 {f3(tm['macro_f1'])}. Multi-label micro-F1 was {f3(s3['summary']['micro_f1'])}. PCA, KMeans, scaling, persistence and held-out-country inference are also evaluated.","",
           "## 1. Environment and data","",f"Local Spark/PySpark MLlib processed {v['rows']:,} patches. Split counts were {v['splits']['train']:,} train, {v['splits']['validation']:,} validation and {v['splits']['test']:,} test. No random split or random-fold tuning was used.","",
           "## 2. Richer features","",f"Features v2 contains {v['n_features']} columns with {v['errors']} errors, no null/NaN values and no range failures. New columns include six 20 m band means/std values, NDMI, NDBI and NDVI p10/p50/p90.","",
           "## 3. Multi-class classification","","### Model selection and validation","","The pipeline is StringIndexer -> VectorAssembler -> StandardScaler -> classifier with train-only inverse-frequency weights.","","| Model | Best validation macro-F1 | Best configuration |","|---|---:|---|"]
    for group in s2["grid_results"]:
        best=group["best"]; lines.append(f"| {group['model_name']} | {f3(best['validation_macro_f1'])} | {best['params']} |")
    lines += ["","### Assignment 2 baselines","",f"The selected RF test result was accuracy {f3(tm['accuracy'])}, weighted-F1 {f3(tm['weighted_f1'])}, and macro-F1 {f3(tm['macro_f1'])}. A2 majority, spectral-rule and nearest-centroid baselines are compared in the PDF; they use a smaller three-class task.","",
              "## 4. Multi-label classification","",f"Nineteen binary classifiers with validation-tuned thresholds reached micro-F1 {f3(s3['summary']['micro_f1'])} and macro-F1 {f3(s3['summary']['macro_f1'])}.","",
              "## 5. Unsupervised analysis","",f"PC1/PC2 explained {s4['pca']['explained_variance'][0]*100:.1f}% and {s4['pca']['explained_variance'][1]*100:.1f}%. KMeans selected k={s4['kmeans']['best_k']['k']} with silhouette {f3(s4['kmeans']['best_k']['silhouette'])}.","",
              "## 6. Scalability and deployment","","The best model was trained on 25%, 50% and 100% samples with 4 and 8 partitions. The saved PipelineModel reproduced test metrics exactly.",f"Held-out {s5['held_out_country']} accuracy was {f3(s5['batch_inference']['held_out_country_metrics']['accuracy'])}; macro-F1 was {f3(s5['batch_inference']['held_out_country_metrics']['macro_f1'])}.","",
              "## 7. Discussion and limitations","","The partial data contains 18 of 19 labels and only 13 exact-one training classes. Patch statistics lose spatial texture. Results use local Spark and an HDFS mirror.","","## 8. Conclusion","","All five required MLlib stages are implemented with fixed seeds, official splits and reproducible JSON evidence.","",
              "**AI assistance statement.** OpenAI Codex helped implement, verify and present the project; all numerical results come from submitted artifacts."]
    MD_OUT.write_text("\n".join(lines)+"\n",encoding="utf-8")


def build_pdf(data: dict[str, dict]) -> None:
    st=make_styles(); s1,s2,s3,s4,s5=(data[f"s{i}"] for i in range(1,6)); v=s1["validation"]; tm=s2["test_metrics"]
    mapping={int(r["label_index"]):r["class"] for r in s2["label_index_mapping"]}; width=A4[0]-3.2*cm
    doc=BaseDocTemplate(str(OUT),pagesize=A4,leftMargin=1.6*cm,rightMargin=1.6*cm,topMargin=1.3*cm,bottomMargin=1.25*cm,title="BDA501 Individual Assignment 3",author="Tran Phan Vinh Hoang")
    doc.addPageTemplates(PageTemplate(id="report",frames=[Frame(1.6*cm,1.25*cm,width,A4[1]-2.55*cm)],onPage=header_footer))
    with tempfile.TemporaryDirectory(prefix="a3_report_") as temp:
        charts=make_charts(data,Path(temp)); story=[]
        story += [Spacer(1,.45*cm),para("BDA501 - BIG DATA ANALYTICS",st["top"]),para("Individual Assignment 3",st["title"]),para("Land-Cover Classification at Scale with Apache Spark MLlib",st["sub"]),Spacer(1,.18*cm),para("Tran Phan Vinh Hoang &nbsp; | &nbsp; 28 September 2026",st["meta"]),Spacer(1,.55*cm),para("Abstract",st["h1"]),
                  para(f"This report extends the Assignment 2 BigEarthNet-S2 pipeline with Apache Spark MLlib. The experiment processes <b>{v['rows']:,} patches</b> from <b>{len(v['countries'])} countries</b> and preserves the official train, validation and test split. Twenty-eight spectral features support multi-class, multi-label and unsupervised learning. Random Forest achieved test accuracy <b>{f3(tm['accuracy'])}</b>, weighted-F1 <b>{f3(tm['weighted_f1'])}</b> and macro-F1 <b>{f3(tm['macro_f1'])}</b>. Binary relevance reached micro-F1 <b>{f3(s3['summary']['micro_f1'])}</b>. PCA and KMeans identified broad spectral groups, and a reloaded model produced held-out-country accuracy <b>{f3(s5['batch_inference']['held_out_country_metrics']['accuracy'])}</b>. All learned preprocessing is fitted on train only; validation selects models and test is used once.",st["body"]),
                  para("Study scope",st["h1"]),report_table([["Item","Verified value"],["Dataset","BigEarthNet-S2 partial HDF5 collection"],["Scale",f"{v['rows']:,} patches; {len(v['countries'])} countries"],["Official split",f"{v['splits']['train']:,} train / {v['splits']['validation']:,} validation / {v['splits']['test']:,} test"],["Feature table",f"{v['n_features']} numerical features; {v['errors']} errors"],["Tasks","Multi-class, multi-label, PCA, KMeans, scaling and inference"]],[4.2*cm,12*cm],st),
                  Spacer(1,.3*cm),para("Implemented workflow",st["h2"]),report_table([["Features v2","Multi-class","Multi-label","PCA + KMeans","Deploy"],["28 features","LR / RF / GBT","19 models","k = 3..10","Save / reload"]],[3.24*cm]*5,st,small=True),Spacer(1,.25*cm),para("The full repository was not downloaded. Four HDF5 shards from Assignment 2 meet the minimum scale requirement.",st["small"]),PageBreak()]
        story += [para("1. Environment, data and experimental design",st["h1"]),para("The verified run uses local Spark/PySpark MLlib on Windows and the required <font name='Courier'>hdfs_mirror/bigearthnet</font> layout. This is a local HDFS mirror, not a live distributed HDFS service. Fixed seeds are stored in shared configuration. The official split column is used throughout; randomSplit and random-fold tuning are not used.",st["body"]),
                  report_table([["Environment item","Verified value"],["Execution","Apache Spark 3.5.9 / PySpark MLlib, local Windows mode"],["Storage","hdfs_mirror/bigearthnet"],["Countries",", ".join(f"{k} ({n:,})" for k,n in v["countries"].items())],["Leakage control","Train-fitted transformers; validation selection; final test once"],["Reproducibility","One script per step, shared config, requirements and seeds"]],[4.2*cm,12*cm],st),
                  para("2. Step 1 - richer feature engineering",st["h1"]),para("The executor-side job retains the Assignment 2 features and adds mean and standard deviation for B05, B06, B07, B8A, B11 and B12. These bands remain on their native 60 x 60 grid. NDMI and NDBI are computed per pixel and averaged; NDVI p10, p50 and p90 preserve within-patch variation.",st["body"]),
                  report_table([["Feature group","Columns","Count"],["Assignment 2 base","B02/B03/B04/B08 mean and std; NDVI; NDWI; brightness",11],["20 m bands","B05/B06/B07/B8A/B11/B12 mean and std",12],["New indices","NDMI and NDBI",2],["NDVI distribution","p10, p50 and p90",3],["Total","Saved to bigearthnet/features_v2",28]],[4*cm,10.2*cm,2*cm],st),para("2.1 Validation",st["h2"]),
                  report_table([["Check","Result"],["Input / output rows",f"{v['rows']:,} / {v['rows']:,}"],["Extraction errors",v["errors"]],["Null / NaN values",f"{sum(v['null_counts'].values())} / {sum(v['nan_counts'].values())}"],["Index range failures",sum(v["bad_ranges"].values())],["Runtime",f"{v['elapsed_sec']:.3f} s"]],[7.8*cm,8.4*cm],st),para("Row preservation, zero errors and bounded indices show that richer features were added without invalid numerical values.",st["body"]),PageBreak()]
        counts={split:{r["target"]:r["count"] for r in s2["class_counts_per_split"][split]} for split in ("train","validation","test")}; observed=[[name,counts["train"].get(name,0),counts["validation"].get(name,0),counts["test"].get(name,0)] for name in s2["selected_top_classes"] if any(name in counts[x] for x in counts)]
        grid=[["Model","Configuration","Val. macro-F1","Time (s)"]]; model_name={"lr":"LR","rf":"RF","gbt_ovr":"GBT-OvR"}
        for group in s2["grid_results"]:
            for row in group["grid_results"]: grid.append([model_name[row["model"]],", ".join(f"{k}={val}" for k,val in row["params"].items()),f3(row["validation_macro_f1"]),f"{row['training_time_sec']:.3f}"])
        story += [para("3. Step 2 - multi-class classification",st["h1"]),para("The task keeps patches containing exactly one selected frequent label. The partial data contains 18 observed labels, but only 13 appear as exact-one classes in train. Several classes have fewer than ten rows, so inverse-frequency weights and macro-F1 are essential.",st["body"]),para("3.1 Exact-one class counts by official split",st["h2"]),report_table([["Exact-one target","Train","Validation","Test"]]+observed,[10.4*cm,1.9*cm,2.1*cm,1.8*cm],st,small=True,right=(1,2,3)),para("3.2 Leakage-safe pipeline",st["h2"]),report_table([["StringIndexer","VectorAssembler","StandardScaler","Classifier"],["Fit on train","28 inputs","Fit on train","LR / RF / GBT-OvR"]],[4.05*cm]*4,st,small=True),para("Class weights come from train only. Each ParamGridBuilder configuration is fitted on train and selected by validation macro-F1.",st["small"]),para("3.3 Model selection and validation",st["h2"]),report_table(grid,[2.2*cm,7.3*cm,3.5*cm,3.2*cm],st,small=True,right=(2,3)),Spacer(1,.1*cm),scaled_image(charts["grid"],15.3*cm,4.9*cm),para("Figure 1. Validation macro-F1 for every configuration. RF with 40 trees and depth 8 was selected at 0.845. GBT-OneVsRest was much slower locally.",st["caption"]),PageBreak()]
        baseline=[["Model","Accuracy","Macro-F1","Scope"]]
        for key,name in (("majority","A2 majority"),("rule","A2 spectral rule"),("centroid_full","A2 centroid - 11"),("centroid_reduced","A2 centroid - 5")):
            row=s2["assignment2_baselines"][key]; baseline.append([name,f3(row["accuracy"]),f3(row["macro_f1"]),"A2 three-class"])
        baseline.append(["A3 Random Forest",f3(tm["accuracy"]),f3(tm["macro_f1"]),"A3 exact-one"]); per_class=[["Test class","Support","Precision","Recall","F1"]]
        for row in tm["per_class"]: per_class.append([mapping.get(int(row["label_index"]),"Unseen-in-train bucket"),row["tp"]+row["fn"],f3(row["precision"]),f3(row["recall"]),f3(row["f1"])])
        topk=[["Features","Accuracy","Weighted-F1","Macro-F1"],["Full 28",f3(tm["accuracy"]),f3(tm["weighted_f1"]),f3(tm["macro_f1"])]]
        for row in s2["top_k_retrain"]: m=row["test_metrics"]; topk.append([f"Top {row['k']}",f3(m["accuracy"]),f3(m["weighted_f1"]),f3(m["macro_f1"])])
        story += [para("3.4 Test performance and Assignment 2 baselines",st["h1"]),para(f"The selected RF was evaluated once on {tm['n']:,} test rows: accuracy <b>{f3(tm['accuracy'])}</b>, weighted-F1 <b>{f3(tm['weighted_f1'])}</b>, and macro-F1 <b>{f3(tm['macro_f1'])}</b>. The large accuracy-to-macro-F1 gap exposes weak long-tail performance.",st["body"]),report_table(baseline,[5*cm,2.5*cm,2.5*cm,6.2*cm],st,right=(1,2)),para("Assignment 2 baselines use only Pastures, Arable land and Marine waters; the A3 RF has a broader label space. The table provides continuity, not a direct ranking.",st["small"]),para("3.5 Per-class test behaviour",st["h2"]),report_table(per_class,[7*cm,2*cm,2.4*cm,2.4*cm,2.4*cm],st,small=True,right=(1,2,3,4)),para("3.6 Interpretation and top-k retraining",st["h2"]),scaled_image(charts["importance"],11.8*cm,4.7*cm),para("Figure 2. NDVI p10 ranks first; SWIR means B11/B12 and visible/NIR summaries also contribute. LR coefficients rank std_b05, mean_b12, std_b11 and NDVI p10 highest.",st["caption"]),report_table(topk,[5.4*cm,3.6*cm,3.6*cm,3.6*cm],st,small=True,right=(1,2,3)),para("Top-5 and top-10 models reduce macro-F1, showing that rare classes need information across the full feature set.",st["small"]),PageBreak()]
        summary=s3["summary"]; multi=[["Class","Train +","Threshold","Precision","Recall","F1"]]
        for row in s3["per_class"]: multi.append([row["class"],f"{row['train_positive']:,}","N/A" if row["best_threshold"] is None else f"{row['best_threshold']:.1f}",f3(row["test"]["precision"]),f3(row["test"]["recall"]),f3(row["test"]["f1"])])
        story += [para("4. Step 3 - multi-label classification",st["h1"]),para("Nineteen binary columns are created. A train-fitted feature PipelineModel is reused by one Logistic Regression classifier per class. Thresholds from 0.1 to 0.9 are selected on validation and then applied once to test.",st["body"]),report_table([["Metric","Value","Meaning"],["Micro-precision",f3(summary["micro_precision"]),"All decisions pooled"],["Micro-recall",f3(summary["micro_recall"]),"All decisions pooled"],["Micro-F1",f3(summary["micro_f1"]),"Frequent labels dominate"],["Macro-F1",f3(summary["macro_f1"]),"Equal class weight"]],[4.1*cm,2.4*cm,9.7*cm],st),para("4.1 Per-class multi-label performance",st["h2"]),report_table(multi,[6.2*cm,1.8*cm,1.8*cm,2.2*cm,2.2*cm,2*cm],st,small=True,right=(1,2,3,4,5)),Spacer(1,.08*cm),scaled_image(charts["multilabel"],14.2*cm,6.5*cm),para("Figure 3. Test F1 for all 19 labels. Marine waters, Pastures and Arable land are strongest. Agro-forestry has no positive training rows; Coastal wetlands and Beaches/dunes/sands are too rare to produce true positives.",st["caption"]),PageBreak()]
        cont=[["Cluster","Dominant class","Count","Interpretation"]]
        for row in s4["kmeans"]["cluster_class_contingency"]:
            dominant,count=max(((k,n) for k,n in row.items() if k!="prediction"),key=lambda x:x[1]); cont.append([row["prediction"],dominant,f"{count:,}","Broad spectral regime"])
        ev=s4["pca"]["explained_variance"]
        story += [para("5. Step 4 - unsupervised analysis",st["h1"]),para(f"StandardScaler and PCA are fitted on train inside a Pipeline. PC1 explains <b>{ev[0]*100:.1f}%</b> and PC2 <b>{ev[1]*100:.1f}%</b>; together they retain <b>{sum(ev)*100:.1f}%</b> of variance.",st["body"]),scaled_image(FIGURES/"a3_pca_scatter.png",13.7*cm,8.1*cm),para("Figure 4. PC1 and PC2 coloured by exact-one class. Marine waters separates clearly; vegetation and agriculture overlap because summaries remove spatial texture.",st["caption"]),para("5.1 KMeans model selection",st["h2"]),scaled_image(charts["kmeans"],11.2*cm,5.1*cm),para(f"Figure 5. Silhouette for k=3..10. The maximum is {f3(s4['kmeans']['best_k']['silhouette'])} at k={s4['kmeans']['best_k']['k']}.",st["caption"]),report_table(cont,[2*cm,7.1*cm,2*cm,5.1*cm],st,small=True,right=(2,)),para("Clusters do not equal semantic classes. Cluster 0 is mostly Marine waters, cluster 1 includes Arable land, and cluster 2 is mostly Pastures. Multi-label semantics and Euclidean distance prevent a perfect match.",st["body"]),PageBreak()]
        scaling=[["Train fraction","Partitions","Rows","Time (s)","Test macro-F1"]]
        for row in s5["scaling"]["rows"]: scaling.append([f"{int(row['train_fraction']*100)}%",row["partitions"],f"{row['train_rows']:,}",f"{row['training_time_sec']:.3f}",f3(row["test_macro_f1"])])
        held=s5["batch_inference"]["held_out_country_metrics"]; saved=s5["persistence"]["after"]
        story += [para("6. Step 5 - scalability and deployment",st["h1"]),para("The selected RF is retrained on stratified 25%, 50% and 100% samples with four and eight partitions. The sampled training frame is cached. Timings describe this laptop and are not a claim of linear cluster scaling.",st["body"]),report_table(scaling,[3.4*cm,2.7*cm,3*cm,3.2*cm,3.9*cm],st,right=(0,1,2,3,4)),Spacer(1,.1*cm),scaled_image(charts["scaling"],14.8*cm,5.9*cm),para("Figure 6. Training time and test macro-F1. Full data gives the best macro-F1; partition effects vary because scheduling overhead matters locally.",st["caption"]),para("6.1 Persistence",st["h2"]),para(f"The PipelineModel is saved under <font name='Courier'>bigearthnet/models</font>, reloaded and evaluated again. Exact metric reproduction: <b>{s5['persistence']['reload_reproduces_test_metrics_exactly']}</b>.",st["body"]),report_table([["Reloaded model","Rows","Accuracy","Weighted-F1","Macro-F1"],["RF PipelineModel",f"{saved['n']:,}",f3(saved["accuracy"]),f3(saved["weighted_f1"]),f3(saved["macro_f1"]) ]],[5.5*cm,2.3*cm,2.8*cm,3*cm,2.6*cm],st,right=(1,2,3,4)),para("6.2 Batch inference and Domain shift",st["h2"]),report_table([["Held-out country","Rows","Accuracy","Weighted-F1","Macro-F1"],[s5["held_out_country"],f"{held['n']:,}",f3(held["accuracy"]),f3(held["weighted_f1"]),f3(held["macro_f1"]) ]],[5.5*cm,2.3*cm,2.8*cm,3*cm,2.6*cm],st,right=(1,2,3,4)),para("Lithuania is excluded from fitting and used for country-level inference. Accuracy remains high because common classes dominate, but macro-F1 falls to 0.513. This indicates domain shift in class mixture and spectral conditions. Predictions are saved under <font name='Courier'>bigearthnet/predictions</font> partitioned by country.",st["body"]),PageBreak()]
        story += [para("7. Discussion",st["h1"]),para("The project provides a reproducible Spark MLlib workflow across supervised, multi-label, unsupervised and deployment tasks. NDVI percentiles and SWIR features rank highly because they describe vegetation variation, moisture and built surfaces. Accuracy must be read with macro-F1: frequent classes are learned well, while rare classes have little support and may be absent from a split.",st["body"]),para("The multi-label results show the same pattern. Patch means, standard deviations and percentiles remove texture, edges, shapes and spatial arrangement. An image model could use these signals but would require more compute and a different scope.",st["body"]),para("7.1 Limitations and transparent deviations",st["h2"]),report_table([["Limitation","Effect and treatment"],["Partial dataset","52,607 patches meet scale, but the complete corpus was not downloaded."],["18 of 19 labels","Agro-forestry has no positive examples and cannot be learned."],["Fewer than 35 exact-one classes","Only 13 occur in train; scripts request 35 and report availability."],["Local Spark / HDFS mirror","No claim of cluster speed-up or live HDFS evidence."],["Patch summaries","Spectral statistics do not represent texture or geometry."]],[5*cm,11.2*cm],st),para("7.2 Reproducibility",st["h2"]),para("Run the scripts in order; all use shared configuration from <font name='Courier'>scripts/a3_config.py</font>:",st["body"]),Table([[para(".\\.venv\\Scripts\\python.exe scripts\\05_a3_features_v2.py --partitions 16<br/>.\\.venv\\Scripts\\python.exe scripts\\06_a3_multiclass.py<br/>.\\.venv\\Scripts\\python.exe scripts\\07_a3_multilabel.py<br/>.\\.venv\\Scripts\\python.exe scripts\\08_a3_unsupervised.py<br/>.\\.venv\\Scripts\\python.exe scripts\\09_a3_scalability_deploy.py",st["code"])]],colWidths=[16.2*cm],style=TableStyle([("BACKGROUND",(0,0),(-1,-1),colors.HexColor("#F3F5F7")),("BOX",(0,0),(-1,-1),.5,GREY),("LEFTPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6)])),para("8. Conclusion",st["h1"]),para(f"All five required stages were completed on {v['rows']:,} patches with the official split. RF reached {f3(tm['accuracy'])} accuracy and {f3(tm['macro_f1'])} macro-F1; multi-label micro-F1 was {f3(s3['summary']['micro_f1'])}; KMeans selected k=3; and the saved model reproduced its metrics before held-out-country scoring. The main constraints are partial-data class coverage and patch-level representation.",st["body"]),para("AI assistance statement",st["h2"]),para("OpenAI Codex was used to help implement, verify and present this assignment. All counts, timings and metrics come from submitted JSON evidence and can be reproduced from the scripts.",st["small"])]
        doc.build(story)


def main() -> None:
    data=load_data(); write_markdown(data); build_pdf(data); reader=PdfReader(str(OUT)); text="\n".join(page.extract_text() or "" for page in reader.pages)
    required=("Tran Phan Vinh Hoang","Abstract","Model selection and validation","Assignment 2 baselines","Per-class multi-label performance","KMeans model selection","Domain shift","Conclusion")
    assert 5 <= len(reader.pages) <= 10; assert all(value in text for value in required)
    assert "Requirement checkpoint audit" not in text and "C:/Users/DELL" not in text and "C:\\Users\\DELL" not in text
    print(OUT); print("pages",len(reader.pages)); print("verified_sections",len(required))


if __name__ == "__main__":
    main()
