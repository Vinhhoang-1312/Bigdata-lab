# BigEarthNet-S2 v2.0 — A→Z Reference (for this lab)

Source of truth for the assignment pipeline. Prefer this over ad-hoc assumptions.

## 1. What it is

| Item | Value |
|---|---|
| Full name | BigEarthNet v2.0 / reBEN (refined BigEarthNet) |
| Modality used here | **BigEarthNet-S2** only (Sentinel-2 optical) |
| Patches | **549,488** patches from **115** S2 tiles |
| Geography | **10** European countries: Austria, Belgium, Finland, Ireland, Kosovo, Lithuania, Luxembourg, Portugal, Serbia, Switzerland |
| Time window | June 2017 – May 2018 |
| Atmosphere | Level-2A (sen2cor v2.11) |
| Patch size (10 m) | **120 × 120** px ≈ 1.2 km × 1.2 km |
| Labels | Multi-label from CLC2018 → **19-class** nomenclature |
| Official site | https://bigearth.net/ |
| Archive size (S2) | ≈ **59 GiB** (`BigEarthNet-S2.tar.zst`) |

This course only needs S2 + `metadata.parquet` (recommended patches).

## 2. Files you download

| File | Role | Use in this lab? |
|---|---|---|
| `BigEarthNet-S2.tar.zst` | All S2 GeoTIFFs | Yes (or a **subset** of tiles) |
| `metadata.parquet` | Recommended patches (no snow/cloud/shadow) | **Yes — always** |
| `metadata_for_patches_with_snow_cloud_or_shadow.parquet` | Excluded noisy patches | Optional (do not mix into train unless studying noise) |
| Reference maps / S1 archive | Pixel maps / SAR | Out of scope |

**Recommended patches** = rows in `metadata.parquet`. Assignment expects these.

## 3. On-disk layout (v2)

After extract, typical roots:

```
<data_root>/
  metadata.parquet
  BigEarthNet-S2/                    # sometimes the archive root IS this folder
    <TILE_ID>/
      <PATCH_ID>/
        <PATCH_ID>_B01.tif
        <PATCH_ID>_B02.tif
        ...
        <PATCH_ID>_B12.tif
        <PATCH_ID>_B8A.tif
```

Assignment wording:

```text
<TILE>/<PATCH_ID>/<PATCH_ID>_<BAND>.tif
```

### Patch ID anatomy

Example:

```text
S2B_MSIL2A_20180421T100029_N9999_R122_T33TWM_00_00
└──────── product / sensing / processing / tile MGRS ─┘ └H┘ └V┘
```

- Last two `_`-tokens = horizontal / vertical patch index inside the tile (`00_00`, …).
- **Tile directory name** = `patch_id` **without** those last two tokens:

```text
tile_id = "_".join(patch_id.split("_")[:-2])
# → S2B_MSIL2A_20180421T100029_N9999_R122_T33TWM
```

Band file:

```text
{patch_id}_B02.tif
```

## 4. Spectral bands

| Band | Name | Native res | Size in patch | Use in assignment |
|---|---|---|---|---|
| B01 | Coastal aerosol | 60 m | 20×20 | No |
| **B02** | **Blue** | **10 m** | **120×120** | **Yes** |
| **B03** | **Green** | **10 m** | **120×120** | **Yes** |
| **B04** | **Red** | **10 m** | **120×120** | **Yes** |
| B05–B07, B8A, B11–B12 | Red-edge / SWIR | 20 m | 60×60 | No (size mismatch) |
| **B08** | **NIR** | **10 m** | **120×120** | **Yes** |
| B09 | Water vapour | 60 m | 20×20 | No |

**Pitfall:** never `stack` 10 m with 20 m/60 m without resampling. This lab only reads four 10 m bands.

## 5. Pixel values

- dtype: **uint16**
- semantics: surface reflectance × **10⁴** (so divide by `10000.0` → reflectance in ~[0, 1])
- **Pitfall:** uint16 subtraction wraps. Cast to `float32` **before** NDVI/NDWI.

## 6. `metadata.parquet` columns

| Column | Type | Meaning |
|---|---|---|
| `patch_id` | string | Patch folder / id |
| `labels` | array\<string\> | Multi-label list (19-class names) |
| `split` | string | `train` \| `validation` \| `test` (**official** — do not reshuffle) |
| `country` | string | Country from Natural Earth borders |
| `s1_name` | string | Paired S1 patch id |
| `s2v1_name` | string | Legacy v1 name |
| `contains_seasonal_snow` | bool | In recommended file: false |
| `contains_cloud_or_shadow` | bool | In recommended file: false |

## 7. The 19 classes (order used by common toolkits)

1. Urban fabric  
2. Industrial or commercial units  
3. Arable land  
4. Permanent crops  
5. Pastures  
6. Complex cultivation patterns  
7. Land principally occupied by agriculture, with significant areas of natural vegetation  
8. Agro-forestry areas  
9. Broad-leaved forest  
10. Coniferous forest  
11. Mixed forest  
12. Natural grassland and sparsely vegetated areas  
13. Moors, heathland and sclerophyllous vegetation  
14. Transitional woodland, shrub  
15. Beaches, dunes, sands  
16. Inland wetlands  
17. Coastal wetlands  
18. Inland waters  
19. Marine waters  

Class imbalance is severe (forests / arable / pastures dominate; beaches/wetlands rare). Report this in Step 3.

## 8. Official splits

- Column `split` is **fixed** by the dataset authors (spatial / quality aware).
- Using `randomSplit` = grading penalty.
- All classifier statistics (means, stds, thresholds, centroids, majority class) from **`train` only**.
- Validation: choose rule thresholds / feature subset.
- Test: report once.

## 9. Features required by the brief

For each band \(b \in \{B02,B03,B04,B08\}\), pixels \(x_i\) as float reflectance, \(N=120\times120\), \(\epsilon=10^{-6}\):

- \(\mu_b\), \(\sigma_b\)
- \(\mathrm{NDVI} = \frac{1}{N}\sum_i \frac{B08_i-B04_i}{B08_i+B04_i+\epsilon}\)
- \(\mathrm{NDWI} = \frac{1}{N}\sum_i \frac{B03_i-B08_i}{B03_i+B08_i+\epsilon}\)
- \(\mathrm{brightness} = \mathrm{mean}(\mu_{B02},\mu_{B03},\mu_{B04},\mu_{B08})\)

NDVI/NDWI must lie in **[-1, 1]** (up to float noise).

## 10. Small-files problem (why Step 1 exists)

Per patch ≈ 12 TIFFs × hundreds of thousands of patches → **millions of tiny files**. HDFS NameNode hates this.

**Fix:** pack selected bands into **Parquet** (one row / patch, binary columns), **partitioned by `country`**. Measure file count and bytes before vs after.

## 11. Drop-in layout for *this* repo

Put downloads here (either works; auto-detected):

```text
Lab2-Bigdata/
  data/
    incoming/                         # PREFERRED drop zone
      metadata.parquet
      BigEarthNet-S2/                 # extracted S2 tree
        <TILE>/<PATCH>/...
```

Also accepted:

- `data/metadata.parquet` + `data/BigEarthNet-S2/`
- `data/metadata.parquet` + `data/raw/<TILE>/...`
- Env `BEN_DATA_ROOT=/path/to/folder` containing the above

Pipeline writes (local mode) under:

```text
hdfs_mirror/bigearthnet/{raw,packed,metadata,features,results}/
```

On a real cluster, set `BEN_USE_LOCAL=0` and use `/bigearthnet/...` on HDFS.

## 12. Subset strategy (no full 59 GiB required)

1. Always keep `metadata.parquet` (small).  
2. Run `00_build_subset.py` → top-3 countries × **60 000** patches → `tiles.txt`.  
3. Extract / copy **only those tiles** into `data/incoming/...`.  
4. Scale: debug **500** patches → then ≥**50 000**.

## 13. Assignment pitfalls → code mitigations

| Pitfall | Mitigation in this repo |
|---|---|
| One band per TIFF | Read four files / patch |
| Mixed resolutions | Only B02/B03/B04/B08 |
| uint16 wrap | `astype(float32)/1e4` before indices |
| Driver loop | `mapPartitions` + MemoryFile |
| Corrupt TIFF | `error_flag`, skip crash |
| Leakage | Train-only stats helpers |
| “35 classes” OCR | Use **3–5** frequent classes (only 19 exist) |

## 14. Citations (report)

- BigEarthNet / reBEN papers and https://bigearth.net/  
- AI assistance: Cursor / Composer (cite as required by course)
