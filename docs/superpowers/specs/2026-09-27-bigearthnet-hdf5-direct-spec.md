# BigEarthNet partial HDF5-direct specification

## Decision

Use only four Hugging Face HDF5 shards on `D:\BigEarthNet-partial`: `train_p0`,
`train_p1`, `val_p0`, and `test_p0`. This is a bounded partial acquisition
(about 17 GB compressed), not a full BigEarthNet snapshot.

The main graded path reads the HDF5 arrays directly on Spark executors. It does
not materialise 52k patches as individual TIFF files. A tiny TIFF/packing pilot
may be retained only as evidence for the small-files discussion.

## Data contract

The HDF5 mirror contains `images` with shape `(rows, 12, 120, 120)` and labels
datasets. The accompanying CSV index maps `s2_folder` to the HDF5 file and row
offset. Official V2 metadata supplies `patch_id`, labels, country and split.
The manifest joins these two sources, keeps official splits, removes duplicates,
and records the selected shard and row offset.

Bands use the Sentinel-2 order B01, B02, B03, B04, B05, B06, B07, B08, B8A,
B09, B11, B12; features select B02/B03/B04/B08. A feature partition opens each
shard once, reads indexed arrays, computes means/stds, NDVI, NDWI and brightness,
and emits an explicit Spark schema with `error_flag` and `error_message`.

## Acceptance evidence

The report must show the four shard names/sizes, manifest row count (at least
50,000), at least three countries, official train/validation/test counts,
`n_in == n_out`, zero errors, NumPy checks, partition timing, ten SQL results,
three charts, and three-class rule/nearest-centroid metrics. It must state the
limitation that original-TIFF packing is demonstrated only by a small pilot.
