# HDF5-direct execution plan

1. Download exactly the four selected HDF5 shards and three small index CSVs to
   `D:\BigEarthNet-partial`; retain checksums and free-space evidence.
2. Download official V2 metadata, join it to the CSV row index, and write a
   manifest containing shard paths and row offsets.
3. Decompress the four archives in place without deleting the compressed copies.
4. Run `scripts/hdf5_features.py`: Spark `mapPartitions` opens each HDF5 shard
   once per partition and writes the common feature schema.
5. Reuse the SQL and classification stages, adding HDF5 validation and charts
   to the report. Keep the existing small TIFF pilot for Step 1 evidence.
6. Run unit tests, Spark smoke tests, manifest checks and final acceptance
   assertions before claiming completion.
