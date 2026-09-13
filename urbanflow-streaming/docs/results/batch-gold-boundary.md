# Batch Historical Gold Boundary Result

Date: 2026-09-13T15:10:56Z
Status: `BLOCKED_MISSING_ARTIFACT`
Path: `s3a://urbanflow/gold/historical_zone_baseline/`
Batch source commit: `749ea2e5420b538d45280f86b48569a576ac1ad9`

## Summary

- Reason: No Parquet files were found at the contract path.
- Row count: `None`
- Unique key count: `None`
- Duplicate key count: `None`

## Schema

_No schema loaded._

## Sample Join

```json
null
```

## Notes

- This verifier reads the canonical S3A path directly.
- It does not import Batch application source code.
- It validates Streaming can consume Historical Gold without an adapter.
