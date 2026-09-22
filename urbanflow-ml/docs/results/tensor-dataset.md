# Tensor Dataset Evidence

Status: `TENSOR-GO = PASS` for the current Silver snapshot.

Run ID: `gtqn-20260917-001`

Artifact:

```text
s3a://urbanflow/dev/ml/runs/gtqn-20260917-001/tensor/artifact.npz
```

Measured output:

```text
artifact size: 1.2 MiB
counts shape: [10176, 265]
calendar shape: [10176, 7]
fixed adjacency shape: [265, 265]
train indices: [336, 7344)
validation indices: [7344, 8832)
test indices: [8832, 10176)
unobserved buckets: 2935
dataset fingerprint: 44f66dfa517b2bdfabf2919776408792f68db3be57758980ac8e82b0de767ad0
```

Verification read the artifact back from S3A, recalculated the fingerprint,
checked finite non-negative counts, checked the observed-mask shape, checked
positive scaler values, and checked that every graph row sums to one.

The `2935` unobserved buckets expose the incomplete October and December 2022
source. This run is suitable for engineering proof and a current-data result,
but not for a publication-ready quality claim.
