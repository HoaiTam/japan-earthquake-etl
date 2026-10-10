# Synthetic result bundles for MLI-01

Each directory has the complete required layout and tiny real Parquet files.
No Drive, dataset dump, model or credential is included. Dataset ds_fixture and
exp_fixture are opaque test IDs. Mainshock main + POST candidate post are the
expected pinned lineage supplied by ResultBundleContractTest.

| Directory | Expected |
|---|---|
| success | WINDOW RESULT_READY only |
| checksum | IMP_CHECKSUM_MISMATCH |
| schema | IMP_SCHEMA_MISMATCH (row schema_version 2.0) |
| duplicate | IMP_DUPLICATE_GRAIN |
| unknown | IMP_UNKNOWN_EVENT_ID |
| hdb-success | HDBSCAN_GLOBAL RESULT_READY only |
| hdb-noise | HDBSCAN mainshock noise, failed-window summary with explicit reason |

Invalid field/count/probability, physical-schema assertion, run reuse and lifecycle
are also tested as mutations. Fixture generation uses the Java Parquet API in
ResultBundleContractTest. To regenerate intentionally, add -DgenerateMlFixtures=true
to the Maven test command in ML_RESULT_BUNDLE_CONTRACT.md; inspect changed checksums.
Normal tests only read the committed fixtures. Millisecond UTC and Parquet null
types are exact. These bundles do not claim scientific/model success.
