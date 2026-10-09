# MLD-01 — Dataset identity preparation

Status: In Progress, metadata fixtures only. Owner Trang; reviewer unassigned.

`DatasetIdentityPlan` implements CON-03 canonical JSON (recursive object key
sorting, preserving array order), SHA-256 identity, exact half-open reproduction
and extension periods, BUILDING manifest fixtures and rerun conflict detection.
The minimum identity fields follow `ML_DATA_MODEL.md` §3.1. Cutoff is also hashed
because it changes source eligibility; this is an additive identity rule for new
datasets. Existing datasets must keep their stored identity/version.

`SnapshotFixture` represents mock publication metadata, never a real publication
registry. `canMaterializePublishedDataset()` is always false. This preparation
does not read current tables, write Iceberg or advance VALIDATED/EXPORTED.
Canonical filter strings reject non-finite numbers and credential-bearing URI /
personal account paths. Runtime resolver must additionally validate complete
filter semantics and secret key policy before materialization.

## Integration plan / hard dependency

1. [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md): obtain verified
   Published publication record and exact positive physical snapshot ID.
2. Resolve the logical `gold.earthquake_event_current` view to physical
   `gold.event_current` plus natural/study-area predicate. Views have no independent
   Iceberg snapshot. Keep this mapping in resolver provenance; do not silently
   assign the latest physical snapshot to an existing dataset.
3. Implement manifest schema/materialization in `ml.dataset_manifest`, exact
   publication lookup, immutable identity/config comparison and idempotent insert.
4. Test real reproduction/extension manifests against pinned time travel,
   publication revocation, changed cutoff/config and retry. Pilot coverage does
   not establish research-period completeness.
5. [MLD-02](../task/tasks/MLD-02.md), [MLD-03](../task/tasks/MLD-03.md),
   [MLD-04](../task/tasks/MLD-04.md), [MLD-05](../task/tasks/MLD-05.md): audit/Mc,
   candidates/features and export gates must finish before lifecycle advances.

## Checks

PowerShell with JAVA_HOME set to JDK 17; run through Git Bash wrapper:

    & 'C:\Program Files\Git\bin\bash.exe' -lc './mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=DatasetIdentityPlanTest -Dsurefire.failIfNoSpecifiedTests=false test'

Five deterministic unit tests; no Docker, network dataset or lake writes.
