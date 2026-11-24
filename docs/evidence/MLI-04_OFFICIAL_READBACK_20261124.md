# MLI-04 — REPRODUCTION snapshot readback

- SQL parity: **VERIFIED** (8/8 shards)
- Dataset: ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87
- Collection: col_bd10c69365da841b2cf6a62172e2350a797001eac04ce4bc6f2277a4d150ad98
- EXP-04 LF SHA-256: 0946298b33aa871273071ea2b64e9490a4a63bd639f515a6d0fdf9d9a7fb12b6
- Gold snapshot: 7580315846938041321
- Scientific approval: false. Candidate sequences are retrospective; no prediction or causal claim.

## DBSCAN / shard_00000

- Run: exp_run_0d55ad02cb2d5a86eee4feb6305113fa (CANDIDATE)
- Config / code: dbscan-global-dev-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:34:59.472337714Z
- Snapshots: {"experiment_snapshot_id": 4419274765171891155, "membership_snapshot_id": 6377617683236073483, "summary_snapshot_id": 2287588604720905262}
- Query IDs: 20261118_005024_00059_v3i3r, 20261118_005024_00060_v3i3r, 20261118_005026_00061_v3i3r, 20261118_005026_00062_v3i3r
- Counts: {"ambiguous_post": 11072, "candidate_rows": 85972, "failure_windows": 22, "membership_rows": 85972, "rank_one_post": 53442, "raw_post": 64514, "resolved_unique": 53442, "skipped_windows": 0, "success_windows": 65, "summary_rows": 87}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 65, "mean": 0.2120923094109124}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: [["FAILED", "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", 22]]

## DBSCAN / shard_00001

- Run: exp_run_8bb89b552fbec0aa303f2ac9fa2d5458 (CANDIDATE)
- Config / code: dbscan-global-dev-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:35:41.641639636Z
- Snapshots: {"experiment_snapshot_id": 5764799399263431987, "membership_snapshot_id": 8936318433001920133, "summary_snapshot_id": 670123567204101459}
- Query IDs: 20261118_005027_00063_v3i3r, 20261118_005027_00064_v3i3r, 20261118_005028_00065_v3i3r, 20261118_005028_00066_v3i3r
- Counts: {"ambiguous_post": 7181, "candidate_rows": 70738, "failure_windows": 44, "membership_rows": 70738, "rank_one_post": 46792, "raw_post": 53973, "resolved_unique": 46792, "skipped_windows": 0, "success_windows": 63, "summary_rows": 107}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 63, "mean": 0.200389726272473}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: [["FAILED", "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", 44]]

## HDBSCAN_GLOBAL / shard_00000

- Run: exp_run_6dcc5de182692e8e07a1f0b92d46201c (CANDIDATE)
- Config / code: hdbscan-global-dev-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:36:30.839743839Z
- Snapshots: {"experiment_snapshot_id": 3994907215121136351, "membership_snapshot_id": 8687383638161468453, "summary_snapshot_id": 8575971062893547241}
- Query IDs: 20261118_005028_00067_v3i3r, 20261118_005028_00068_v3i3r, 20261118_005028_00069_v3i3r, 20261118_005029_00070_v3i3r
- Counts: {"ambiguous_post": 2227, "candidate_rows": 85972, "failure_windows": 20, "membership_rows": 85972, "rank_one_post": 5172, "raw_post": 7399, "resolved_unique": 5172, "skipped_windows": 0, "success_windows": 67, "summary_rows": 87}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 67, "mean": 0.16210411182005513}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: [["FAILED", "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", 20]]

## HDBSCAN_GLOBAL / shard_00001

- Run: exp_run_5a669d4f40a13d3d8f253ccf00904751 (CANDIDATE)
- Config / code: hdbscan-global-dev-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:37:12.050507886Z
- Snapshots: {"experiment_snapshot_id": 8601303551576257765, "membership_snapshot_id": 1269808824289107226, "summary_snapshot_id": 7938350238626489626}
- Query IDs: 20261118_005029_00071_v3i3r, 20261118_005029_00072_v3i3r, 20261118_005029_00073_v3i3r, 20261118_005029_00074_v3i3r
- Counts: {"ambiguous_post": 1713, "candidate_rows": 70738, "failure_windows": 32, "membership_rows": 70738, "rank_one_post": 6006, "raw_post": 7719, "resolved_unique": 6006, "skipped_windows": 0, "success_windows": 75, "summary_rows": 107}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 75, "mean": 0.15724490933234742}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: [["FAILED", "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", 32]]

## WINDOW / shard_00000

- Run: exp_run_2e8155cfc8609ec0371529e5ec23aeaa (CANDIDATE)
- Config / code: window-baseline-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:33:35.616140050Z
- Snapshots: {"experiment_snapshot_id": 8711590798002857163, "membership_snapshot_id": 1589872702319916199, "summary_snapshot_id": 3504974519972302319}
- Query IDs: 20261118_005030_00075_v3i3r, 20261118_005030_00076_v3i3r, 20261118_005030_00077_v3i3r, 20261118_005030_00078_v3i3r
- Counts: {"ambiguous_post": 11064, "candidate_rows": 85972, "failure_windows": 0, "membership_rows": 85972, "rank_one_post": 60075, "raw_post": 71139, "resolved_unique": 60075, "skipped_windows": 0, "success_windows": 87, "summary_rows": 87}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 0, "mean": null}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: []

## WINDOW / shard_00001

- Run: exp_run_7a6591045568ec8b2460a7360c3f62d9 (CANDIDATE)
- Config / code: window-baseline-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:34:17.457398875Z
- Snapshots: {"experiment_snapshot_id": 7764117898858788810, "membership_snapshot_id": 1470889297533565647, "summary_snapshot_id": 120858620898474578}
- Query IDs: 20261118_005030_00079_v3i3r, 20261118_005031_00080_v3i3r, 20261118_005031_00081_v3i3r, 20261118_005031_00082_v3i3r
- Counts: {"ambiguous_post": 6668, "candidate_rows": 70738, "failure_windows": 0, "membership_rows": 70738, "rank_one_post": 58046, "raw_post": 64714, "resolved_unique": 58046, "skipped_windows": 0, "success_windows": 107, "summary_rows": 107}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 0, "mean": null}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: []

## HDBSCAN_ADAPTIVE / shard_00000

- Run: exp_run_37d6d2121034d1e2f2585dd17261828c (CANDIDATE)
- Config / code: hdbscan-adaptive-dev-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:38:15.287509138Z
- Snapshots: {"experiment_snapshot_id": 3515751900453226493, "membership_snapshot_id": 1565781986528835522, "summary_snapshot_id": 7572536492473112337}
- Query IDs: 20261118_005031_00083_v3i3r, 20261118_005031_00084_v3i3r, 20261118_005032_00085_v3i3r, 20261118_005032_00086_v3i3r
- Counts: {"ambiguous_post": 0, "candidate_rows": 85972, "failure_windows": 26, "membership_rows": 85972, "rank_one_post": 45155, "raw_post": 53343, "resolved_unique": 45155, "skipped_windows": 0, "success_windows": 61, "summary_rows": 87}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 61, "mean": 0.3204171647142814}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: [["FAILED", "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", 26]]

## HDBSCAN_ADAPTIVE / shard_00001

- Run: exp_run_3082725c2877a2b8a6fb08d9dd4e30ed (CANDIDATE)
- Config / code: hdbscan-adaptive-dev-v1 / c746dc6dc2db0f40cf24c3561ca3ff1c4b79a60d
- Publication time: 2026-11-23T17:39:12.886141762Z
- Snapshots: {"experiment_snapshot_id": 1774552306020177685, "membership_snapshot_id": 4749416527188404194, "summary_snapshot_id": 483818915357212368}
- Query IDs: 20261118_005032_00087_v3i3r, 20261118_005032_00088_v3i3r, 20261118_005032_00089_v3i3r, 20261118_005032_00090_v3i3r
- Counts: {"ambiguous_post": 0, "candidate_rows": 70738, "failure_windows": 38, "membership_rows": 70738, "rank_one_post": 42923, "raw_post": 45724, "resolved_unique": 42923, "skipped_windows": 0, "success_windows": 69, "summary_rows": 107}
- SQL means and contributing windows: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 69, "mean": 0.30290121869491254}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- Failure reasons: [["FAILED", "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", 38]]

## Full algorithm coverage

- **DBSCAN** · Query 20261118_005033_00092_v3i3r
  - Counts: {"ambiguous_post": 18253, "candidate_rows": 156710, "failure_windows": 66, "membership_rows": 156710, "rank_one_post": 100234, "raw_post": 118487, "resolved_unique": 92310, "skipped_windows": 0, "success_windows": 128, "summary_rows": 194}
  - SQL metrics: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 128, "mean": 0.20633244427246178}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- **HDBSCAN_ADAPTIVE** · Query 20261118_005034_00094_v3i3r
  - Counts: {"ambiguous_post": 0, "candidate_rows": 156710, "failure_windows": 64, "membership_rows": 156710, "rank_one_post": 88078, "raw_post": 99067, "resolved_unique": 88078, "skipped_windows": 0, "success_windows": 130, "summary_rows": 194}
  - SQL metrics: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 130, "mean": 0.3111202395193856}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
  - EXP-03 deduplicated aftershock assignments: 10989
- **HDBSCAN_GLOBAL** · Query 20261118_005034_00093_v3i3r
  - Counts: {"ambiguous_post": 3940, "candidate_rows": 156710, "failure_windows": 52, "membership_rows": 156710, "rank_one_post": 11178, "raw_post": 15118, "resolved_unique": 10851, "skipped_windows": 0, "success_windows": 142, "summary_rows": 194}
  - SQL metrics: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 142, "mean": 0.15953763163288556}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}
- **WINDOW** · Query 20261118_005032_00091_v3i3r
  - Counts: {"ambiguous_post": 17732, "candidate_rows": 156710, "failure_windows": 0, "membership_rows": 156710, "rank_one_post": 118121, "raw_post": 135853, "resolved_unique": 109723, "skipped_windows": 0, "success_windows": 194, "summary_rows": 194}
  - SQL metrics: {"dbcv_score": {"count": 0, "mean": null}, "noise_ratio": {"count": 0, "mean": null}, "omori_fit_metric": {"count": 0, "mean": null}, "omori_p": {"count": 0, "mean": null}}

## Scientific evaluation

DBCV, Omori and stability from the scientific evaluator are reported separately in [EXP-04](./EXP-04_OFFICIAL_REPRODUCTION.md). SQL values above come from persisted shard summaries. Per-shard resolved counts are shown separately; distinct counts cannot be summed across shards.
