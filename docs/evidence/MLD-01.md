# MLD-01 — Runtime evidence

Ngày kiểm chứng: 2026-10-11. Branch: `feat/mld-01-published-dataset-manifest`.
Code version: `56b0cf464c7492f27eb1e75e0bd9b43a31b77dcd`.

## Checks đã chạy

- Maven Java17 `-pl spark -am -Dtest=DatasetManifestTest
  -Dsurefire.failIfNoSpecifiedTests=false verify`: 7 tests, 0 errors/failures,
  BUILD SUCCESS; đóng gói runner JAR.
- Linux QA container unittest `test_dataset_manifest_qa.py`: 3 tests đạt;
  exact reference, success releases own lease, timeout retains lease,
  production namespace bị reject.
- `check-task-status.sh`: 73 tasks metadata/tracking/index đồng bộ.
- `check-data-model-contract.sh`: CON-03 model check đạt.
- Compose config validation và `git diff --check` đạt.

## Integration thật

QA service `dataset-manifest-qa` chạy Java SQL qua Trino 483 và Iceberg/MinIO
hiện có. Không dùng mock SQL cho evidence này. Public readback trong
[MLD-01-runtime.json](./MLD-01-runtime.json).

- Publication: `pub_d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f`.
- Physical Gold snapshot: `1901025108914282099`.
- Metadata table: `iceberg.ml_qa_2d155a4ff190443bb76e2e186bb39e7e.dataset_manifest`.
- Reproduction dataset: `ds_494b1d7240d12b9e58ff34502b937a71a9689c3124edacd7f06927b9b5e2b828`.
- Extension dataset: `ds_cdcaa140bea61ef8f5b3f985db1b8773e7abee0136f365ff7e61a7fd5019b46a`.
- Mỗi split build rồi rerun; exact SQL readback cùng identity/clock/run,
  `rerun_unchanged=true`, status `BUILDING`.
- Unit tests chứng minh changing Gold revision/config/cutoff tạo identity mới;
  unpublished/ambiguous/expired snapshot hoặc wrong period không ghi manifest;
  downstream SQL time travel physical table bằng literal snapshot.

Config QA `config/ml/dataset_identity_v1.json` pin commit implementation thật.
Runbook/recovery nằm tại [Dataset runtime](../specs/DATASET_MANIFEST_RUNTIME.md).

## Giới hạn / bước tiếp theo

- Published pilot có 16 USGS events (GLD-03/04 evidence), chưa có primary-JMA
  eligible coverage. Manifest BUILDING chứng minh identity task, chưa chứng minh
  Mc đại diện toàn reproduction/extension.
- **MLD-02 — Audit Gold và xác định magnitude of completeness**: cần audit
  đúng manifest, source coverage JMA và Mc có evidence.
- **MLD-03 — Chọn mainshock và tạo candidate windows**: cần pinned audit thật
  từ MLD-02 trước khi nghiệm thu integration.
- **MLD-04 — Tạo feature tương đối 4-D** và **MLD-05 — Export dataset bundle**:
  cần feature gates/export checksum để nâng lifecycle VALIDATED/EXPORTED.
- Reviewer độc lập P0: `unassigned`; không có approval được tự điền.
- PR stack trên GLD-04; chưa merge GitHub theo yêu cầu người dùng.
