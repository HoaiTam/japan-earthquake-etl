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
- [PR #65](https://github.com/HoaiTam/japan-earthquake-etl/pull/65) cập nhật từ
  main sau khi GLD-04 [#63](https://github.com/HoaiTam/japan-earthquake-etl/pull/63)
  đã merge; base được chuyển về `main`.

## Đối chiếu PR #65 sau khi dependency merge

- Baseline main: `4e31f01e6d512702f1b0e863a7f2ae60045e152a`.
- Toàn bộ 15 files của implementation/evidence MLD-01 từ #65 đã nằm trên main
  qua squash merge [#60](https://github.com/HoaiTam/japan-earthquake-etl/pull/60).
  Không áp lại code hoặc khôi phục phiên bản MLD-02/03 cũ từ nhánh stacked.
- Ba conflict ở evidence/task GLD-04 và task index giữ đúng bản main mới.
  Diff so với main chỉ còn cập nhật docs MLD-01 về base/handoff/checks.
- Java 17.0.12 trên Windows, Maven `-pl spark -am -Dtest=DatasetManifestTest
  -Dsurefire.failIfNoSpecifiedTests=false verify`: **7 tests PASS**, 0 failures/
  errors/skips, **BUILD SUCCESS**; log local ignored tại
  `staging/mld01-main-update-tests.log`, kết thúc 2026-10-11 18:01:29 +07:00.
- Task status (73 files), CON-03 model, diff và secret-pattern check: PASS.
  Reviewer vẫn unassigned; CI cập nhật theo head PR #65, không dùng CI main cũ
  làm bằng chứng của head mới.
- Control-plane test local chưa chạy lại: Docker Linux daemon không sẵn sàng.
  Lệnh Compose unit test thất bại trước khi tạo container; không coi là test PASS.
  Bộ CI Airflow có `test_dataset_manifest_qa.py` và là gate của head mới.
- Integration Trino/Iceberg/MinIO ở mục trên là evidence lượt nghiệm thu trước;
  không tuyên bố đã chạy lại integration ở lượt chỉ đồng bộ branch/docs.
