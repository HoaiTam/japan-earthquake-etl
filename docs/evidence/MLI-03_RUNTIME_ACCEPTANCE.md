# MLI-03 — Runtime acceptance

Mốc UTC: **19/10/2026**, branch `feat/mli-03-runtime-acceptance`, từ main
`b92bf2b` (PR #89). Evidence máy đọc: [runtime JSON](./MLI-03-runtime.json).
Đây là component acceptance bằng **MLI-01 synthetic fixtures trên storage thật**,
được task cho phép; không phải official research dataset, không dùng DAT-02
pilot và không có scientific approval.

## Build và regression

- `make docker-build`: Java 17.0.20.1, Maven `clean verify` đạt **407 tests,
  0 failures/errors, 1 skipped**. Build `2dj4jw1lgc6vz8w6vvivshoob` hoàn tất,
  source/JAR fingerprints nằm trong JSON. Skip là probe notebook Python/PyArrow
  trong `ResultBundleContractTest` (notebook harness không nằm trong Docker build
  context); không phải skip publication gate.
- Bốn nhóm không cần Spark trên host (`TrinoMlVerifierTest`,
  `ResultBundleContractTest`, `MlResultImportJobTest`, `TrustedDatasetResolverTest`)
  đạt **21/21, không skip**, gồm cross-process fixture probe. Host dùng JDK 26
  compile release 17; kết quả này bổ sung, không thay build/runtime Java 17.
- `TMPDIR=/private/tmp python3 -m unittest discover -s airflow/tests -p 'test_*.py'`:
  **336/336**. Dùng `/private/tmp` để không bị `/var` symlink của macOS chặn
  bounded-path tests QA-03; không nới guard trong production.
- Docker lifecycle/Make regressions, CI contract suite và `make ci-docs-base`
  được chạy lại sau cập nhật docs; xem checklist task và bàn giao.

## Chuẩn bị tách khỏi smoke

`make up-ml WAIT_TIMEOUT=600` và `make up-airflow WAIT_TIMEOUT=600` đã cập nhật
runtime theo image mới; giữ nguyên named volumes. Init chỉ chạy khi config/image
thực sự thay đổi. Hai smoke sau chỉ preflight/run/exec, không build, pull, start,
reset, xóa namespace hoặc xóa warehouse.

## Storage và production runner

Lệnh `make smoke-mli-import-acceptance` đạt `PASSED`.

- QA run: `mli03-qa-ea096dbd384a40b599137dff9df6d86c`.
- Namespace: `iceberg.ml_qa_6f191301e7734cec8a29185c2cbc0c0d`.
- Input 2 candidate / 1 mainshock / 1 EXPORTED registry được Java seed trong
  namespace QA rồi pin/verify qua Trino. Đây là dữ liệu synthetic component,
  không giả rằng MLD-05 đã export official dataset.
- Result `CANDIDATE`: membership **2**, summary **1**, experiment **1**;
  experiment registry có hai snapshots reservation/publication nhưng chỉ một row.
- Exact snapshot IDs giữ nguyên qua rerun và qua DAG.test:

| Bảng | Snapshot đã pin | Rows |
|---|---:|---:|
| sequence_membership | 2416850644351807568 | 2 |
| sequence_summary | 8945933300571189287 | 1 |
| experiment_run | 8634617115857105431 | 1 |

| Case | Kết quả |
|---|---|
| Checksum / schema / duplicate / unknown ID / NaN | Reject trước result-table writes |
| BUILDING / DEVELOPMENT_PILOT | IMP_UNKNOWN_DATASET |
| Đổi tên pilot marker thành _SUCCESS.json | IMP_INCOMPLETE_BUNDLE |
| Cùng fingerprint, journal mới | Counts và toàn bộ result snapshots không đổi |
| Experiment ID cũ, fingerprint khác | IMP_EXPERIMENT_RUN_REUSED; không append |
| Mất outcome sau membership commit | Không CANDIDATE; retry giữ nguyên membership snapshot, không duplicate |
| Inject Trino quality blocker | Registry REJECTED, không serving CANDIDATE |
| Gold | Schema/snapshot state không đổi; importer không ghi Gold |

Failure injection chỉ nằm trong Java QA entrypoint, không có flag từ request
production. Namespace recovery/blocker có hậu tố `ab`/`cd`, tách khỏi publication
chính. Partial snapshots có thể tồn tại vật lý; serving bắt buộc qua CANDIDATE
registry + exact pins, không đọc raw result current làm publication.

Cuối smoke đã gọi lại **production Python → shell → Java → Trino** với cùng
bundle, không runner giả. `control_plane.release=null` vì outer QA wrapper giữ
whole-run lease; receipt cuối `lease_release=RELEASED` mới xác nhận release.

## Airflow graph thật, không clear/mark-success

Lệnh đã chạy:

```bash
make smoke-mli-import-dag MLI_IMPORT_REQUEST=/opt/pipeline/staging/mli-03-qa/mli03-qa-ea096dbd384a40b599137dff9df6d86c/import-request.json
```

DagRun `manual__2026-10-19T16:50:52.703206+00:00`, thời lượng khoảng **49.95s**,
đạt `success`, đủ **9/9 task success**. `publish_readback` thực hiện production
runner; `release_lease` trả đúng owner của DagRun; `completion_gate` trả COMPLETE.
Request operation ID khác DagRun lease owner; context SHA được kiểm trong từng
task. Guard từ chối logical date đã tồn tại/active run trước DAG.test; không clear
run cũ, mark-success, pause/unpause hoặc giả cron acceptance.

Trước khi chạy graph, preflight phát hiện Make command-line request làm thay đổi
hash shared Airflow/init config. Bản vá `unexport MLI_IMPORT_REQUEST` chỉ truyền
input từng run qua argv/`compose run -e`; không bỏ freshness guard và không rerun
init để chữa một thay đổi request. Regression thực thi Make bảo vệ hành vi này.

## Evidence giữ lại và giới hạn

- Shared staging: `/opt/pipeline/staging/mli-03-qa/mli03-qa-ea096dbd384a40b599137dff9df6d86c/`.
  Giữ `acceptance-report.json`, `storage-report.json`, registry, journals,
  import-request và `dag-acceptance-f6cfa7775a3942a88c11c8e1cb33b258.json`.
- Lần QA đầu `mli03-qa-f2ce85aee3b149f8b2ce5893e4149232` thất bại vì harness
  ghi trùng `blocked.json`; không publish CANDIDATE, local JVM đã exit và lease
  được release. Evidence/QA tables được giữ; đã sửa tên file riêng từng trạng
  thái và thêm regression. Không coi lần đó là PASSED.
- Private logs chỉ để chẩn đoán local, không commit/chia sẻ. Source/history audit
  phát hiện 7 pattern flags ngoài bản vá MLI-03 và 3 coverage items: cần triage
  trong [SEC-01 — Security review](../task/tasks/SEC-01.md), không tuyên bố audit Done.
- [MLD-05 — Validate feature snapshot/export](../task/tasks/MLD-05.md),
  [MLI-02 — Build dataset DAG](../task/tasks/MLI-02.md): official export/coverage
  acceptance còn riêng; không được nâng DAT-02 pilot thành EXPORTED.
- [EXP-04 — Scientific evaluation](../task/tasks/EXP-04.md),
  [MLI-04 — Trino/static report](../task/tasks/MLI-04.md),
  [MLQ-01 — ML E2E](../task/tasks/MLQ-01.md): scientific review, consumers và whole-flow
  acceptance chưa thuộc nghiệm thu component này. `scheduled_run=false`; không
  thay [QA-01 — Daily E2E](../task/tasks/QA-01.md).
- Reviewer độc lập chưa ghi nhận (`unassigned`); không tự tạo approval.
