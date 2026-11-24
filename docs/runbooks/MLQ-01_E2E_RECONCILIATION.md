# MLQ-01 — Chuẩn bị đối soát E2E Gold đến ML report

[MLQ-01](../task/tasks/MLQ-01.md) đang chuẩn bị test plan, truy vấn và fixture.
[MLI-03 — Validate/import kết quả và commit bảng ML Iceberg](../task/tasks/MLI-03.md)
và [MLI-04 — Tạo Trino views và static report cho experiment](../task/tasks/MLI-04.md)
đã hoàn tất trên `main` (`VERIFIED` 8/8 shard `REPRODUCTION` tại exact snapshots);
phần nghiệm thu E2E chính thức của `MLQ-01` (`EXTENSION WINDOW`, fault bundle
không tạo `CANDIDATE`, đối chiếu rerun và receipt tổng hợp) vẫn giữ `In Progress`.
Giữ nguyên namespace, dữ liệu, và frozen rule của EXP-04/05 khi đối soát.

## Ghim đầu vào

| Phạm vi | Pin cần có |
|---|---|
| Gold Published | Publication ID, tên bảng physical, snapshot ID và evidence Trino |
| Dataset REPRODUCTION hoặc EXTENSION | Dataset ID, Gold pin, snapshot registry chứa đúng dòng EXPORTED, candidate snapshot, mainshock snapshot và SHA của trusted registry manifest |
| Artifact experiment | Algorithm, model/code version, run ID, shard ID, SHA marker, artifact checksums và JSON rule/config đã khóa. Với adaptive nhiều shard, lấy cùng cohort digest và đúng shard/run/marker pins |
| Import | Receipt CANDIDATE, SHA bytes receipt, per-bundle fingerprint, ba snapshot IDs của experiment/membership/summary và kết quả Trino |
| Report | Evidence MLI-04, SHA của evidence/report, query IDs và counts/metrics cùng dataset/run/snapshot |

## Một event xuyên tầng

Chọn một canonical event vai trò POST có rank 1 trong window thành công của run
đại diện. Dùng chính event ID và mainshock ID từ bundle đã kiểm checksum. Render
[SQL đối soát](../../sql/ml/mlq01_reconciliation.sql) bằng các pin trên, giữ file
đã render cùng query ID và kết quả trong evidence của lần chạy:

- `__GOLD_TABLE__` là physical Gold event table trong publication.
  `__DATASET_TABLE__` và `__CANDIDATE_TABLE__` lấy tên đầy đủ của
  `dataset_manifest` và `sequence_candidate_snapshot` trên catalog của dataset.
  `__RUN_TABLE__`, `__MEMBERSHIP_TABLE__`, `__SUMMARY_TABLE__` lấy lần lượt
  `experiment_run`, `sequence_membership`, `sequence_summary` trong namespace receipt.
- Thay mỗi `__*_SNAPSHOT__` bằng ID Iceberg xác thực, số nguyên dương signed
  64-bit. `__DATASET_SNAPSHOT__` là registry snapshot của dataset, còn
  `__RUN_SNAPSHOT__` lấy từ `experiment_snapshot_id` của receipt.
- `__MAINSHOCK_SNAPSHOT__` lấy từ trusted dataset manifest đã xác minh. SQL
  so nó dưới kiểu BIGINT ở dataset registry và chuỗi decimal ở experiment registry.
  `__GOLD_PUBLICATION_ID__` lấy từ evidence Gold Published.
  `__DATASET_MANIFEST_SHA256__` lấy SHA-256 của trusted registry manifest từ
  export receipt. Tên bảng dùng trong `__GOLD_TABLE__` cũng phải khớp
  `physical_gold_table_name` của dataset.
- Dùng chính xác các ID đã ghim cho `__DATASET_ID__`, `__RUN_ID__`,
  `__MAINSHOCK_ID__`, `__EVENT_ID__` và fingerprint 64 ký tự hex cho
  `__BUNDLE_SHA256__`. Chỉ nhận tên bảng catalog.schema.table an toàn. Thoát
  dấu nháy đơn trong giá trị SQL bằng hai dấu nháy đơn. Kiểm hết placeholder
  trước khi đưa truy vấn SELECT vào Trino. File SQL bỏ dấu `;` cuối câu để
  gửi trực tiếp tới Trino HTTP API (`/v1/statement`) mà không lỗi cú pháp.

Query phải trả một dòng `lineage_check=PASS`. Window `SUCCESS` phải có đúng
một dòng mang vai trò `MAINSHOCK` trong toàn window ở cả candidate lẫn membership,
và dòng đó phải là tự tham chiếu của mainshock (ở membership phải là sequence member).
Event được truy vết vẫn phải là POST
rank 1. Lưu kết quả kể cả khi FAIL. So
counts và role theo đúng grain một window, rồi đối chiếu số liệu tổng hợp toàn
run và snapshot IDs với evidence MLI-04. Dùng query ID và dòng kết quả để truy
ngược số liệu trên static report. Event có thể xuất hiện trong nhiều window,
vì vậy trace kèm mainshock ID thay vì đếm mọi membership của event thành một
window.

## Cấu trúc receipt MLQ-01 đồng bộ với DEMO-01

Để [DEMO-01](../task/tasks/DEMO-01.md) chuyển trạng thái `PASS_OFFICIAL_READY`,
receipt chính thức của `MLQ-01` (`task_id="MLQ-01"`, `status="PASSED"`,
`scientific_approval=false`) phải ghim `mli04_sha256_lf`, `dataset_id`,
`gold_snapshot_id`, năm cờ `checks=true` và kèm đầy đủ bằng chứng cho từng cổng
(nhúng trực tiếp hoặc qua object `{"path","sha256_lf"}`):

1. **Cấu trúc hai cấp cho `runs.<experiment_run_id>` (cho đủ 4 thuật toán đại diện
   `REPRODUCTION`: `WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`):**
   - **Cấp `runs.<experiment_run_id>` (bắt buộc để khớp gate MLI-04):** Bắt buộc
     chứa trực tiếp ba snapshot IDs (`experiment_snapshot_id`,
     `membership_snapshot_id`, `summary_snapshot_id`) khớp báo cáo
     [MLI-04](../evidence/MLI-04_OFFICIAL_READBACK_20261124.json) **cùng**
     trường `trace`. Nếu bỏ ba snapshot IDs ở cấp run (dù có trong `trace`),
     preflight `DEMO-01` sẽ từ chối với `DEMO_MLQ01_RUN_MISMATCH`.
   - **Cấp `runs.<experiment_run_id>.trace` (và `extension.trace`, nhúng trực tiếp
     hoặc `{"path","sha256_lf"}`):** Ghi `query_id` Trino riêng biệt, `dataset_id`,
     `experiment_run_id`, `gold_snapshot_id`, lặp lại ba snapshot IDs trên,
     `mainshock_event_id` != `candidate_event_id` (ánh xạ từ các cột `run_id`,
     `mainshock_id`, `event_id` của `sql/ml/mlq01_reconciliation.sql`) và
     `lineage_check="PASS"`.
2. **Ca `extension` (`EXTENSION` `WINDOW`):** Ghi `exp05_sha256_lf`, `dataset_id`,
   `collection_id`, `gold_snapshot_id`, `algorithm_name="WINDOW"`, `shard_id`,
   `experiment_run_id`, `success_marker_sha256`, `success_marker_path`,
   `import_receipt` (`{"path","sha256_lf"}`, trạng thái `CANDIDATE` với
   `namespace` là `iceberg.ml` hoặc `<catalog>.ml_qa_<8..64 hex>`) và `trace`
   (theo cùng schema mục 1).
3. **Reject bundle lỗi (`reject_verification`):** Ghi `namespace` bắt buộc theo
   ranh giới QA cô lập của [MLI-03](./MLI-03_RESULT_IMPORT.md)
   (`<catalog>.ml_qa_<8..64 hex>`, tuyệt đối không dùng `iceberg.gold*`,
   `iceberg.dat03_unified_20261114` hay `iceberg.ml` production) và danh sách
   `cases` hợp lệ với `case_name`, `experiment_run_id` (khác các run
   `CANDIDATE`), `expected_reason_code` == `actual_reason_code`
   (`IMP_*`/`EXP_*`), `experiment_status` thuộc `{"REJECTED", "NOT_CREATED"}`,
   `candidate_created=false`, `candidate_row_count=0` và `query_id`.
4. **Rerun không trùng (`rerun_verification`):** Ghi `dataset_id` và `namespace`
   khớp đúng `namespace` trong báo cáo `MLI-04` (nếu kiểm `REPRODUCTION`, ví dụ
   `iceberg.ml_qa_53cf813a988340fa9ebd45a3dfebbe7b`) hoặc `extension.import_receipt`
   (nếu kiểm `EXTENSION`), cùng `before_rerun` và `after_rerun` cho ba bảng
   `experiment_run`, `sequence_membership`, `sequence_summary` (tương thích trực
   tiếp [evidence rerun MLI-03](../evidence/MLI-03_ADAPTIVE_REPRODUCTION_20261124.json)
   của `#149` — có thể tái dùng trực tiếp bằng đúng đường dẫn và SHA-256 LF),
   có đủ **12 `query_id` Trino phân biệt** (3 bảng × 2 loại
   `snapshot_query_id`/`count_query_id` × 2 giai đoạn `before_rerun`/`after_rerun`),
   danh sách `snapshots` (chứa snapshot đã ghim) và `run_row_counts`
   (`experiment_run=1`) không đổi.

### Ví dụ JSON tối thiểu cho receipt `MLQ-01` (chỉ minh họa cấu trúc)

Ví dụ sau chỉ mô tả cấu trúc schema (không phải receipt official); mọi trường
`<...>` phải thay bằng `query_id`, event ID, SHA-256 và đường dẫn thật của đúng
run đã đối chiếu.

~~~json
{
  "task_id": "MLQ-01",
  "status": "PASSED",
  "scientific_approval": false,
  "mli04_sha256_lf": "<64-hex sha256 LF của MLI-04_OFFICIAL_READBACK_20261124.json>",
  "dataset_id": "ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87",
  "gold_snapshot_id": "7580315846938041321",
  "checks": {
    "gold_to_report_lineage": true,
    "count_parity": true,
    "reject_without_candidate": true,
    "rerun_no_duplicate": true,
    "extension_rule_preserved": true
  },
  "runs": {
    "exp_run_2e8155cfc8609ec0371529e5ec23aeaa": {
      "experiment_snapshot_id": 8711590798002857163,
      "membership_snapshot_id": 1589872702319916199,
      "summary_snapshot_id": 3504974519972302319,
      "trace": {
        "query_id": "<trino_query_id_trace_window_shard_00000>",
        "dataset_id": "ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87",
        "experiment_run_id": "exp_run_2e8155cfc8609ec0371529e5ec23aeaa",
        "gold_snapshot_id": "7580315846938041321",
        "experiment_snapshot_id": 8711590798002857163,
        "membership_snapshot_id": 1589872702319916199,
        "summary_snapshot_id": 3504974519972302319,
        "mainshock_event_id": "<mainshock_event_id>",
        "candidate_event_id": "<post_rank_1_event_id>",
        "lineage_check": "PASS"
      }
    }
  },
  "extension": {
    "exp05_sha256_lf": "<64-hex sha256 LF của EXP-05_EXTENSION_EVALUATION.json>",
    "dataset_id": "ds_5259d2fe13e027245559e1ec4a1e696b5d94d0b67f44d5ec80738c9e984978be",
    "collection_id": "col_0dc6dc2fd0a60c2890f68863b8561813ecd1d5e1941a8995be1d58e192fe0906",
    "gold_snapshot_id": "7580315846938041321",
    "algorithm_name": "WINDOW",
    "shard_id": "shard_00000",
    "experiment_run_id": "<exp_run_id_extension_window_shard_00000>",
    "success_marker_sha256": "<64-hex sha256 bytes của _SUCCESS.json>",
    "success_marker_path": "docs/evidence/<staging>/_SUCCESS.json",
    "import_receipt": { "path": "docs/evidence/<ext_import_receipt>.json", "sha256_lf": "<64-hex>" },
    "trace": { "path": "docs/evidence/<ext_trace>.json", "sha256_lf": "<64-hex>" }
  },
  "reject_verification": {
    "namespace": "iceberg.ml_qa_53cf813a988340fa9ebd45a3dfebbe7b",
    "cases": [
      {
        "case_name": "checksum",
        "experiment_run_id": "<exp_qa_reject_run_id>",
        "expected_reason_code": "IMP_CHECKSUM_MISMATCH",
        "actual_reason_code": "IMP_CHECKSUM_MISMATCH",
        "experiment_status": "REJECTED",
        "candidate_created": false,
        "candidate_row_count": 0,
        "query_id": "<trino_query_id_reject_checksum>"
      }
    ]
  },
  "rerun_verification": {
    "path": "docs/evidence/MLI-03_ADAPTIVE_REPRODUCTION_20261124.json",
    "sha256_lf": "<64-hex sha256 LF của file rerun>"
  }
}
~~~

Ghi chú: mẫu trên chỉ mô tả cấu trúc schema (chưa phải receipt official) và chỉ minh họa một phần tử trong `runs`; receipt thật phải chứa đủ 4 thuật toán đại diện (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`) dưới `runs` với `query_id` thật của từng run không trùng giữa 5 lần `trace`. Có thể nhúng trực tiếp hoặc dùng cặp `{"path","sha256_lf"}` cho từng `trace`, `reject_verification` và `rerun_verification`.

## Test matrix

Tái dùng [result bundle fixtures](../../spark/src/test/resources/ml/results)
và local Spark/Iceberg của MLI-03. Với runtime chạy lỗi trong QA namespace đã
duyệt, lưu receipt lỗi, snapshot và count trước/sau. Giữ dữ liệu official gốc.

| Ca | Fixture hoặc cách chạy | Kỳ vọng |
|---|---|---|
| Checksum sai | `checksum` | `IMP_CHECKSUM_MISMATCH`, không CANDIDATE mới |
| Candidate lạ | `unknown` | `IMP_UNKNOWN_EVENT_ID`, không CANDIDATE mới |
| Trùng membership grain | `duplicate` | `IMP_DUPLICATE_GRAIN`, không CANDIDATE mới |
| Bundle thiếu file hoặc marker thiếu field | Copy fixture `success` vào thư mục riêng, bỏ file bắt buộc hoặc trường bắt buộc trong marker của bản copy | `IMP_ARTIFACT_PATH_INVALID` hoặc `IMP_INCOMPLETE_BUNDLE`, không CANDIDATE mới |
| Mainshock bị noise | `hdb-noise` | Window giữ `FAILED` và reason `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, không sinh productive sequence |
| Reuse run ID | Cùng run, thay bytes của artifact hợp lệ và cập nhật marker trên bản copy | `IMP_EXPERIMENT_RUN_REUSED`, số dòng và snapshot không đổi |
| Rerun đúng | Cùng run/fingerprint/pins, kiểm exact snapshot trước và sau | Một dòng registry, counts và snapshot IDs không đổi, independent readback vẫn đạt |
| Extension | So sánh frozen rule/config pins EXP-04 với EXP-05 | Giữ cùng rule; ghi nhận lệch count và failure reasons, không tự điều chỉnh model |

Kiểm offline trên fixture với hai lệnh:

~~~bash
python -m unittest tests.test_mlq01_reconciliation
make test-mli-import
~~~

CI `docs-contracts` chạy fixture SQL qua `make ci-docs-base`. Fixture gồm 9 ca:
một trace thành công, không chứa dấu chấm phẩy `;` (tương thích Trino HTTP API
`/v1/statement`), kiểm khớp trường ánh xạ `trace` cho receipt `DEMO-01`, các ca
mất Gold event, lệch từng pin publication hoặc lineage, run REJECTED, counts đúng
nhưng hai lớp cùng ghi sai MAINSHOCK và thêm một event khác mang role MAINSHOCK
trong cùng window.
Fixture và local writer tests chỉ dùng làm evidence thành phần. Đóng
MLQ-01 sau E2E official, có các query/readback và static report thật. Diễn giải
kết quả là aftershock candidate hồi cứu.
