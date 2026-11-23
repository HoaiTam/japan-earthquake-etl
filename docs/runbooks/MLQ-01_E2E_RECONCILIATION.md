# MLQ-01 — Chuẩn bị đối soát E2E Gold đến ML report

[MLQ-01](../task/tasks/MLQ-01.md) đang chuẩn bị test plan, truy vấn và fixture.
Nghiệm thu official chờ [MLI-04 — Tạo Trino views và static report cho
experiment](../task/tasks/MLI-04.md) xác minh exact snapshots. Giữ nguyên
namespace, dữ liệu, và frozen rule của EXP-04/05 khi đối soát.

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
  trước khi đưa truy vấn SELECT vào Trino.

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

CI `docs-contracts` chạy fixture SQL qua `make ci-docs-base`. Fixture gồm
một trace thành công và các ca mất Gold event, lệch từng pin publication hoặc
lineage, run REJECTED, counts đúng nhưng hai lớp cùng ghi sai MAINSHOCK và
thêm một event khác mang role MAINSHOCK trong cùng window.
Fixture và local writer tests chỉ dùng làm evidence thành phần. Đóng
MLQ-01 sau E2E official, có các query/readback và static report thật. Diễn giải
kết quả là aftershock candidate hồi cứu.
