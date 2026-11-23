# MLI-04 — Đối soát REPRODUCTION qua Trino

Kết quả pilot giữ nhãn DEVELOPMENT_ONLY. Luồng official ghim EXP-04 trên main,
hai cross-check và đúng sáu shard baseline replay cùng hai shard adaptive.
Chạy trên máy giữ Iceberg catalog DAT-03,
registry staging/ml-artifacts với snapshot registry/candidate/mainshock,
và đúng tám bundle cùng receipt import được ghim theo EXP-02/EXP-03.
Research kit chỉ đọc có thể kiểm file và lineage, không cung cấp Iceberg
snapshot hoặc receipt CANDIDATE.

## Kết quả nghiệm thu 24/11/2026

Readback **VERIFIED**: tám receipt `CANDIDATE`, 24 snapshot pins kết quả
(ba pin mỗi run) và **36/36 truy vấn Trino** (32 theo run, 4 gộp theo thuật toán).
[Báo cáo tĩnh](../evidence/MLI-04_OFFICIAL_READBACK_20261124.md) được render từ
[evidence JSON](../evidence/MLI-04_OFFICIAL_READBACK_20261124.json), SHA-256
`d2d1ed3082cbcdc2b1c835a7ad315ca7d85f608d0fc32dab0aaee97efe6ef175`.

Import các bundle chính thức vào **namespace QA riêng** qua control plane
MLI-03 và Spark local, không phải scheduled DagRun. Tất cả phiên import thành
công trả đúng lease; registry, dataset và Gold gốc không bị ghi đè. Runner
được build từ [PR #149](https://github.com/HoaiTam/japan-earthquake-etl/pull/149),
commit `a377a736022c9d4760e8f3afcaad6df734e8e986`, JAR SHA-256
`1796be2e193e4a96de3b97eaa270bbce0d0d892800c2b7005331ffa05b50e2da`.
Evidence chỉ chứng nhận snapshot đã đọc trong phiên QA; không chứng nhận
scientific approval hoặc publication trên namespace production.

## Cohort được ghim

| Thuật toán | Shard | Experiment run ID |
|---|---|---|
| WINDOW | shard_00000 | exp_run_2e8155cfc8609ec0371529e5ec23aeaa |
| WINDOW | shard_00001 | exp_run_7a6591045568ec8b2460a7360c3f62d9 |
| DBSCAN | shard_00000 | exp_run_0d55ad02cb2d5a86eee4feb6305113fa |
| DBSCAN | shard_00001 | exp_run_8bb89b552fbec0aa303f2ac9fa2d5458 |
| HDBSCAN_GLOBAL | shard_00000 | exp_run_6dcc5de182692e8e07a1f0b92d46201c |
| HDBSCAN_GLOBAL | shard_00001 | exp_run_5a669d4f40a13d3d8f253ccf00904751 |
| HDBSCAN_ADAPTIVE | shard_00000 | exp_run_37d6d2121034d1e2f2585dd17261828c |
| HDBSCAN_ADAPTIVE | shard_00001 | exp_run_3082725c2877a2b8a6fb08d9dd4e30ed |

Verifier đối soát cohort adaptive với EXP-03 sau khi đủ hai shard:
99.067 lượt gán aftershock, 88.078 aftershock duy nhất, loại trùng 10.989 lượt.
Trên đúng snapshot membership ghim trong các receipt, đối chiếu với Parquet
đã kiểm SHA cả số dòng POST rank 1, số event POST rank 1 phân biệt và số dòng
POST `AMBIGUOUS`. EXP-03 ghim 88.078 dòng rank 1, 88.078 event duy nhất và
0 dòng POST `AMBIGUOUS`. Hai rank 1 cho cùng event bị từ chối dù distinct
count vẫn bằng kỳ vọng. Hai receipt adaptive phải mang cùng digest
`adaptive_cohort.sha256` và đúng danh sách shard/run/SHA marker EXP-03. Verifier
tính lại digest từ evidence đã ghim. Receipt adaptive đời cũ thiếu digest
hoặc receipt lấy từ lần chạy khác sẽ không qua bước đối soát.

## Chạy trên máy giữ catalog DAT-03

1. Nhận bundle đúng run ID và SHA từ owner. Xác minh layout theo shard. Dùng
   receipt CANDIDATE đã có hoặc import tuần tự với cấu hình và namespace QA
   đã duyệt theo [MLI-03](MLI-03_RESULT_IMPORT.md), bắt đầu WINDOW shard_00000.
   Không dùng full-split làm đầu vào import và không đổi adaptive ranks.
   Khi chuyển từ macOS vào Docker, chỉ chép `_SUCCESS.json` và đúng các tệp
   trong `checksums` của marker; không đưa AppleDouble (`._*`) vào bundle.
   Kiểm lại danh sách tệp, SHA marker và SHA từng artifact tại đích trước khi
   acquire lease. Nếu import từ chối metadata, giữ journal lần lỗi; không xóa
   artifact hay ghi đè trạng thái `REJECTED` để chạy lại.
2. Lưu request JSON ngoài Git với **schema_version** mli04-request-v1.
   Trường **namespace** là namespace ghi trong
   receipt MLI-03. **runs** là danh sách object có **experiment_run_id**,
   **artifact_root** (thư mục WHOLE_WINDOW_SHARD), **receipt** (file import-report.json)
   và **receipt_sha256** (SHA của bytes receipt). Cung cấp SHA của request qua
   kênh tin cậy và cấu hình endpoint QA được phép trong MLI04_TRINO_URL.
3. Chạy readback và render bằng SHA của evidence do operator xác nhận.

~~~bash
python3 scripts/verify-mli04-parity.py --request "$REQUEST" --request-sha256 "$REQUEST_SHA" --output "$EVIDENCE"
python3 scripts/generate-experiment-report.py --mode official --input-json "$EVIDENCE" --evidence-sha256 "$EVIDENCE_SHA" --output-md "$REPORT"
~~~

Verifier chỉ gửi SELECT với ba snapshot của từng run. Đối chiếu receipt,
registry, bytes Parquet, counts, failure reasons và số mẫu của metric. Rollup
cho thuật toán đủ shard dùng trung bình theo số mẫu và một query đếm membership
trên các snapshot. Chỉ đủ tám shard mới ghi VERIFIED toàn cohort. Khi render,
trình tạo báo cáo cũng đối chiếu `adaptive_cohort_sha256` của từng dòng adaptive
với digest tính từ EXP-03. Evidence cũ thiếu digest cần được tạo lại qua
verifier trước khi render. Metric
khoa học riêng giữ nguồn EXP-04 đã pin.

Kiểm hồi quy offline bằng `make test-mli04`. Trước khi merge MLI-04, chốt
[MLI-03 — Validate/import kết quả và commit bảng ML Iceberg](../task/tasks/MLI-03.md)
phần adaptive overlap của PR #149. Nếu runner merge thay đổi hành vi import hoặc
contract cohort so với pin trên, chạy lại acceptance trên phiên bản mới.
