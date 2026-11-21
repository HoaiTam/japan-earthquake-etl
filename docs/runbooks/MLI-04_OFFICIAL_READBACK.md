# MLI-04 — Đối soát REPRODUCTION qua Trino

Kết quả pilot giữ nhãn DEVELOPMENT_ONLY. Luồng official ghim EXP-04 trên main,
hai cross-check và đúng sáu shard baseline replay cùng hai shard adaptive.
SQL readback official còn PENDING. Chạy trên máy giữ Iceberg catalog DAT-03,
registry staging/ml-artifacts với snapshot registry/candidate/mainshock,
và đúng tám bundle cùng receipt import được ghim theo EXP-02/EXP-03.
Research kit chỉ đọc có thể kiểm file và lineage, không cung cấp Iceberg
snapshot hoặc receipt CANDIDATE.

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
count vẫn bằng kỳ vọng.

## Chạy trên máy giữ catalog DAT-03

1. Nhận bundle đúng run ID và SHA từ owner. Xác minh layout theo shard. Dùng
   receipt CANDIDATE đã có hoặc import tuần tự với cấu hình và namespace QA
   đã duyệt theo [MLI-03](MLI-03_RESULT_IMPORT.md), bắt đầu WINDOW shard_00000.
   Không dùng full-split làm đầu vào import và không đổi adaptive ranks.
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
trên các snapshot. Chỉ đủ tám shard mới ghi VERIFIED toàn cohort. Metric
khoa học riêng giữ nguồn EXP-04 đã pin.

Kiểm hồi quy offline bằng make test-mli04. Nghiệm thu runtime cần evidence
thật và vẫn chờ phần adaptive overlap của [MLI-03](../task/tasks/MLI-03.md).
