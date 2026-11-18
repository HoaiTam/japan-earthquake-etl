# MLI-04 — Đối soát REPRODUCTION qua Trino

Kết quả pilot giữ nhãn DEVELOPMENT_ONLY. Luồng official ghim EXP-04 trên main,
hai cross-check và đúng sáu shard baseline replay cùng hai shard adaptive.
SQL readback hiện PENDING. Chưa thấy bundle đúng pin trong staging được kiểm
tra, chưa có receipt import cho cohort này và Trino local chưa chạy.

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
cho thuật toán đủ shard dùng trung bình theo số mẫu và một query distinct
trên các snapshot. Chỉ đủ tám shard mới ghi VERIFIED toàn cohort. Metric
khoa học riêng giữ nguồn EXP-04 đã pin.

Kiểm hồi quy offline bằng make test-mli04. Nghiệm thu runtime cần evidence
thật và vẫn chờ phần adaptive overlap của [MLI-03](../task/tasks/MLI-03.md#follow-up--import-adaptive-overlap-xuyên-shard-exp-03--pr-141).
