# MLI-02 — Build dataset bằng DAG thật

## Phần này làm gì và dùng để làm gì?

`mli_02_build_dataset` là DAG manual, độc lập daily ETL, tạo bundle ML có
lineage từ Gold đã Published. DAG có 12 task: resolve context, 10 phase dưới
đây và contract gate cuối. Python chỉ điều phối metadata/process/lease;
`DatasetBuildJob` Java gọi lại các engine MLD-01..05, Spark đọc records,
tính Mc/window/features và ghi Iceberg/Parquet. Không chạy model trong Airflow.

```text
wait_gold_published → resolve_gold_snapshot → audit_gold_input → resolve_mc
→ select_mainshocks → build_candidate_windows → build_spacetime_features
→ validate_feature_snapshot → export_parquet → mark_dataset_exported
```

- Plan đọc publication và exact physical snapshot thật qua Trino, không tin
  riêng `published=true` từ request. Java quyết định dataset identity, tránh
  khác canonical float/Unicode giữa Python/Java; bigint không qua float.
- Resolve tạo/reuse BUILDING manifest. Audit và coverage dựa trên
  Bronze/Silver/Gold receipts; Mc phải reliable, windows/features có quality
  và resource gates. REPRODUCTION cần 225 tháng, EXTENSION cần 63 tháng.
- Các phase audit/Mc/window/feature chạy engine thật từ cùng pin, không phải
  status-only mock. Có recompute từ pin khi chuyển phase; chưa tối ưu cache
  liên task. Trong mỗi phase, audited/mainshock/candidate/feature DataFrames
  được cache và unpersist trong finally để không tính lại cho từng cột;
  local[1] dùng 4 shuffle partitions. Receipt ghi counts/fingerprint, không rows.
- Validate commit/reconcile mainshock/candidate snapshots, Trino readback
  rồi chuyển VALIDATED, **chưa** xuất bundle.
- Export gọi MLD-05 thật: kiểm lại coverage/quality, portable bundle,
  Parquet/checksum readback, chuyển EXPORTED, pin registry và trusted handoff.
  Không viết lại manifest bằng adapter mock/Python.
- Mark verify lại exporter và `TrustedDatasetResolver`, rồi release đúng
  owner. Leaf kiểm receipt/checksum/release trước run summary. Lỗi observer
  sau export hợp lệ không rollback EXPORTED hoặc xóa bundle.
- PROVISIONAL/operational Gold không dùng cho ML. DAT-02 pilot không được
  đổi tên thành official. Synthetic QA không chứng nhận research readiness.

## Chuẩn bị riêng một lần

```bash
make docker-build
make up-ml WAIT_TIMEOUT=600
```

Đây là bước chuẩn bị, không nằm trong mỗi test. DAG còn cần ba Airflow
services healthy; dùng `make docker-up WAIT_TIMEOUT=600` nếu đã build đúng
image và stack chưa chạy. Không reset dữ liệu.

Compose đặt `ML_BUILD_MODE=real`; runtime không có biến này cũng mặc định real,
giá trị sai bị từ chối, không fallback mock. Nhánh cũ chỉ opt-in
`legacy-test` trong offline tests, không cấu hình trên deployment. Cấu hình:

| Biến | Ý nghĩa |
|---|---|
| ML_BUILD_NAMESPACE | Namespace ghi dataset, mặc định iceberg.ml; QA dùng namespace riêng |
| ML_BUILD_RUNNER_SHA256 | SHA-256 JAR thực tế trong container; bắt buộc, không tự bypass |
| ML_BUILD_RUNNER_TIMEOUT_SECONDS | Timeout từng phase, mặc định 1800s, giới hạn 1..7200s |
| ML_BUILD_STAGING_ROOT | Context/receipts/attempts, mặc định staging/ml-build |
| ML_BUILD_EXPORT_ROOT | Bundle hoàn chỉnh, mặc định staging/ml-export |
| MLI_DATASET_REGISTRY_ROOT | Trusted metadata, mặc định staging/ml-artifacts |

Sau build, xem SHA bằng `shasum -a 256 spark/target/japan-earthquake-etl-runner.jar`;
đối chiếu với JAR trong container rồi cấu hình pin/cập nhật deployment.
Không thay JAR/config khi còn run active. Mount JAR/DAG riêng chỉ dành cho
nghiệm thu explicit có pin; không coi đó là cached-image preflight đã đạt.

```bash
make check-mli-build-runtime
make test-mli-build
```

Lệnh đầu read-only, không build/pull/start/init hoặc acquire lease. Lệnh sau
offline graph/control-plane + Java/Spark tests; cần JDK tương thích Spark và
loopback socket. Legacy/mock tests giữ để kiểm boundary cũ, không dùng để
đánh dấu EXPORTED thật.

## Chuẩn bị request và chạy trên Airflow

Request là **nội dung conf**, không bọc thêm `conf`. Chuẩn bị từ Published
publication và config MLD-01/04 thật, không copy pins evidence hoặc chọn latest:

| Trường | Bắt buộc |
|---|---|
| dataset_split | REPRODUCTION hoặc EXTENSION |
| period_start_utc, period_end_utc_exclusive | REPRODUCTION [2000-01-01,2018-10-01), EXTENSION [2018-10-01,2024-01-01), timestamp UTC |
| observation_cutoff_utc | >= period end và >= publication time, có proof |
| filter_config | JMA_BULLETIN/UNIFIED, natural=true, study area=true; completeness/mainshock/window/feature_scaling configs version hóa |
| Versions | dataset_config_version, audit_rule_version, mc_method_version, window_model_version, feature_version, scaling_config_version, code_version |
| publication_reference | gold_publication_id, gold_snapshot_id (integer chính xác), gold_table_name, physical_gold_table_name, publication_bundle_sha256, verify_status=PASSED, published=true |

Chi tiết: [ML identity](../specs/ML_DATA_MODEL.md),
[feature/export](../specs/FEATURE_EXPORT.md), [MLD-05 runbook](./MLD-05_FEATURE_EXPORT.md).
Không gửi credentials, shell command, output paths, Mc tự chọn hoặc mock_pipeline
qua DagRun conf. Namespace/paths/runner pin do operator cấu hình môi trường.

1. Kiểm MinIO/Catalog/Trino/Airflow healthy, không có source/build/backfill run
   khác active; source lease trống. Không takeover foreign lease.
2. Airflow UI → `mli_02_build_dataset` → unpause riêng DAG manual này.
3. Trigger với conf đã resolve ở trên, run ID duy nhất. Không trigger daily.
   Với snapshot bigint >2^53, dùng JSON file/CLI Python của Airflow, không
   parse/reserialize bằng JavaScript/Excel hoặc lấy số đã làm tròn từ UI.
4. Graph phải lần lượt đủ 12 task success; cuối là
   `mark_dataset_exported.contract_gate`. Không clear/retry đồng loạt nếu lỗi.
5. XCom chỉ metadata/receipt bounded, không events/candidates/Parquet/private
   stdout/stderr. Sau nghiệm thu pause lại DAG.

Đặt REQUEST_PATH là file conf thực tế đã resolve, RUN_ID là run ID mới. CLI
chuyển nguyên JSON text, Python/Java giữ integer chính xác; UI dùng quan sát Graph:

```bash
make check-mli-build-runtime
docker compose exec -T airflow-scheduler airflow dags trigger --run-id "$RUN_ID" --conf "$(cat "$REQUEST_PATH")" mli_02_build_dataset
```

Sau khi run terminal success và không còn run khác active, collector read-only
kiểm đủ 12 task, pins/Trino, Gold không đổi, checksum và release đã tồn tại:

```bash
docker compose exec -T airflow-scheduler python3 /opt/airflow/dags/ml_dataset_build_acceptance.py --run-id "$RUN_ID"
```

Rerun có thể thêm `--compare-run "$FIRST_RUN_ID"` để xác nhận cùng ID/ba pins/
ba SHA. Collector không trigger/clear/ghi registry/tạo summary hoặc release lease;
nếu evidence leaf chưa tồn tại thì fail closed, không tự hoàn tất hộ DAG.

## Nhận biết thành công

Context tại `ml-build/mli02-<hash exact dag/run>/context.json`; Airflow run ID
gốc vẫn là lease owner, hash chỉ dùng làm safe Java/build path. Trong cùng root:

```text
context.json
attempts/<phase>-<unique>/request.json, receipt.json, outcome.json
attempts/<phase>-<unique>/stdout.private, stderr.private   (0600)
receipts/<step>.json
lease-release.json
run-summary.json
```

Summary: status DATASET_EXPORTED, local_handoff_verified=true,
lease_released=true; export counts >0, invalid_feature_count=0,
quality_gate_status=PASSED, registry/mainshock/candidate pins và checksum pins.
Đây là nghiệm thu kỹ thuật: scientific_approval=false, research_ready=false.
colab_handoff.ready=false, auto_triggered=false; cần EXP-02 local gate rồi mới
handoff Colab thủ công, không lấy export success làm kết quả fit/parity.

Iceberg `<ML_BUILD_NAMESPACE>.dataset_manifest` phải EXPORTED với đúng ID/pins.
Trino readback dùng FOR VERSION AS OF với pin decimal từ receipt, không latest.
Một bảng có thể chứa nhiều dataset QA/official; mọi count phải lọc exact dataset_id.

```text
ml-export/dataset_id=<id>/
  features-part-00000.parquet         (có thể nhiều part theo resource bound)
  export-checksums.sha256             (chỉ index data parts)
  dataset_manifest.json               (tham chiếu data-only index, không hash vòng)
  checksums.sha256                    (index parts + manifest + data-only index)
ml-artifacts/<id>.json                (trusted metadata + EXPORTED registry pin)
ml-artifacts/exports/<id>/export-checksums.sha256
```

Java kiểm full candidate lineage trong trusted registry; Python chỉ hash file,
không kéo candidate rows vào XCom. Local loader dùng hai expected SHA
bundle_manifest_sha256 và checksums_sha256 từ receipt tin cậy:

```bash
make verify-ml-export MLD05_BUNDLE_PATH="$BUNDLE_PATH" MLD05_MANIFEST_SHA256="$MANIFEST_SHA" MLD05_CHECKSUMS_SHA256="$CHECKSUMS_SHA"
```

Copy/download và verify theo [MLD-05](./MLD-05_FEATURE_EXPORT.md), sau đó
Jupyter EXP-01/02. Không chuyển .env, keys, private logs hoặc warehouse lên Drive.
Colab/Drive chưa triển khai phải mở lại SEC-01 trước migration.

## Rerun, failure và recovery

- DagRun khác cùng publication/config/cutoff giữ dataset identity; reuse bundle
  và ba pins, không duplicate feature snapshot. Không đổi output root hoặc code
  version giữa retry rồi xem là cùng build.
- Retry task dùng canonical successful receipt nếu đã có; logs từng invocation
  riêng, không overwrite attempts thất bại. Mỗi step kiểm context bất biến,
  predecessor receipt SHA, owner và pin.
- Metadata <=64KiB; không áp giới hạn này cho trusted registry chứa candidate
  lineage lớn, do Java đọc/kiểm chứ không đưa vào Python/XCom.
- Timeout/nonzero giữ lease và private logs. Kiểm JVM/task/Trino query
  quiescence trước replay theo [QA-04](./QA-04_FAILURE_RECOVERY.md).
  Không TTL/foreign lease takeover.
- Nếu release thành công nhưng ghi release-receipt bị ngắt, leaf fail closed;
  operator phải reconcile đúng run, không viết receipt/mark-success.
- Lỗi sau VALIDATED giữ snapshot, export tiếp sau đối soát. Lỗi sau EXPORTED
  giữ bundle/pins, chỉ reconcile trusted handoff/observer; không hạ trạng thái
  hoặc regenerate dữ liệu để che lỗi.

## Giới hạn / task tiếp theo

- **MLD-05 — feature/export** đã có component QA đủ temporal scope. Backfill
  và nghiệm thu **catalog thật toàn kỳ nghiên cứu chưa có task riêng**; không
  lấy synthetic component QA làm bằng chứng thay thế hoặc tự đổi MLD-05 về chưa Done.
- **EXP-02 — baseline/Colab parity**, **EXP-03 — adaptive scientific gates**:
  không mở gate chỉ vì DAG export thành công.
- **MLQ-01 — Gold→dataset→experiment→import E2E**, **MLI-04 — report**:
  MLI-02 không thay các nghiệm thu này.
