# MLD-05 — Chạy và kiểm tra feature export

Component acceptance ngày 2026-10-21:
[runtime/Trino/rerun và local credential-free loader](../evidence/MLD-05_RUNTIME_ACCEPTANCE.md).
Fixture tổng hợp đủ temporal scope, không thay official research-data acceptance.

## Chuẩn bị riêng

Đọc [cơ chế/contract](../specs/FEATURE_EXPORT.md). Cần JDK17 cho Spark/Maven,
Docker Desktop và .env hợp lệ. Từ repo root, chỉ chuẩn bị lại khi source/config
đã đổi hoặc runtime chưa sẵn sàng:

```bash
make docker-build
make up-ml WAIT_TIMEOUT=600
```

Không chạy build/start trong mỗi smoke. Nếu JAR/images/init đã đúng fingerprint,
chỉ cần MinIO/Catalog/Trino healthy, không cần Airflow/Spark standalone.
Notebook cần môi trường EXP-01/PyArrow đã khóa; chuẩn bị bằng make pilot-env
nếu chưa có. Không cài lại môi trường trong từng lần verify/fit.

## Test implementation

```bash
make test-ml-export
make smoke-ml-export
```

Offline gồm Java validator/Parquet/Iceberg retry/coverage và process/lease tests.
Smoke chỉ preflight rồi one-off --no-deps --pull never. Nó tạo archive tổng hợp
1999–2018 (1999 dùng làm halo), ingest bằng production Bronze writer, thực hiện
10 bounded ETL batches phủ 225 tháng REPRODUCTION, rồi export/reuse bằng job thật.
Mỗi write batch vẫn <=24 tháng; cumulative registry <=480 tháng. Trước đây
parent registry bị đọc nhầm bằng giới hạn write-window; MLD-05 tách reader này,
không tăng giới hạn affected scope hoặc source bytes/rows của một lần xử lý.

Nhận biết thành công:

- Terminal status PASS, task MLD-05 và lease_release.status RELEASED.
- runtime-report.json có 10 publication, required_month_count=225,
  partial_coverage_rejected=true, trusted_resolver_verified=true.
- first.status DATASET_EXPORTED, rerun.reused=true, ba snapshot pins và ba
  checksum pins giữ nguyên. Gold trước/sau export không đổi.
- Partial dataset ở lượt đầu vẫn BUILDING, không có bundle export; không
  lấy đủ temporal scope của archive tổng hợp làm completeness catalog thật.

Event bắt đầu in root /opt/pipeline/staging/mld-05/mld05-qa-<uuid>/.
Trong đó có request, private logs 0600, từng operation/sáu receipts,
runtime-report, execution, lease-release và acceptance. Bundle QA nằm ở
ml-export/dataset_id=<id>/ bên trong root đó; trusted metadata ở registry/.
Không đưa raw Parquet/archive/private logs vào Git.

## Export dataset đã chuẩn bị thật

Chỉ dùng dataset_id/namespace đã resolve trong catalog, không dùng ID/pins
từ ví dụ hoặc đổi tên DAT-02 pilot. Dataset config phải pin feature_scaling_config,
window_config và mainshock_selection_config; không đổi config của ID đã tồn tại.
CLI thực hiện audit/window/features bằng shared code để kiểm lại nguồn trước export.

```bash
make export-ml-dataset MLD05_NAMESPACE="$ML_NAMESPACE" MLD05_DATASET_ID="$DATASET_ID"
```

Hai biến phải được đặt từ manifest thật. Report thành công có status
DATASET_EXPORTED, feature/mainshock counts, registry/candidate/mainshock pins
dạng chuỗi, dataset_manifest_ref và SHA cho importer. Có thể lấy public bundle
manifest SHA và checksums SHA từ cùng receipt để kiểm offline.

Default production outputs (khác staging từng execution):

- /opt/pipeline/staging/ml-export/dataset_id=<id>/
- /opt/pipeline/staging/ml-artifacts/<id>.json
- /opt/pipeline/staging/ml-artifacts/exports/<id>/export-checksums.sha256

Vì output theo dataset, execution mới cùng ID reuse nguyên bundle/pins.
Operator có thể cấu hình MLD05_EXPORT_ROOT / MLI_DATASET_REGISTRY_ROOT trên
container nếu triển khai khác; không chuyển output root giữa các retry rồi
coi artifact cũ là mất. Không tự đổi .env để vượt gate.

## Kiểm offline và handoff Jupyter/Colab

Sao chép đúng thư mục dataset_id từ volume staging sang local/Drive, kèm
receipt checksum metadata qua kênh đã tin cậy. Không copy warehouse hay
MinIO credentials. Không chỉ nhận checksum nằm bên trong bundle tự cung cấp.

Đặt BUNDLE_CONTAINER_PATH bằng exact directory từ run đã nghiệm thu (production
ở /opt/pipeline/staging/ml-export/dataset_id=<id>; QA ở root run/ml-export/).
Helper dưới đây chỉ mount volume để copy, không chạy Java hoặc ETL:

```bash
MLD05_COPY_CONTAINER="mld05-copy-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p staging/ml-download
docker compose --profile ml run --rm --no-deps --pull never --name "$MLD05_COPY_CONTAINER" -d --entrypoint sleep mld05-feature-export 300
docker cp "${MLD05_COPY_CONTAINER}:${BUNDLE_CONTAINER_PATH}" staging/ml-download/
docker stop "$MLD05_COPY_CONTAINER"
```

Chỉ stop helper vừa tạo; --rm bỏ container đó, không bỏ volume hay bundle.
Nếu copy lỗi, kiểm lại exact path/ID; không dùng wildcard chọn dataset gần nhất.
Sau verify local, tải nguyên directory lên Drive và kiểm lại bằng cùng hai SHA
trước notebook Colab; không chỉnh manifest/checksum hoặc lấy một vài part lẻ.

Đặt MLD05_BUNDLE_PATH, MLD05_MANIFEST_SHA256 (= bundle_manifest_sha256),
MLD05_CHECKSUMS_SHA256 (= checksums_sha256) từ receipt vừa nghiệm thu:

```bash
make verify-ml-export MLD05_BUNDLE_PATH="$BUNDLE_PATH" MLD05_MANIFEST_SHA256="$MANIFEST_SHA" MLD05_CHECKSUMS_SHA256="$CHECKSUMS_SHA"
```

Expected: VERIFIED_EXPORTED_BUNDLE, row count khớp, credentials_required=false,
scientific_approval=false. Script kiểm hai expected SHA trước rồi gọi chính
EXP-01 OFFICIAL loader: checksum tất cả files, schema/types, finite, count,
lineage, unique grain và zero-self. Không chạy model, Docker hoặc gọi nguồn.
Colab có thể chạy cùng script/module + requirements của EXP-01 trên bản bundle
đã chuyển; việc fit/parity chỉ sau local EXP-02 gate, không tự trigger Colab.

## Khi lỗi và scoped cleanup

- DS_EXPORT_COVERAGE_REQUIRED/INCOMPLETE: chưa có receipt-bound history đủ
  split; giữ parent BUILDING, không tự sửa coverage/period/research_ready.
- DS_EXPORT_MC_UNRELIABLE, feature gate/resource/biên period: dừng export,
  xem audit/config. Không lấy tập valid nhỏ hơn để che lỗi.
- Snapshot/identity conflict hoặc corrupt bundle: giữ dữ liệu và pins để
  điều tra; không overwrite, regenerate hoặc nhặt parts từ attempts khác.
- Nonzero/timeout giữ lease. Kiểm container/JVM/query Trino quiescence và
  reconcile đúng run trước operator release theo
  [QA-04 recovery](./QA-04_FAILURE_RECOVERY.md). Không TTL/stale takeover.
- Nếu QA đã có history-complete.json nhưng lỗi ở exporter, có thể tiếp tục
  bằng đúng owner/run sau khi xác nhận JVM cũ đã dừng. Java đối chiếu namespace,
  10 publications và current Gold pins trước khi reuse checkpoint; không nhận
  checkpoint thiếu hoặc Gold đã đổi. Dùng đúng RUN_ID từ request đã lưu:
  `docker compose --profile ml run --rm --no-deps --pull never --entrypoint python3 mld05-feature-export /opt/airflow/dags/feature_export_runtime.py --resume-qa "$RUN_ID"`.
  Retry lưu log/execution mới, không ghi đè log lỗi; lease phải vẫn thuộc run đó.
  Chưa có checkpoint full history thì không resume tùy ý một batch bằng CLI này.
  Nếu lỗi cũ xảy ra sau EXPORTED, receipt đầu khi resume có reused=true là đúng:
  QA chỉ nhận khi có checkpoint và execution.json nonzero gốc còn nguyên.
  Không ghi đè bằng chứng đó để làm first.reused=false.
- Trino có thể ghi metadata Parquet bằng DELTA_LENGTH_BYTE_ARRAY mà Iceberg
  vectorized reader của Spark 3.5 chưa hỗ trợ. Exporter dùng row reader riêng
  cho exact pinned readback; không sửa file, pin hoặc table property dùng chung.
- Partial commit cùng dataset/identity được writer reconcile qua snapshot
  properties; replay không tạo snapshot thứ hai cho bảng đã commit. VALIDATED
  nhưng export chưa xong thì kiểm bundle/attempt trước chạy tiếp.
- Nếu đã EXPORTED nhưng trusted metadata ghi chưa xong, rerun verify nguyên
  bundle, tìm snapshot registry EXPORTED gốc và hoàn tất handoff; không thay pins.
- Không có automatic cleanup command. Trước maintenance, owner phải đối chiếu
  đúng đường dẫn .mld05-attempt-<id>, xác nhận không có process/lease dùng,
  lưu inventory/evidence và ưu tiên move/quarantine thư mục đó. Không đụng
  dataset_id đã công bố, referenced snapshot, root staging hoặc warehouse.

## Giới hạn / task tiếp theo

- [MLI-02 — Tạo Airflow DAG build ML dataset](../task/tasks/MLI-02.md): còn
  nối step runner/DAG live; exporter CLI không tự hoàn tất DAG task.
- [EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md):
  còn official experiment/Colab parity, không được nhận Done từ loader verify.
- [MLQ-01 — Chạy E2E Gold snapshot đến ML report](../task/tasks/MLQ-01.md):
  còn whole-flow official acceptance; component QA không thay gate này.
- [QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  còn profiling quy mô lớn; v1 vẫn giữ source bytes/rows và candidate hard limits.
- Full catalog thật 2000–2018/2018–2023 phải được backfill/publish và đủ coverage
  trước official export; [QA-03 — Kiểm thử JMA historical backfill](../task/tasks/QA-03.md)
  chỉ có bounded evidence, không chứng minh đã tải toàn kỳ.
  [ORC-03 — Implement backfill và reprocessing](../task/tasks/ORC-03.md) và
  [ORC-06 — Resolve affected months và dựng full-month Silver state](../task/tasks/ORC-06.md)
  cung cấp execution/state; [DAT-03 — Catalog thật và handoff](../task/tasks/DAT-03.md)
  sở hữu full research backfill acceptance, hiện In Progress.
