# Runbook cài đặt và vận hành local

Cập nhật DOC-01 **19/10/2026**. Chọn đúng mục đích trước khi chạy:
foundation ≠ source smoke ≠ real ETL acceptance ≠ notebook experiment.
[Known issues](./KNOWN_ISSUES.md) và [onboarding](./runbooks/DOC-01_TEAM_ONBOARDING.md)
giải thích trạng thái và expected outputs; không coi mọi target đều read-only.

## 1. Yêu cầu và đường đi ngắn

Backend: Git, GNU Make, Docker/Compose plugin, Python 3, jq, unzip và công cụ
SHA; static scripts dùng rg/grep fallback tùy script. Khuyến nghị máy 16 GiB
RAM trở lên, ít nhất 4 core và 50 GiB trống; không bảo đảm đủ cho 40 năm.
Java host chỉ cần khi chạy Maven/Spark local: **JDK 17 hoặc 21**, không mặc định
mọi JDK mới đều tương thích. Docker build dùng Java17 riêng.

Notebook-only: Python 3.11–3.13, dependencies trong notebooks/requirements-lock.txt
và bundle local; **không cần Docker, Java, .env hoặc MinIO credential**.
Power BI Desktop/ODBC chỉ khi chọn Stretch BI-01..05, không phải điều kiện Core.

Tại root repository:

```bash
make help
make check-docs
make test-docs
```

Muốn test dataset/experiment nhỏ, đi thẳng
[DOC-01 onboarding](./runbooks/DOC-01_TEAM_ONBOARDING.md).
Muốn ingest/ETL thật, thực hiện lifecycle dưới đây trước.

## 2. Chuẩn bị, startup, test là ba bước riêng

### 2.1. Chuẩn bị explicit — có thể lâu, không gọi lại trong từng test

```bash
make env-init
# Chỉ tạo nếu chưa có .env; sửa mọi placeholder secret trên máy.
make check-config-local
make check-java-build-inputs
make docker-build
```

docker-build build image/JAR và pull dependencies, không start/init foundation.
Chỉ cần lần đầu hoặc build inputs thay đổi. Không bỏ test, giả fingerprint hay
xóa volume khi Maven/registry lỗi. Nếu Docker daemon không phản hồi, xử lý
Docker Desktop trước, không chạy thêm build song song.

### 2.2. Khởi động hoặc áp dụng image/config đã chuẩn bị

```bash
make docker-up WAIT_TIMEOUT=600
make docker-check
make status
```

Expected: 9 long-running services healthy, minio-init và airflow-init exited/0,
config/image fingerprint phù hợp. docker-up reuse service/init đã đúng trạng
thái, không tự build/pull. WAIT_TIMEOUT là readiness timeout, không tăng timeout
download hoặc Maven. Xem [lifecycle](./DOCKER_RUNTIME_WORKFLOW.md).

### 2.3. Smoke có phạm vi — không tự build/start/init

```bash
make smoke-foundation
make smoke-usgs-live
make smoke-jma-live
```

Chạy tuần tự và đọc phần expected ở
[foundation](./specs/FOUNDATION_SMOKE.md),
[USGS live](./specs/USGS_LIVE_BRONZE_RUNBOOK.md),
[JMA QA](./specs/JMA_BRONZE_QA.md).
Smoke có thể tạo one-off QA container, gọi nguồn hoặc ghi QA artifacts; không
phải chỉ xem health. Collector QA-01 là ngoại lệ read-only, không chạy ETL.
Không dùng target smoke khi còn run khác giữ lease hoặc đang build/đổi image.

## 3. Chọn bài test theo flow

| Mục đích | Lệnh đã có | Dấu hiệu / giới hạn |
|---|---|---|
| Unit/contracts offline | make test-contracts; make test-airflow; make test-pilot | Không chứng minh backend live; pilot tests cần model venv |
| Source → Bronze | make smoke-usgs-live; make smoke-jma-live | Exact manifests/SHA/counts, BronzeReady; không Silver/Gold |
| Readback sample | make verify-samples | Sample phải tồn tại; không tự tải, không chứng minh 40 năm |
| Silver integration | make smoke-silver-integration | Hai nguồn → bundle/reconciliation/Gold handoff bounded; không mặc định Published |
| ORC-06 proof | make test-full-month; make qa-full-month-live | Lệnh live dùng public captures/file-backed proof, không MinIO Gold commit |
| ORC-07 adapters | make test-real-etl; make qa-real-etl | Opt-in QA thật đến Published bằng DAG.test, không scheduler acceptance |
| QA-01 scheduled collector | make test-qa-01; make smoke-qa-01 với exact QA_01_RUN_ID từ UI | Đủ 8 task, cùng chain và 17 SQL mới Passed; source-only NOT_READY |
| ML audit/windows | make smoke-ml-audit; make smoke-ml-window | Bounded pinned Published input; không official EXPORTED |
| Pilot/notebook | make inspect-pilot; make inspect-exp02; make inspect-exp03 | Chọn exact PILOT_BUNDLE, fresh kernel và đọc actual gate report |
| Build/import ML | make test-mli-import; make test-qa-04 | Kiểm tra interface/offline không thay live acceptance MLI-02/03 |

Không copy nguyên bảng rồi chạy tất cả. Handoff/điều kiện/lệnh đầy đủ:
[ORC-06](./runbooks/ORC-06_FULL_MONTH_STATE.md),
[ORC-07](./runbooks/ORC-07_REAL_ETL.md),
[QA-01](./runbooks/QA-01_DAILY_E2E.md),
[MLI-02](./runbooks/MLI-02_DATASET_BUILD.md),
[MLI-03](./runbooks/MLI-03_RESULT_IMPORT.md).
ORC-07 có prepare-real-etl-runtime explicit cho image QA riêng; không chạy
prepare mỗi lần smoke hoặc lúc foundation đang có writer.

## 4. Quan sát trên UI và kiểm tra output

| Nơi | Đọc gì để nhận biết thành công? |
|---|---|
| Airflow | Exact dag_id/run_id, run_type, interval, task states; source-only success không phải Published |
| MinIO Console | Đúng configured bucket/prefix; raw + manifest Bronze; Silver bundle/full-month proof; Gold commit/publication journals |
| Trino | Exact namespace/table và snapshots trong receipt; PASSED publication, canonical/lineage/ROI counts cùng filter |
| Notebook | Fresh-kernel report, pilot_id/checksums, số window/candidate, noise/failure reasons và coverage limitations |

Mặc định host loopback: Airflow http://127.0.0.1:8080; MinIO Console
http://127.0.0.1:9001; Spark master UI http://127.0.0.1:8082; Trino
http://127.0.0.1:8081. S3 API minio:9000 và Catalog iceberg-rest:8181 chỉ nội bộ.
Không suy ORC-07 job chạy trên standalone master: adapter v1 dùng local[1].

```bash
make logs SERVICE=airflow-scheduler TAIL=100
make status
```

Log có thể chứa thông tin riêng; không gửi toàn log hoặc .env lên PR.
Counts full Bronze / selected / rejects / revisions / links / canonical /
natural-ROI là các metric khác nhau. Đối soát theo
[quality guide](./DATA_QUALITY_AND_OBSERVABILITY.md) và
[run summary](./specs/RUN_OBSERVABILITY_CONTRACT.md), không chỉ COUNT(*) một bảng.

## 5. Chạy DAG và dữ liệu thật

[Daily flow](./flows/DAILY_ETL_PIPELINE.md) mô tả cron owner orc_02_daily_sources,
default real mode disabled. Manual full-month reprocess là orc_07_real_etl,
không arbitrary conf cho daily. Trước trigger: exact request/pins/coverage,
phiên bản runtime và lease phải hợp lệ; đọc schema request tại runbook tương ứng.
Không unpause tất cả DAG để test.

**Không bật unattended current-2026 real publication với inventory JMA đến
2023 và historical request**. ORC-06/07 v1 chỉ closed months; chính sách
current-month/as-of-cutoff/JMA mới chưa có task riêng.
QA-01 còn scheduled acceptance; MLD-05 còn official export; MLI-02/03 còn live
handoff/import. Không sửa status/receipt để làm demo xanh.

## 6. Chẩn đoán và phục hồi

| Dấu hiệu | Làm gì an toàn? |
|---|---|
| Docker info/daemon treo | Kiểm tra Docker Desktop/disk/RAM; không spam build hoặc prune rộng |
| IMAGE_UNPREPARED_OR_STALE | Chuẩn bị image explicit đúng code rồi docker-up; không đổi label/receipt giả |
| Init waiting | Xem đúng init/dependency logs; không gọi init lại trong mỗi test |
| Airflow DAG thiếu | Kiểm tra mount/import errors và runtime version; không mark-success |
| SourcesReady, chưa Published | Xem real mode/context/phase receipts; đây không phải lỗi cần xóa Bronze |
| FullMonth proof thiếu | Bổ sung input coverage trong request, không thay tháng bằng overlap ba ngày |
| Timeout/partial commit | Giữ owner/lease/journal; xác nhận writer cũ và Trino statements đã dừng rồi theo scoped recovery |
| Fallback notebook warning | Dùng đúng venv nbclient/ipykernel; fallback không mở Colab gate |
| Kernel không bind được loopback | Kiểm tra sandbox/firewall/quyền chạy local kernel; không đổi report thành Passed |

Retry/resume chỉ theo [backfill/recovery](./flows/BACKFILL_AND_RECOVERY.md),
[ORC-05](./specs/RECOVERY_AND_RESOURCES.md), [QA-04](./runbooks/QA-04_FAILURE_RECOVERY.md).
Không clear/mark-success, auto-release foreign lease, sửa Parquet/metadata hoặc
xóa warehouse/down -v để chữa lỗi. Một retry giữ exact operation; một run mới
có identity mới, không tùy tiện trộn cả hai.

## 7. Dừng và backup

```bash
make stop
# Khi thật sự muốn gỡ container/network nhưng giữ named volumes:
make down
```

Dừng job đúng phạm vi trước, giữ volumes/receipts cho điều tra.
Backup/restore acceptance chưa hoàn tất: [OPS-01](./task/tasks/OPS-01.md).
Cần sao lưu nhất quán MinIO warehouse, Catalog registration state và Airflow
metadata; không coi copy nóng volume hoặc có backup file là restore đã test.
SEC-01 còn retained logs/actual ML handoff audit, xem
[known issues](./KNOWN_ISSUES.md).

## 8. Tài liệu chuyên biệt

[Make command guide](./MAKEFILE_COMMANDS.md),
[configuration/secrets](./specs/CONFIGURATION_AND_SECRETS.md),
[Compose](./specs/COMPOSE_FOUNDATION.md), [Airflow](./specs/AIRFLOW_LOCAL.md),
[Spark](./specs/SPARK_STANDALONE.md), [Iceberg/Trino](./specs/ICEBERG_TRINO.md),
[onboarding/Jupyter → Colab](./runbooks/DOC-01_TEAM_ONBOARDING.md).
