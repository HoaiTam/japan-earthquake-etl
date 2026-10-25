# DAT-03 — Bước 3 collection, bước 4 bounded real-source E2E

## Phạm vi

Tiếp tục branch/PR #99; không tạo research release từ bốn tháng đại diện.
[Contract collection](../specs/RESEARCH_COLLECTION.md) và
[task DAT-03](../task/tasks/DAT-03.md) là nguồn acceptance. Business ETL,
audit/Mc/windows/features/Parquet dùng Java/Spark; Python chỉ capture bytes,
pin/transfer metadata, lease, gọi runner và kiểm evidence.

## Bước 3 — Collection preparation

`ResearchCollectionJob.prepare` đọc persisted BUILDING parent và exact Published
Gold pin qua Trino, xác minh receipt-bound đủ 225/63 tháng, chạy audit/Mc trên
**toàn split**, rồi shared window engine/strict-split và feature engine.
Không nhận events/Mc/completeness flag từ request để thay source proof.

- Stable next-fit theo mainshock ID; mỗi shard <=128 mainshocks và <=100k
  feature rows, tối đa 64 shards/4096 mainshock metadata.
- Một PRE/POST window nằm trọn một shard; oversized window fail, không split
  hoặc truncate. Exclusions giữ từng mainshock/reason/required interval.
- Cùng parent dataset_id, feature grain/29 columns và global Mc/config/hash.
- `collection_id`/plan SHA pin parent, Gold/split/cutoff, coverage/Mc/exclusions,
  layout và toàn assignment/count. Không estimate Mc lại cho từng shard.
- Readback Parquet bằng Spark, recompute features/plan/counts; exact files,
  schema/content/SHA và external manifest/checksum pins. Rerun verify nguồn
  upstream và bytes cũ; không tự sửa artifact hỏng hoặc regenerate file mất.

Output dưới output_root/col_<hash>: collection-manifest.json, checksums.sha256,
_PREPARED, shard_NNNNN/mainshocks.parquet và features.parquet. External receipt
ở output_root/receipts/col_<hash>.json. Trạng thái
**RESEARCH_COLLECTION_PREPARED**, research_ready=false, official_exported=false,
parent BUILDING không đổi. Không tạo official _SUCCESS/EXPORTED, không ghi
Iceberg/registry hoặc mở MLI-03 gate.

Entrypoint Java `ie212.earthquake.spark.ml.ResearchCollectionJob` nhận một
request JSON bounded gồm đúng: collection_version, ml_namespace, dataset_id,
dag_id=dat03_collection_prepare, run_id, layout_limits, output_root,
runner_sha256. Cần prebuilt exact JAR/dependencies, môi trường Spark/Trino/MinIO
và own whole-run source lease. Chưa chạy production entrypoint khi catalog
chưa đủ coverage; không dùng bốn tháng bước 4 để lách gate.

Test component offline, không Docker/source network:

```bash
make test-research-collection
make test-research-e2e
```

Test synthetic chỉ chứng minh component/sharding/Parquet/tamper behavior;
không full-catalog Mc hoặc scientific evidence.

## Bước 4 — Các lệnh tường minh

Các service có sẵn phải healthy; test không build/pull/start/init, đổi deployed
JAR/config, cron, DagRun hoặc credential. Không có active DagRun; shared lease
trống trước acquire, giữ exact owner toàn phiên Java/Trino/validation.

1. Có JAR package đúng code và 25 JMA captures đã pin ở bước 1/2.
   Lệnh chuẩn bị **không tự build JAR**.
2. Capture USGS riêng cho December2023 + halo16s, rectangle20–50N/120–155E,
   không magnitude floor. [API USGS](https://earthquake.usgs.gov/fdsnws/event/1/)
   dùng endtime inclusive, nên query kết thúc 00:00:15.999 để đại diện exclusive
   2024-01-01T00:00:16Z; limit20k/offset1, saturated response không được truncate.
3. Stage exact JAR/baseline+boundary inventory/five JMA ZIP/capture metadata và
   USGS capture vào thư mục UUID mới. Chỉ tên file allowlist, bounded stream,
   check SHA; tạo file mới trong existing volume bằng user Airflow, không root/
   chown hoặc thay mount/deployed runner.
4. Chạy một explicit E2E. Bốn tháng cố định: 2000-01, 2011-04, 2016-04,
   2023-12. Case cuối dùng h2023+h2024+USGS, không đưa 2024 ngoài split vào ML.
   Mỗi tháng đủ sáu phase và rerun **cùng operation/context**.

```bash
make capture-research-e2e
make prepare-research-e2e RESEARCH_DEPLOYMENT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl
```

Prepare in đường dẫn UUID mới. Gán **đúng đường dẫn được in** cho biến shell
`dat03_e2e_folder`, không copy identity/snapshot của evidence làm đầu vào mới:

```bash
make run-research-e2e RESEARCH_DEPLOYMENT_ROOT=/Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl RESEARCH_E2E_FOLDER="$dat03_e2e_folder"
make verify-research-e2e RESEARCH_E2E_FOLDER="$dat03_e2e_folder/runtime"
```

Runtime dùng `/opt/spark/bin/spark-submit --master local[1]`, driver768MiB,
MaxDirectMemory256MiB và hai Iceberg1.10.1 JAR đã có. Timeout3600s là fail
bound, không ETA. Chỉ namespace `iceberg.dat03_e2e_<uuid>` mới, không Gold core.
Không tự tải lại source trong Java: dùng đúng original capture bytes/HTTP/
retrieval/release/SHA để publish immutable BronzeReady rồi adapter v2 thật.

## Nhận biết thành công / lỗi

Retained run lỗi ngày24/10: dùng [DAT-03 recovery khối 1](./DAT-03_RECOVERY.md)
để kiểm tra chỉ đọc; không chạy fresh-run entrypoint lại trên cùng folder.

- Mỗi tháng: `event=dat03_e2e_month_passed`, first/rerun giống hệt receipts,
  snapshot IDs và publication; không tạo snapshot mới khi rerun.
- Silver COMPLETE_MONTHS, affected scope đúng một tháng, reconciliation balanced.
- Sáu Gold table pins; Trino kiểm schema, counts, identity/blockers/retention.
  Collector kiểm context/receipt upstream/artifact SHA chain, commit bundle SHA,
  actual/expected và SQL count đọc exact snapshot; không chỉ tin cờ PASSED.
- Các partition tháng trước giữ fingerprint sau tháng mới; publisher kiểm thêm
  outside scope cho cả event_current và event_source_bridge.
- Worker exit0 + validation thật mới release **own** lease. acceptance.json pin
  request/report/JAR, timing/RSS/disk và release. Host copy ở runtime/; private
  stdout/stderr không commit, không gửi nhóm.
- Lỗi/nonzero/timeout: giữ artifacts, lease; không auto retry/release/takeover,
  clear DAG hoặc tạo PASS giả. Timeout không chứng minh child tree quiescent.
  Chẩn đoán exact run/process trước khi xin hướng phục hồi; không rerun lệnh mù.
- Prepare hoặc transfer lỗi trước acquire: giữ thư mục lỗi; dùng UUID mới chỉ
  khi xác nhận không execution/lease. Không overwrite nguyên thư mục cũ.

`make verify-research-e2e` là offline metadata verification, không truy vấn lại
Trino tại thời điểm verify; SQL runtime readbacks nằm trong exact report đã pin.
Hash xác minh byte identity, không là chữ ký xác thực nguồn/người gửi.

## Giới hạn / bước kế tiếp

- **DAT-03 — Catalog thật và team handoff:** bước 5 còn rollout225+63 tháng
  với source/release/checkpoint, capacity và Gold/Trino từng batch; không suy
  profile hoặc bốn tháng mẫu thành complete research catalog/ETA.
- **DAT-03 bước 6 / MLI-02 — Dataset build integration:** nối official
  collection-aware lifecycle/registry/loader/import, chạy real export từ full
  pin, rồi release và receipts ba máy. Prepared collection không thay EXPORTED;
  MLI-02 technical component Done không đổi phạm vi nghiệm thu cũ.
- **EXP-02/03/04/05:** official experiments/evaluation/extension chưa được mở
  bằng preparation/bounded E2E. Pilot vẫn DEVELOPMENT_ONLY/PARTIAL.
- **SEC-01 — Security review:** local scope giữ nguyên; chưa cloud/Drive/Colab,
  cần mở lại cloud acceptance trước migration. Không upload hoặc đổi ACL.

Kết quả thực tế, namespace/pins/counts/resources và giới hạn capacity ghi ở
[evidence bước 3/4](../evidence/DAT-03_COLLECTION_BOUNDED_E2E.md).
