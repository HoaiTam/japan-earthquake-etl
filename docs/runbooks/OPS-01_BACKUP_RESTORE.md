# OPS-01 — Backup và restore smoke an toàn

Mục tiêu: chứng minh một **tập đại diện nhỏ** Gold/ML có thể được lấy từ bản
backup, đăng ký lại catalog và truy vấn bằng Trino mà không sửa bảng gốc.
Đây **không phải** backup toàn bộ warehouse, disaster recovery toàn project,
official experiment hoặc nghiệm thu bảo mật cloud của SEC-01.

## 1. Có gì trong bản backup?

- [Profile được version](../../config/ops/backup-smoke-v1.json): đúng 7 bảng
  Gold/publication và 6 bảng ML của các QA run đã có. Gold có dữ liệu nguồn
  thực đã chuẩn hóa; ML là synthetic component acceptance của
  [MLI-03](../evidence/MLI-03_RUNTIME_ACCEPTANCE.md), không phải fit nghiên cứu.
- Catalog export: identifier → metadata URI/UUID/current snapshot/count/row
  checksum. Lưu toàn bộ closure của các metadata JSON được pin: metadata-log,
  snapshot manifest lists, Avro manifests, Parquet và Puffin statistics.
  Không chỉ zip `metadata.json` rồi dựa vào Parquet cũ để gọi là đã restore.
- Exact bytes của dataset/export registry, completion manifest, result Parquet,
  config/metrics/requirements và evidence nhỏ của đúng MLI-03 run.
- Schema `public` của PostgreSQL Airflow, **không có rows**: không backup
  connections, variables, XCom, passwords hay session/account records.
- Hai Bronze manifests JMA2000/USGS01/10 đã pin SHA từ evidence JMA-05/QA-01;
  lineage vẫn nằm trong bridge (`raw_object_uri`, manifest IDs, source keys).
  **Raw Bronze/Silver và toàn source history không nằm trong
  bộ smoke**; vẫn phải giữ source pins/warehouse gốc. Không dùng kit DAT-03
  hoặc bản smoke này làm lý do xóa chúng.

ZIP có manifest/index SHA-256; blobs dedup theo SHA. Archive/receipts mode600,
thư mục riêng mode700 trong ignored `staging/`; không commit/upload tự động.
Secret scan kiểm raw JSON/text, decoded Avro/Parquet và các configured secrets
từ `.env` của deployment trong memory, không ghi giá trị vào report.

## 2. Setup

Tại repository, dùng Python **3.13** và venv riêng; không đổi môi trường model:

```bash
python3 -m venv .venv-ops
.venv-ops/bin/python -m pip install -r requirements-ops.txt
make test-ops-backup OPS_PYTHON=.venv-ops/bin/python
```

Runtime commands cần 5 container hiện có: MinIO, Iceberg REST, Trino, Airflow
scheduler và PostgreSQL đều healthy. Không cần bật Spark/API/DAG processor.
Không có build/pull/start/init/restart tự động trong các lệnh dưới đây.
`desktop-linux` là Docker context mặc định; CLI hỗ trợ `--context default`
và `--project` cho deployment riêng.

Điều kiện trước mọi phiên:

- Host trống ít nhất4GiB; **không** lấy dung lượng logical của Docker VM thay thế.
- Lease source empty, không Java/DagRun queued/running hoặc truy vấn Trino active.
- Đúng các bảng/run của profile hiện hữu. Trên máy khác, không tự seed lại hoặc
  thay namespace cho đủ bảng: phải chọn lại một profile đại diện và nghiệm thu.
- `OPS_DEPLOYMENT_ROOT` trỏ đến checkout giữ `.env` của deployment thật để quét
  exact configured secrets; không gửi `.env` hoặc credential lên chat/Drive/Git.

## 3. Backup chỉ đọc

`OPS_OUTPUT` phải là đường dẫn staging **chưa tồn tại**. Với deployment ở cùng
checkout, biến `OPS_DEPLOYMENT_ROOT` mặc định đã đúng:

```bash
make ops-backup OPS_PYTHON=.venv-ops/bin/python OPS_OUTPUT=staging/ops01-backup-local
```

Nếu deployment ở checkout khác, thêm `OPS_DEPLOYMENT_ROOT` với đường dẫn thật.
Không phải copy `.env` sang checkout làm code. Output:

- `backup.zip`: exact objects/artifacts gốc, không rewrite nội dung backup.
- `backup.SHA256`: digest dùng làm trusted pin từ chính phiên đã kiểm chứng.
- `receipt.json`: chỉ có khi toàn bộ graph, SQL fingerprint, artifact lineage,
  secret scan và postflight đạt `BACKUP_VERIFIED`.
- `failed.json` khi lỗi; giữ intent/artifacts, không tự chạy vòng lặp retry.

Giới hạn v1: ≤16 bảng, ≤512 objects, ≤32MiB/object, ≤64MiB graph; Avro
≤10.000 entries/file, Parquet ≤50.000 rows/file cho decoded secret scan.
Hỗ trợ Iceberg format v2/Parquet và Puffin theta với footer không nén.
Delete files, partition statistics, footer nén hoặc external graph refs bị chặn;
không bỏ chúng âm thầm, không tăng cap hoặc sampling để pass.

## 4. Verify offline

Dùng SHA đã nhận từ receipt/nguồn tin cậy, không tự tính lại một ZIP lạ rồi coi
đó là authentic backup. Lệnh có thật; thay SHA theo phiên backup của mình:

```bash
make ops-verify-backup OPS_PYTHON=.venv-ops/bin/python OPS_BUNDLE=staging/ops01-backup-local/backup.zip OPS_SHA256="$(tr -d '\n' < staging/ops01-backup-local/backup.SHA256)"
```

Expected: `event=ops01_offline_verified tables=13 objects=...`, exit0.
Verify không dùng Docker/network, kiểm ZIP member set/duplicate/traversal,
blob SHA/size/full graph, snapshot IDs int64, original result checksums/counts
và dataset→export→table pins. Không extract ZIP không kiểm soát.

## 5. Restore riêng và nghiệm thu

Đọc/run chỉ với backup tin cậy; schema SQL là mã thực thi từ deployment nguồn,
không được chạy archive/schema do người lạ cấp chỉ vì ZIP khớp SHA họ tự cung cấp.

```bash
make ops-restore OPS_PYTHON=.venv-ops/bin/python OPS_BUNDLE=staging/ops01-backup-local/backup.zip OPS_SHA256="$(tr -d '\n' < staging/ops01-backup-local/backup.SHA256)" OPS_OUTPUT=staging/ops01-restore-local
```

Controller sinh runUUID mới và lưu `operation.json`/`restore-intent.json` trước
remote writes. Chỉ tạo:

- Objects tại `s3://.../warehouse/ops01-restores/<run>/...`, conditional put
  `If-None-Match:*`: không overwrite bytes cũ. Không đổi bucket policy/IAM.
- Hai namespace `ops01_<run>_0/1`, REST register đúng metadata đã phục hồi.
  Không bật Trino `register_table` bằng restart hoặc sửa cấu hình production.
- Database PostgreSQL `ops01_<run>` từ template0, chỉ restore schema. Không đổi
  connection string hoặc state của Airflow đang vận hành. DDL duy nhất được
  điều chỉnh là `CREATE SCHEMA public` → `CREATE SCHEMA IF NOT EXISTS public`
  vì template0 đã tạo public; không bỏ table/index/constraint.

Restore chỉ remap Iceberg metadata/Avro URI và manifest lengths. Snapshot IDs,
UUID, data rows và Parquet/Puffin bytes giữ nguyên. **Không rewrite giá trị
lineage bên trong bảng**, không cấp trạng thái Published/CANDIDATE mới cho
scratch namespace hoặc đưa namespace này vào serving.

Điều kiện success:

1. Graph sau remap khép kín, không trỏ tới data/metadata nguồn để đọc bảng.
2. Mỗi object upload có GET/SHA readback; 14 artifacts restore local đúng bytes.
3. Trino `count(*) + checksum(ROW(...))` của13bảng bằng bản gốc, cùng snapshot;
   đọc lại lần2 không ghi/đăng ký trùng, kết quả bất biến.
4. Dataset registry/experiment/membership/candidate/summary joins cân bằng;
   completion/export manifest hashes và input snapshots khớp backup.
5. Schema columns/types/defaults Airflow restore bằng bản nguồn; không có rows.
6. Source metadata pointers, service IDs/start times/restarts giữ nguyên;
   cuối phiên idle/lease empty. Không release/acquire lease, pause hoặc clear DAG.

Expected: `event=ops01_restore_verified ...`, `receipt.json` có
`status=RESTORE_VERIFIED`. Xem [actual evidence](../evidence/OPS-01_BACKUP_RESTORE_20261117.md).

## 6. Khi lỗi và giới hạn

- Không tự retry/clear/takeover/release. Giữ backup, source pins và output
  folder; đọc `failed.json` và intent để xác định bước cuối đã gửi. Unknown
  outcome phải đối chiếu exact namespace/prefix/DB bằng read-only trước quyết định.
- Không dùng DROP TABLE để cleanup: các metadata cùng UUID/snapshot có thể còn
  phục vụ provenance. Tool không có delete/drop command. Giữ các namespace/DB
  scratch nhỏ để review; cleanup cần yêu cầu riêng với exact targets.
- Đây là smoke phục hồi representative tables, không snapshot volume
  `minio_data`/`iceberg_catalog_data`, không phục hồi mọi views hoặc toàn scheduler
  history. Registry export + original metadata đủ dựng lại **các bảng đã chọn**,
  không chứng minh phục hồi toàn deployment sau mất ổ đĩa.
- Full disaster recovery chưa có task riêng; không ghi ngầm là đã hoàn tất.
  [SEC-01](../task/tasks/SEC-01.md) vẫn sở hữu cloud ACL/roster/session gates;
  [MLQ-01](../task/tasks/MLQ-01.md) và [MLI-04](../task/tasks/MLI-04.md) vẫn sở hữu
  official ML E2E/scientific report parity, không được đóng bằng smoke này.

## Tài liệu kỹ thuật

- [Iceberg REST specification](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/open-api/rest-catalog-open-api.yaml): namespace/register endpoints.
- [Iceberg Puffin specification](https://iceberg.apache.org/puffin-spec/): footer/statistics references.
- [fastavro writer](https://fastavro.readthedocs.io/en/latest/writer.html): preserve schema/header metadata khi rewrite URI.
- [PostgreSQL pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html): consistent schema-only backup, cảnh báo restore untrusted SQL.
