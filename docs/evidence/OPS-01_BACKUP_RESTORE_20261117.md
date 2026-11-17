# OPS-01 — Nghiệm thu backup/restore, 17/11/2026

**Done cho smoke đại diện.** Profile cuối đã backup và restore đầy đủ, không
full disaster recovery hoặc scientific approval. [Machine-readable evidence](OPS-01_BACKUP_RESTORE_20261117.json).

Branch `feat/ops-01-backup-restore` từ main `001e33b`, HoaiTam thực hiện,
reviewer unassigned. [Runbook](../runbooks/OPS-01_BACKUP_RESTORE.md),
[profile](../../config/ops/backup-smoke-v1.json).

## Bằng chứng đã đạt

- Backup04:13bảng/77objects/14artifacts, ZIP1.802.349bytes,
  SHA256 `60b44f775308ab9b54cc0d258774c4b22f42a202f9d19c516e5312424ff1def2`.
- Core restore03:77objects readback,13Trino full-row signatures +13rerun,
  ML joins1experiment/2memberships/1summary/0orphan candidates;
  Airflow schema71tables/531columns khớp; source pointers và5services giữ nguyên.
- Core data là component QA, ML synthetic; không phải experiment scientific
  trên official release DAT-03. Không suy test này thành toàn project Done.

## Final acceptance — backup04 / restore04

- Backup `60b44f775308ab9b54cc0d258774c4b22f42a202f9d19c516e5312424ff1def2`
  đọc lại đạt;14artifacts gồm2Bronze manifests, dataset/export registry,
  completion manifest/result/config/metrics và Airflow schema/evidence.
- Restore run `921393754413447caa47597eddf21954`: `RESTORE_VERIFIED`,
  completed `2026-11-16T18:11:44.773285+00:00` (17/11 tại Việt Nam).
  Hai namespace `ops01_921393754413447caa47597eddf21954_0/1`, database schema-only
  `ops01_921393754413447caa47597eddf21954`; không sửa namespace/DB cũ.
- 77objects gốc gồm24metadata JSON,15manifest lists,15Avro manifests,
  22Parquet và1Puffin. Graph2.043.481bytes, restored2.054.459bytes do remap
  URI/Avro lengths; **Parquet/Puffin bytes không đổi**. Mỗi upload được GET và
  SHA256 kiểm lại; graph mới khép kín, không mượn physical data/metadata nguồn.
- 13source SQL fingerprints +13restore +13rerun +4ML lineage joins, tổng
  **43TrinoSELECT trong final backup/restore pair**. Không tính GET metadata
  hoặc PG schema queries thành TrinoSQL. Các source/copy snapshot IDs và
  row checksum giữ nguyên; checksum Trino không phải SHA256 của rows.

| Nhóm bảng | Rows sau restore |
|---|---|
| Gold event_current / event_source_bridge | 11.034 / 11.034 |
| dim_date / dim_region / dim_depth_band / dim_magnitude_band | 33 / 1 / 5 / 7 |
| publication_status | 1 |
| candidate_event / mainshock_event / dataset_registry | 2 / 1 / 1 |
| experiment_run / sequence_membership / sequence_summary | 1 / 2 / 1 |

Dataset `ds_fixture`, experiment `exp_fixture`, completion/result/export hashes
khớp registry pins. Published-experiment join1, membership/run2, summary/run1,
orphan candidate0. Đây là **synthetic MLI-03 component**, không phải official
dataset/experiment mới; original lineage URI/IDs trong table data không rewrite.

- Airflow schema restore **71tables/531columns** khớp types/nullability/defaults;
  pg_dump schema-only, không copy rows/secrets/account records hoặc đổi metadata
  của scheduler hiện tại.12→14artifacts đều có local fresh readback/SHA sau restore.
- Source metadata pointers không đổi, hai lượt query bất biến;5service IDs/
  started_at/restart counts bằng trước phiên, healthy, lease empty và runtime idle.
  Không build/pull/start/restart/DAG/cron/ACL, không acquire/release lease.
- Configured-secret check và decoded JSON/text/Avro/Parquet/Puffin scan đạt;
  đây là coverage trên bounded backup, không chứng nhận cloud ACL của SEC-01.

## Test và tài liệu

- `make test-ops-backup OPS_PYTHON=staging/ops01-20261117/venv/bin/python`:
  **47/47PASS,0skip**, graph/SHA/ZIP member set, traversal/duplicate/symlink,
  byte/count caps, unsupported formats, int64 registry, original result manifests,
  URI relocation/partition paths, corrupt readback/no-auto-retry và secret scan.
- `python3 -B -m unittest discover -s tests/ci -p 'test_*.py'`: **43/43PASS**.
  CI docs-contracts cài requirementsOPS và chạy offline suite, không Docker.
- Link checker/task-status/whitespace và CFG repository/example hygiene được
  chạy lại sau cập nhật task/index. Không gọi repository check là fresh deployed
  SEC-01 audit; configured-secret backup scan sử dụng deployment `.env` trong memory.
- Venv riêng fastavro1.12.1/cramjam2.11.0, PyArrow21.0.0; không sửa môi trường
  model hoặc install package vào container. Review độc lập unassigned, không
  tự tạo approval. Sau bàn giao local, owner duyệt commit/push/tạo PR và
  merge17/11; chỉ merge sau khi checks đạt, không gọi đó là reviewer approval.

## Các lần dừng an toàn giữ lại

- Backup01: SQL alias `checksum(t)` không hợp lệ; chưa restore. Đổi sang
  `checksum(ROW(explicit_columns))`, SELECT probe thực tế đã đạt.
- Backup02: phát hiện Puffin stats ngoài profile ban đầu; chưa restore. Thêm
  kiểm footer/size và copy statistics bytes nguyên bản, không bỏ reference.
- Restore01: registry dùng decimal string trong khi table metadata dùng JSON
  int64; guard dừng trước writes. Thêm normalization không qua float và regression.
- Restore02: guard không nhận `=` của partition path; đã chép21objects vào
  prefix thử riêng,0table registration/0DB create. Đã đối chiếu state chỉ đọc,
  thêm exact run/prefix guard và kiểm toàn destination trước writes. Không
  resume/overwrite/xóa prefix cũ; restore03 dùng runUUID mới và đã đạt.

Raw backups, intents, receipts và các lần fail giữ trong ignored staging,
không commit data dump, credential hoặc private logs. Không ETL/build/pull/
restart/cron/ACL hoặc release lease để vượt gate.

## Bàn giao và giới hạn

- Backup/receipts ở `staging/ops01-20261117/backup-04/` và `restore-04/` của
  checkout này. Mode600/private; không commit ZIP, artifacts raw hoặc private logs.
  Không có upload mới hoặc đổi quyền Drive.
- Giữ cả partial21objects của restore02 và các run accepted03/04 để review;
  không cleanup/drop schema/DB/bucket/data. Không còn controller/job của phiên.
- [OPS-01](../task/tasks/OPS-01.md) chỉ đóng smoke đại diện; full warehouse,
  raw Bronze/Silver, catalog-volume/whole-Airflow-history disaster recovery
  **chưa có task riêng**. Không dùng bản nhỏ này làm lý do xóa nguồn/warehouse.
- [SEC-01](../task/tasks/SEC-01.md) còn cloud ACL/roster/session;
  [MLI-04](../task/tasks/MLI-04.md)/[MLQ-01](../task/tasks/MLQ-01.md) còn official
  ML/report/E2E acceptance, không được đóng từ synthetic component restore.
- DAT-03 closure đã commit/push `fb4e2ca` trên `docs/dat-03-final-acceptance`
  và merge [PR #136](https://github.com/HoaiTam/japan-earthquake-etl/pull/136)
  sau23/23checks PASS. [OPS-01 PR #137](https://github.com/HoaiTam/japan-earthquake-etl/pull/137)
  rebase theo main mới, giữ DAT-03 Done và cập nhật counts67Done/5Needs Update/
  9Backlog. Không chuyển các edits SEC-01 sang đây; chỉ merge khi CI của đúng
  OPS-01 head đạt, không bypass hoặc tự tạo reviewer approval.
