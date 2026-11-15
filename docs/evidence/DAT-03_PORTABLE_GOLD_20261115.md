# DAT-03 — Gold từ PREPARED, 15/11/2026

## Trạng thái thực tế

Lượt đầu **Published** trên `iceberg.dat03_unified_20261114`: actual Spark commit,
Trino verify 139 SQL calls và publish/post-readbacks 41 SQL calls đều đạt.
Rerun độc lập cũng đạt **139 Verify + 15 publication reuse/post-readbacks**,
cùng SHA commit/publication, sáu snapshot IDs và history 1 snapshot/table.
**Phần 2 hoàn tất kỹ thuật.** DAT-03 vẫn **In Progress** do official exports và
receipts ba máy chưa đủ; reviewer `unassigned`, không tự ghi scientific approval.

[Pins và metadata](DAT-03_PORTABLE_GOLD_20261115.json) là bản tóm tắt public;
full result/execution/acceptance và private logs giữ trong scoped staging.
Không commit payload, runner JAR, credential hoặc private logs.

## Input và phạm vi ghi

- Dùng [phần 1 đã nghiệm thu](DAT-03_PORTABLE_IMPORT_20261115.md): 288 tháng,
  27 checkpoints; catalog SHA `4e9282c35af384779fc5977c43b740e81a7185c9a9755b2cb117a2fbed75f8e8`.
- Cutoff gốc `2026-10-25T06:13:04.747Z`; không tải lại/replay ETL nguồn.
- Request `6840c54e4b1d9712b5deb9d933e82349bcfc76d6bc2c59d0c26d0b9e71545c82`,
  operation `0a84fa1310ca493bb5da7684d5fbab5a`.
- Chỉ namespace mới được duyệt, Gold journals/request anchor của đúng run và
  staging riêng. Không sửa namespace/source pins cũ, cron, deployed DAG runner,
  credential/ACL hoặc build/pull/recreate/start thêm service.
- Context Gold run/ETL timestamp được chuẩn hóa theo immutable request; source
  observation values, revisions và lineage giữ nguyên. Không fake SilverReady,
  FullMonthReady, registry hoặc legacy 288 checkpoints.

## Kết quả sáu bảng

| Bảng | Rows | Snapshot ID chính xác |
|---|---:|---|
| event_current | 4.488.650 | `7580315846938041321` |
| event_source_bridge | 4.489.823 | `7372656386804280438` |
| dim_date | 8.767 | `1582943291900582080` |
| dim_region | 1 | `4944154528209845868` |
| dim_magnitude_band | 7 | `7485466677857380137` |
| dim_depth_band | 5 | `6537142432516406541` |

Serving view natural earthquakes + study area có **4.476.586** rows. Chênh lệch
12.064 so với core canonical không phải dữ liệu bị xóa: core vẫn giữ những event
không thỏa đồng thời hai điều kiện đó. Bridge nhiều hơn canonical 1.173 rows,
phù hợp accepted source links/reconciliation phần 1 và actual Trino checks.

## Lỗi đã chẩn đoán và recovery

Phiên `bf26dd8fe0b642cc843c485bdb40f012` ghi hai fact rồi dừng tại dimension:
Spark USING join đưa INT key lên đầu; EXCEPT theo vị trí gặp DATE/INT. Không retry
mù, release lease bằng tay hoặc thay input. Giữ request/runner/failure evidence.

Sau 10 ca Spark/Iceberg Java 17 thật trên filesystem riêng (không S3 writes),
continuation hẹp `0be2ecb916914800b481548d0116802b` đã đạt bytecode gate:
chỉ writer/entry adapter đổi, validator là class thêm duy nhất. Mọi ETL class
khác byte-identical. Căn projection trước EXCEPT, không đổi incoming schema,
fingerprint hoặc identity. Manifest riêng pin old/new code, exact partial
snapshots và original identity raw SHA.

Recovery `d66532590f84465b996d0b4567bcacb9` giữ nguyên hai fact IDs, ghi bốn
dimension còn thiếu, full-row Spark fingerprints/readbacks đạt trước Trino.
Commit SHA `832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4`;
publication SHA `59cf8805bfd537e6eb5ad449868ad3b6d4003b535ad85a896eddcb5a24b914e8`.
Resolution liên kết failed execution với accepted recovery; **không sửa phiên lỗi
thành success**. Own source lease release sau capture; services unchanged/idle.

## Thực nghiệm và giới hạn

- Preflight read-only: 2 SQL calls, namespace ban đầu rỗng, source/input pins đạt.
- 36 pure Java tests; 61 Python metadata/control-plane tests đạt.
- Actual Java 17 Spark/Iceberg local fixture: 10 cases đạt, gồm partial commit,
  DATE-before-INT dimension, immutable rerun, conflict và legacy compatibility;
  không ghi S3, services không đổi.
- Actual commit recovery: 825,023 giây; cả phiên tới publication: 2.555,570 giây.
  local[1]/768 MiB driver, facts DISK_ONLY; Spark dừng trước Trino để giảm RAM peak.
- Actual rerun: Spark readback 513,210 giây, cả phiên 1.484,250 giây. Hai phase
  `reused=true`, không thêm Gold snapshot; raw commit/publication SHA không đổi.
  Hai lượt có 334 SQL calls (không tính 2 preflight), gồm publish DDL/DML lần đầu,
  không gọi toàn bộ số này là SELECT. Hai lượt Verify riêng đã có 278 SELECT calls.
- Trino dùng đủ 16 bucket cho fact blockers, không sampling; measured bound
  4.527.126 rows giữ nguyên. Snapshot/ref/history, quality, lineage/source counts,
  primary, temporal/dimension/aggregate và scoped outside checks đều đạt.
- Host reserve 4 GiB được theo dõi; services chạy sẵn, không build/restart.

Rerun dùng original pinned runner + request và accepted commit/publication pins;
chỉ readback/reuse, journal put/delete/move và non-SELECT publication SQL bị chặn.
Candidate/result/execution/acceptance SHA chain được đối chiếu độc lập cả hai
phiên; exact six pins/counts, failure-resolution, exit0/no timeout đều đạt.
Own source lease trống, không active Java/DagRun/Trino query, services unchanged
sau nghiệm thu. Không mở thêm ETL hoặc official ML export trong phần 2.

Phần tiếp theo thuộc [DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao
dataset dùng chung](../task/tasks/DAT-03.md): official adapter cho
**REPRODUCTION/EXTENSION EXPORTED** (tích hợp MLD-05/MLI-02), all-shard
loader/rerun, immutable release và receipt thật trên ba máy. Gold Published không
đồng nghĩa official dataset/experiment ready hoặc scientific approval.

## Cách kiểm tra lại

Xem [contract](../specs/PREPARED_GOLD_PUBLICATION.md) và
[runbook](../runbooks/DAT-03_PORTABLE_GOLD.md). Operation host nằm tại
`staging/dat03-portable-gold/0a84fa1310ca493bb5da7684d5fbab5a/`;
runtime tương ứng dưới `/opt/pipeline/staging/dat03-portable-gold/`.
Root assets giữ immutable; continuation nằm riêng dưới `continuations/`.
Không gọi `run` lần hai hoặc tái tạo operation để né checkpoint lỗi.

## Git bàn giao

Các lệnh dùng để bàn giao trên local branch `feat/dat-03-gold-publication` và
cập nhật **nhánh hiện có của PR #131**, không mở PR mới hoặc merge. Sau khi agent
đã commit/push thành công thì không cần chạy lại phần commit/push dưới đây.

```bash
cd /Users/hoaitam/.codex/worktrees/task-status-replan/japan-earthquake-etl-col-02
git diff --check
git diff --stat
git add -- Makefile \
  docs/specs/GOLD_ICEBERG_COMMIT.md docs/specs/GOLD_TRINO_PUBLICATION.md \
  docs/specs/PREPARED_GOLD_PUBLICATION.md docs/runbooks/DAT-03_PORTABLE_GOLD.md \
  docs/evidence/DAT-03_PORTABLE_GOLD_20261115.md docs/evidence/DAT-03_PORTABLE_GOLD_20261115.json \
  docs/task/tasks/DAT-03.md docs/task/tasks/README.md \
  scripts/dat03-portable-gold.py scripts/dat03-portable-gold-recovery.py \
  spark/src/main/java/ie212/earthquake/spark/gold/GoldCommitScope.java \
  spark/src/main/java/ie212/earthquake/spark/gold/GoldIcebergWriter.java \
  spark/src/main/java/ie212/earthquake/spark/gold/GoldPublisher.java \
  spark/src/main/java/ie212/earthquake/spark/gold/PortablePreparedCatalog.java \
  spark/src/main/java/ie212/earthquake/spark/gold/PortableGoldPublicationJob.java \
  spark/src/main/java/ie212/earthquake/spark/gold/PreparedGoldContinuation.java \
  spark/src/main/java/ie212/earthquake/spark/gold/PreparedGoldProof.java \
  spark/src/test/java/ie212/earthquake/spark/gold/PreparedGoldContinuationTest.java \
  spark/src/test/java/ie212/earthquake/spark/gold/PreparedGoldProofTest.java \
  spark/src/test/java/ie212/earthquake/spark/gold/PreparedGoldWriterFixtureJob.java \
  spark/src/test/java/ie212/earthquake/spark/gold/PreparedGoldWriterTest.java \
  tests/test_dat03_portable_gold.py
git diff --cached --check
git commit -m "feat(gold): publish prepared research snapshots with pinned rerun"
git push origin HEAD:feat/dat-03-payload-reuse-inventory
```
