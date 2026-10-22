# QA-01 — Gold/publication component, 22/10/2026

## Phạm vi và identity

- Tiếp tục branch `test/qa-01-scheduled-runtime-acceptance` của
  [PR #96](https://github.com/HoaiTam/japan-earthquake-etl/pull/96), xác nhận
  remote head `7bf0a476cb13de2ca48a4f6173eb5b4d1b95f8fa` trước sửa.
- Theo yêu cầu owner: **bước 2 Gold và publication**, sau
  [Source/Silver](./QA-01_SOURCE_SILVER.md); không mở branch/PR mới, không tự
  commit/push/merge. HoaiTam giữ owner, reviewer unassigned.
- Component protocol `current-month-gold-v1`, reserved schema
  `operational_current_month`; journal dưới `silver/current-month/`.

## Đã triển khai

1. `CurrentMonthGold` reverify exact source/Silver state, pin registry/receipt,
   code/request, hai cutoff, observed coverage và prior published six snapshots.
   Parent Silver/Gold phải cùng chain; bootstrap không dùng current/latest.
2. Gold input đọc cumulative state nhưng lọc affected months **trước** GLD-01.
   Đối soát current/source index và bridge; giữ tháng ngoài scope.
3. Reuse GLD-03 writer/resume; operational policy có trong identity, commit,
   table properties và SHA snapshot summary. Legacy writer/publisher không
   được ghi/publish reserved operational namespace.
4. Trino path query exact six snapshots và các blockers GLD-04, thêm snapshot
   policy/cutoff/USGS-only/coverage. Chỉ sau verification/freshness mới ghi
   status/views/alias operational. Status, serving views và receipt ghi rõ
   PROVISIONAL, OBSERVED_AS_OF, research/ML false; không chạm historical aliases.
5. Guard MLD-01 manifest/identity/pinned readers, audit/windows/export reject
   operational namespace; persisted snapshot policy query ngăn metadata đổi
   tên/forged eligibility bypass. Không đổi lifecycle official/pilot.
6. `CurrentMonthGoldJob` explicit MinIO/REST/Trino có exact held-lease check;
   capture fetcher bị khóa, không HTTP refetch. Make/host wrapper dùng prepared
   JAR, private logs, bounded readback acknowledgement; không Docker mutation.

Chi tiết schema, commands và recovery: [runbook](../runbooks/CURRENT_MONTH_GOLD_PUBLICATION.md).

## Tests đã chạy

JDK21 `21.0.11`, compiler target Java17; Spark `3.5.9`. Fixture HTTP, local
Parquet/Iceberg thật và scripted SQL adapter; **không phải live Trino proof**.

- `make test-current-month-gold`: **26 Java tests, 0 failure/error/skip**, cùng
  **8 host CLI tests**. Gồm 3 current-month pipeline, 5 policy/publication/ML
  guards, 5 Iceberg writer/recovery, 5 legacy publication và 8 dataset manifest.
- Pipeline test: bootstrap tháng 9 → tháng 10, revision, giữ row/run ID tháng
  ngoài affected scope, exact old snapshot readback; rerun cũ không mutate alias.
- Negative/recovery: SHA/namespace/unknown field/foreign lease fail trước Gold;
  SQL blocker không tạo publication/marker; retry reuse cùng six snapshots;
  partial per-table commit không recommit; policy thay đổi bị chặn; metadata
  PASSED/eligibility giả và snapshot đã đổi tên không ghi được ML manifest.
- CLI: prepared Java only, không source capture/Docker/build trong Gold mode;
  exit 0 thiếu readback hoặc operation/quality/eligibility sai vẫn không success;
  logs 0600, không cleanup artifacts khi failure.

- `./mvnw -pl spark -am clean verify`: **463 tests, 0 failure/error/skip**, JAR
  build đạt. Sau read-only Trino probe, predicate missing-key được tinh chỉnh
  sang `element_at`; lần `./mvnw -pl spark -am verify` cuối cũng đạt **463 tests,
  0 failure/error/skip**, BUILD SUCCESS (3 phút 57 giây).
- `make test-qa-01`: **52 tests** (28 collector + 11 planner + 5 CLI QA + 8 CLI
  component); `make ci-airflow`: **428 tests** đạt.
- `make ci-docs-base`: đạt docs/contracts, CLI/security/Docker static/runtime
  guards và CI inventory checks. Không gọi nguồn thật/start service.
- `make prepare-current-month`: BUILD SUCCESS; prepared JAR/classpath/dependency
  fingerprints được verify lại sau build. Không dùng artifact Source/Silver cũ.

Read-only SQL probe trên **Trino 483 thật** của stack đang chạy:

```sql
SELECT version(),
  element_at(MAP(ARRAY['gold.operation_id'],ARRAY['legacy-run']),
             'gold.operational_policy_sha256') IS NULL AS legacy_allowed,
  element_at(MAP(ARRAY['gold.operational_policy_sha256'],ARRAY['classified']),
             'gold.operational_policy_sha256') IS NULL AS provisional_allowed;
```

Kết quả: `483, true, false`. Đây chỉ là kiểm chứng semantics của research
predicate khi map thiếu/có key, **không** phải six-table publication hoặc
MinIO input acceptance. Dùng `element_at`, vì [Trino map subscript](https://trino.io/docs/current/functions/map.html)
báo lỗi khi key không tồn tại; không làm legacy snapshots thiếu key mới lỗi SQL.

## Runtime thực tế và phần chưa chứng minh

Read-only `docker ps` xác nhận MinIO/Trino/REST Catalog đang healthy; Airflow
không chạy trong lần kiểm tra. Không start/pull/build/reset service hoặc xóa
volumes/objects. Silver của live source test bước 1 vẫn là **file-backed local**,
không có MinIO handoff của protocol mới; không copy/sửa URI/manifests thành
evidence giả để chạy qua SQL S3 lineage gate.

Chưa có live MinIO Source/Silver → Iceberg → Trino publication cho component
này; chưa có DagRun/TaskInstance scheduled 2026. Fake SQL test pass không chứng
minh deployment, timestamp type, S3 lineage hoặc SQL runtime của service thật.
Mọi count trong pipeline tests là fixture, không phải thống kê live catalog.

## Trạng thái và phần tiếp theo

**QA-01 — Daily E2E vẫn In Progress.** Bước 2 implementation có code/tests;
MinIO/Trino live component acceptance, bước 3 Airflow wiring/collector v2/JMA
historical readiness và scheduled acceptance thật còn thiếu. Không giả owner
lease, unpause/trigger/clear DAG, gắn research-ready hoặc reviewer approval.

**QA-02 / QA-04:** fixture retry/failure mới không thay runtime acceptance cho
operational scheduled chain. **ORC-06/07, GLD-03/04, MLD-01/05** v1 không bị
reopen hay coi là đã deploy v2 chỉ vì thêm component này. Work packages v2,
JMA sau baseline và automatic deletion/closure/compaction **chưa có task ID riêng**.
