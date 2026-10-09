# GLD-04 — SQL plan và fixture harness

**In Progress; chỉ phần chuẩn bị được task cho phép. Chưa có live Trino adapter.**

`GoldVerificationPlan` nhận exact committed snapshots/expected counts, tạo SQL
readback cho mỗi bảng, canonical/bridge unique, required fields/coordinates,
source coverage/JMA era, bridge orphan/source count/PRIMARY provenance và natural
ROI count. Catalog/table identifier chỉ nhận allowlisted shape; snapshot phải là
positive long. Không dùng latest/current để verify một snapshot ID khác.

Time travel dùng `FOR VERSION AS OF` theo [Trino Iceberg documentation](https://trino.io/docs/current/connector/iceberg.html#time-travel-queries).
SQL là draft chưa được execute/parse trên Trino thật. `outsideScopeSql` so sánh full
event rows với baseline snapshot theo affected months; writer phải cung cấp thêm
outside-scope evidence cho bridge/dimensions trong real verification.

Serving view natural/ROI đọc current table khi consumer yêu cầu current. View
không có Iceberg snapshot riêng: MLD-01 phải pin physical `gold.event_current`
snapshot trong publication bundle và áp dụng đúng natural/ROI predicate. Chưa đổi
logical table name `gold.earthquake_event_current` của CON-03. Physical mapping/
publication identity cần GLD-03/04 thống nhất trước runtime.

Fixture harness yêu cầu exact snapshot/count bundle và đủ sáu checks ORC-01.
Output luôn engine `MOCK_TRINO`, `canPublish=false`, dù fixturePassed=true.
Không ghi `gold.publication_status` hoặc cấp real Published receipt từ fixture.

## Tests

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=GoldVerificationPlanTest -Dsurefire.failIfNoSpecifiedTests=false test
./scripts/check-task-status.sh
git diff --check
```

Cases: positive fixture không publish, từng blocker false, missing check,
stale snapshot/count mismatch, SQL identifier injection, missing/pinned table,
invalid affected month và empty dataset complete bundle.

## Runtime gate còn thiếu

- [GLD-03 — Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md):
  chưa có real committed snapshot bundle/DDL/expected counts/baseline scope.
- GLD-04 cần triển khai Trino executor, execute SQL/expected outputs thật,
  schema/type check (không chỉ kiểm tra null), supporting field provenance,
  timestamp/freshness và outside-scope checks cho mọi bảng bắt buộc.
- GLD-04 cần persist exact verification report và idempotent publication metadata
  sau all blockers passed; partial commit/verify failure phải chặn Published.
- [MLD-01 — Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md):
  nhận publication/table-view mapping/coverage thật, không nhận MOCK_TRINO report.
- [SLV-09 — Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  SilverReady input thật cho Gold integration. `make smoke-query` chỉ chứng minh
  hạ tầng QRY-01, không thay Gold verification.

Reviewer unassigned; .env/Docker chưa sẵn sàng. Giữ task In Progress cho tới khi
hard dependency và integration acceptance thật đạt.
