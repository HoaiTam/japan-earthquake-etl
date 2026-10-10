# GLD-01 — Gold transformation

## Giao diện và output

`GoldEventTransformer.transform` nhận bốn Spark DataFrames: Silver observations,
canonical membership CON-03, source links (ít nhất left/right observation IDs và
decision), và optional region classification. Region dataset rỗng là hợp lệ;
output giữ `UNKNOWN`. `GoldRunContext` giữ run/window/processing date/backfill,
config version và exact input manifest references. Timestamp cập nhật do caller
truyền để rerun fixture có tính xác định; Spark session timezone phải là UTC.

Transformer dùng current observations và PRIMARY đã được SLV-07 chốt. Không
dedup/link hoặc tạo canonical ID lại. Thiếu membership, ID trùng, nhiều primary,
version lệch hoặc nhiều current observation cùng source trong một canonical
event đều fail closed. Input required fields/coordinates/finite values và region
classification được kiểm tra trước khi tạo Gold.

Outputs theo [CON-03](./SILVER_GOLD_DATA_MODEL.md):

- `eventCurrent`: `gold.event_current`, một row/canonical ID.
- `earthquakeEventCurrent`: natural earthquake trong study area, serving view logic.
- `eventSourceBridge`: một row/current observation, đủ source/Bronze provenance.
- `dimDate`, `dimRegion`, `dimMagnitudeBand`, `dimDepthBand`.
- Counts current observation/canonical/bridge. Bridge count bằng current count;
  canonical count bằng số membership group, không count fact sau bridge join.

Hypocenter/magnitude/depth lấy từ PRIMARY, không coalesce magnitude null từ
supporting source. Alert/significance lấy từ accepted USGS; intensity/agency/era
lấy từ accepted JMA. Tsunami true thắng false, tất cả unknown giữ null. Warning
của supporting source được giữ ở canonical quality. Ambiguous source-link members
giữ event riêng theo membership và có `link_status=AMBIGUOUS`.

Band mapping dùng half-open intervals đúng CON-03, null → UNKNOWN; negative depth
→ NEGATIVE. UTC/JST date dimensions giữ cả hai ngày khi event vượt biên JST.
Không có spatial boundary trong task; không suy ra OFFSHORE chỉ vì thiếu boundary.
Caller chỉ cung cấp region classification đã biết; fallback UNKNOWN giữ event.

## Kiểm thử lặp lại

Từ repo root qua Bash/Git Bash (Java 17):

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=GoldEventTransformerTest -Dsurefire.failIfNoSpecifiedTests=false test
./mvnw --batch-mode --no-transfer-progress -pl spark -am test
./scripts/check-task-status.sh
git diff --check
```

Fixtures trong Java test nhỏ/synthetic/offline: linked pair có primary magnitude
null và supporting USGS, ambiguous/non-natural/Offshore, biên band, empty input,
invalid/missing/duplicate membership, history/current revision và invalid latitude.
Spark local thực thi transformations; đây không phải Spark standalone/MinIO smoke.

Test JVM có module exports Java17 và Jackson Scala test dependency cùng version
SDK Jackson để tránh xung đột với Scala module của Spark. Dependency này chỉ scope
test; production Spark dùng bundled Jackson, SDK tiếp tục được relocate trong
runner JAR theo ORC-05. Không đổi Spark classloader hoặc downgrade SDK Jackson.

## Giới hạn và handoff

- [SLV-07 — Liên kết observation và chọn canonical event](../task/tasks/SLV-07.md):
  cung cấp membership/PRIMARY/model version và source-link evidence thật; GLD-01
  tin policy chọn PRIMARY của producer, chỉ kiểm tra cấu trúc/cardinality.
- [SLV-09 — Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  còn cần SilverReady exact dataset manifests/readback/reconciliation thật.
- [GLD-03 — Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md):
  persist output/affected scope, rerun/partial commit và snapshot readback thật.
- [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  verify committed snapshot bundle và publication; transformation không ghi
  publication, không nhận Gold Published và không thực hiện lake writes.
- [GLD-02 — Tạo aggregate phục vụ dashboard](../task/tasks/GLD-02.md):
  dashboard aggregate là Stretch, không nằm trong output task này.
