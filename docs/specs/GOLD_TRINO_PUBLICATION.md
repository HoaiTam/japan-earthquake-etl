# GLD-04 — Trino verification và publication

## Exact input

`GoldVerificationJob` nhận request JSON: `run_id`, `dag_id`, `gold_run_id`,
`expected_namespace`, `expected_identity_sha256`. Caller phải giữ whole-run source
lease cùng dag/run; job đọc exact `gold-commits/<gold_run_id>/commit.json` từ MinIO.
Không nhận snapshot ID do người gọi tự gắn cho kết quả query latest.

`GoldVerificationPlan` kiểm tra namespace, six-table bundle, positive snapshot IDs,
count và schema. `TrinoSqlClient` dùng statement HTTP protocol, UTC session, bounded
pages/rows/response/deadline; không in SQL/server error payload. `nextUri` phải cùng
origin và statement endpoint. Endpoint lấy `TRINO_HOST`/`TRINO_INTERNAL_PORT`.
Client khai báo `X-Trino-Client-Capabilities: PARAMETRIC_DATETIME` trên POST và GET
để metadata timestamp giữ precision. Schema vẫn so sánh chính xác `(6)` và timezone;
không bỏ precision hoặc ép kiểu để cho qua gate. Xem [Trino client protocol](https://trino.io/docs/current/develop/client-protocol.html).
Baseline local Trino không có authentication, chỉ network Compose/loopback đã duyệt.

## Blockers

Mọi data query dùng `FOR VERSION AS OF <literal snapshot_id>`:

- Sáu bảng đọc được; columns/types khớp commit schema; count khớp receipt.
- Snapshot summary giữ exact Gold operation/identity; `$refs` main còn cùng snapshot
  khi publication mới. Không công bố một snapshot đã bị run khác thay thế âm thầm.
- Canonical/bridge/dimension keys unique và không null; field bắt buộc/schema version/
  coordinate/finite numeric/quality/source/coverage/era hợp lệ.
- Bridge không orphan; source counts/primary/source flags/provenance nhất quán,
  S3 raw URI và Bronze manifest lineage còn giữ, JMA có catalog release.
- UTC/JST/date/date-key và JMA primary era đúng contract.
- Dimension joins không mất event, labels/date khớp; canonical sum source_count bằng
  bridge count. Region UNKNOWN được giữ.
- Rows của run mới nằm trong affected months; symmetric EXCEPT của fact/bridge ngoài
  scope so với exact baseline phải bằng zero. Baseline 0 là bảng chưa có snapshot.

SQL và actual/expected values được ghi trong publication receipt. Query error/schema
mismatch/blocker dừng trước tạo view/publication metadata. Gate này không thay audit
research coverage/Mc/candidate của MLD-02/03.

## Visibility và metadata

`publication_id = pub_<SHA256 exact commit bytes>`; rerun không dùng ID theo clock.
Sáu views `v_<bundle SHA prefix>_<table>` cố định vào từng snapshot. Natural/ROI view
`v_<prefix>_earthquake_event_current` đọc fact view đó. Views chỉ có rows khi
`publication_status` chứa đúng publication ID với PASSED.

Sau toàn bộ blockers/freshness, ghi **một row atomic** vào Iceberg
`<namespace>.publication_status` bằng Trino. Columns CON-03: Gold run/snapshot,
committed/verified/published UTC timestamps, max_event_time_utc, natural/ROI
event_count, schema_version, verify_status. Metadata bổ sung gồm publication ID,
logical serving/physical event table, exact six-table JSON, bundle SHA, scope JSON
và immutable serving view. MLD-01 dùng physical table + snapshot để time travel,
và cùng natural/ROI predicates; view không tự sở hữu Iceberg snapshot.

Stable alias `earthquake_event_current` đổi bằng một CREATE OR REPLACE VIEW **sau**
khi immutable bundle và PASSED row tồn tại. Consumer join nhiều bảng phải dùng
exact versioned views từ bundle, không join những current tables chưa Published.
Nếu alias update lỗi, metadata mới vẫn pin được full verified bundle; alias cũ
còn đọc publication cũ. Rerun đã có journal không chuyển alias ngược về snapshot cũ.

Journal `gold-verification/<gold_run_id>/publication.json` ghi sau readback status
row, immutable natural view và alias count. Rerun tiếp tục verify exact data/schema/
blockers và PASSED row trước trả receipt. Nếu commit/alias/receipt outcome không rõ,
giữ lease và kiểm tra exact Trino statement trước recovery; không tự delete metadata.

## Commands

`make test-gold-publication` chạy unit fixtures/HTTP protocol/control tests.
`make smoke-gold-publication` chạy exact public GLD-03 reference (default
`docs/evidence/GLD-03-runtime.json`) trên foundation đã healthy, staging đã init.
Reference phải trỏ vào snapshots còn tồn tại trong warehouse của môi trường đang
chạy; trên môi trường mới cần GLD-03 smoke trước và dùng receipt mới của nó.
Đặt `GOLD_COMMIT_REFERENCE` thành absolute path của reference JSON khác khi cần.
QA từ chối namespace sản phẩm; chỉ `gold_qa_<hex>` được phép. Direct Compose:

    docker compose --env-file .env run --rm --no-deps gold-verification-qa

QA control plane chỉ gọi Java; không xử lý dữ liệu ETL bằng Python. Bất kỳ nonzero,
timeout hoặc interruption giữ owner vì query Trino remote có thể còn chạy.
Sau khi operator xác nhận không còn statement hoạt động và identity vẫn đúng,
có thể dùng source_run_guard với owner cụ thể để release; không xóa owner của run khác.

Runtime acceptance và SQL resolve PASSED publication cho MLD-01:
[evidence GLD-04](../evidence/GLD-04.md), [receipt](../evidence/GLD-04-runtime.json)
và [queries chỉ đọc](../evidence/GLD-04-verification.sql).

## Giới hạn / Bước tiếp theo

- [MLD-01 — Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md):
  tiêu thụ exact PASSED row; không resolve current sau pin.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): production DAG adapter,
  full-month state và multi-source coverage; QA namespace không đại diện research period.
- [MLD-02 — Audit Gold và estimate Mc](../task/tasks/MLD-02.md): coverage/Mc/exclusion
  audit trước candidate/feature. Publication của một pilot không là toàn lịch sử đã ready.

Tham khảo [Trino Iceberg snapshots/views](https://trino.io/docs/current/connector/iceberg.html).
