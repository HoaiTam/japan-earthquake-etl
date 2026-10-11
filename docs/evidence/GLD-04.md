# GLD-04 — Evidence Trino verification và publication

## Baseline và kết quả

Branch `feat/gld-04-trino-publication` chứa main `5373cb8` và commit GLD-03
`35e5fb2` từ [PR #62](https://github.com/HoaiTam/japan-earthquake-etl/pull/62).
Không merge thêm main trong lần sửa này. Assignee Trang; reviewer unassigned.

[Receipt thật](./GLD-04-runtime.json) và [verification SQL](./GLD-04-verification.sql)
ghi exact bundle, expected/actual và các query chỉ đọc để lặp lại kiểm chứng.
Trino 483 đọc sáu snapshots qua REST Catalog/MinIO; không mock SQL trong runtime.
Fact/bridge có 16/16 rows, dimensions date/region/magnitude/depth có 3/1/7/5 rows.
37 numeric checks đạt, sáu schemas khớp contract; publication `PASSED`,
`published=true`, natural/ROI view trả 16 events.

Snapshot fact: `1901025108914282099`. Publication ID:
`pub_d03dad056f8ea9b4af290ddf92009042d123bf9db0512e549ca0e5e180449b9f`.
Namespace `iceberg.gold_qa_2d155a4ff190443bb76e2e186bb39e7e` được cách ly.
Rerun trong cùng job trả receipt không đổi; metadata chỉ có một PASSED row.
Trino CLI độc lập xác nhận PASSED row và count immutable natural/ROI view.

## Lỗi đã sửa

HTTP client chưa khai báo `PARAMETRIC_DATETIME`, nên Trino trả metadata legacy
`timestamp`/`timestamp with time zone` thay vì các kiểu có precision `(6)`.
Sau khi thêm `X-Trino-Client-Capabilities: PARAMETRIC_DATETIME` trên POST/GET,
39 cột fact khớp, gồm UTC/JST/record timestamp. Không bỏ precision hoặc timezone
khỏi schema gate; sai `(3)` hoặc timezone vẫn fail trước mọi CREATE/INSERT.
Tham khảo [Trino client protocol](https://trino.io/docs/current/develop/client-protocol.html).

## Test/check thực tế

- Maven `verify` với `-Dtest=GoldPublicationTest,TrinoSqlClientTest
  -Dsurefire.failIfNoSpecifiedTests=false`: 7 tests, 0 failures/errors/skipped,
  BUILD SUCCESS trên JDK 17.0.12. Gồm blocker trước mutation, schema precision/
  timezone sai, rerun không duplicate hoặc rewind alias, exact pins/outside scope,
  HTTP pagination/capability/error redaction và foreign nextUri rejection.
- `python3 -m unittest discover -s airflow/tests -p test_gold_publication_qa.py`:
  4 tests pass trong Linux QA UID 50000; success/release, JVM failure/timeout giữ
  lease, busy lease và non-QA reference không submit.
- `docker compose --env-file .env.gold-qa --profile gold config --quiet`: pass.
- `docker compose --env-file .env.gold-qa run --rm --no-deps gold-verification-qa`:
  pass, report của run `gld04-qa-33596d9decc9401ebeee8a52998e8f8b`.
  Env QA bị ignore, không commit. Receipt công khai không có private staging path.
- `check-config.sh --require-local`, `check-task-status` (73 tasks),
  `check-java-build-inputs`, `check-data-model-contract`: pass.
  Changed-file scan với 7 credential values QA không có leak; `git diff --check` pass.

Lease của lần schema mismatch trước được release đúng dag/run sau khi xác nhận
không còn Trino query hoạt động. Không xóa volume, bucket hoặc Iceberg metadata.

## Lặp lại và handoff

Foundation MinIO/Catalog/Trino cần healthy và staging đã init. Có thể chạy
`make test-gold-publication`, sau đó `make smoke-gold-publication` hoặc command
Compose bên trên. Default reference là receipt GLD-03 đã commit trong repo;
`GOLD_COMMIT_REFERENCE` cho phép chọn reference khác. Namespace phải là QA.
Reference chỉ dùng được trên warehouse còn giữ các snapshots đó; trên môi trường
mới, chạy GLD-03 smoke và dùng receipt của chính môi trường đó.

SQL cuối file evidence resolve PASSED publication cho MLD-01: physical table,
snapshot ID, logical serving table, full bundle và immutable view. Sau khi pin,
consumer đọc đúng physical snapshot cùng natural/ROI predicates; không resolve
latest lại và không coi view là bảng sở hữu snapshot.

## Giới hạn / Rủi ro / Bước tiếp theo

- [MLD-01 — Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md):
  còn triển khai dataset identity/materialization từ PASSED publication.
- [QA-01 — Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): còn production Gold
  orchestration adapter, full-month current state và multi-source coverage.
  Pilot này chỉ 16 events từ public USGS capture, không chứng minh JMA/toàn tháng/
  toàn research period hoặc readiness của dataset nghiên cứu.
- [MLD-02 — Audit Gold và estimate Mc](../task/tasks/MLD-02.md): cần coverage/Mc/
  exclusion audit trước candidate/feature; publication không thay audit này.
- Sáu Iceberg commit không atomic cùng nhau. Consumer join dùng versioned views
  từ exact Published bundle. Stable alias đổi sau PASSED; rerun lịch sử không
  chuyển alias ngược. Trino client hiện dùng Compose network không authentication.
- Chưa có reviewer độc lập; không ghi tên/approval chưa tồn tại.
