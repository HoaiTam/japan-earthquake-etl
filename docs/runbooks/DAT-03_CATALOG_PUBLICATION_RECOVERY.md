# DAT-03 — Phục hồi batch catalog đã commit

Áp dụng cho catalog có session **terminal nonzero, không timed out**, chưa có
result/acceptance, nhưng đã giữ context và bốn artifacts readiness/bronze/
silver/gold. Không dùng normal `resume` khi lease còn giữ hoặc session thiếu
acceptance. Không áp dụng cho partial publication/unknown child/timeout; các
trường hợp đó dừng để chẩn đoán riêng, không xóa evidence hoặc thử mù.

## Phục hồi những gì

`research-catalog-recovery.py` điều phối `ResearchCatalogRecoveryJob` riêng.
Không tạo SparkSession, parse/transform lại events, ghi Bronze/Silver hay commit
lại Gold. V1 giữ nguyên publisher/SQL client, chỉ thêm class recovery.
**V2** giữ publisher/manifest/parser/resolver/ML byte-identical và cho phép
thay đúng **TrinoSqlClient** theo profile `dat03-select-poll-300s-v1` có hash
riêng: SELECT của isolated research catalog được polling tối đa300s; DDL/DML
vẫn120s, mỗi HTTP request90s, không POST retry. Constructor mặc định của các
job khác giữ120s. Đây là thay đổi deadline polling có version, không bỏ quality
checks, tăng memory cap hoặc chứng nhận full-period capacity. Không đổi source cutoff,
namespace, context/code_version hoặc six committed snapshots.

1. Pin exact failed observation/session, original request, bốn artifacts,
   private log hashes, last accepted record/checkpoint và Bronze pins. Private
   log bytes chỉ giữ local, không vào Git. Original execution exit1 không đổi.
   V2 nhận tối đa2related failed recoveries có known nonzero/non-timeout,
   không result/acceptance và intent cùng primary failure; pin riêng cả4file
   của từng session, không bỏ qua phiên recovery failed trước đó.
2. Kiểm actual own lease, Java/DagRun/Trino idle, bốn required services healthy,
   host physical/volume reserve >=4GiB. Không tự start/restart/build/pull/init.
3. Read-only preflight kiểm frozen source bytes, Bronze manifests/raw,
   full-month registry/Silver bundle, first-four phase receipt SHA chain,
   parent publication và exact batch scope. Chạy 13 metadata SELECT thật:
   six current pins, six snapshot identities và publication row chưa tồn tại.
   Verify/publish artifacts hoặc publication journal đã có thì fail closed.
   V2 còn chạy read-only probe `primary_provenance:2` trên actual six pins
   (chính blocker đã lỗi); phải trả integral0 trước khi mở `run`. Metadata vẫn
   13checks; probe ghi riêng SQL/expected/actual/thời gian, không gọi14checks
   là13SQL hoặc dùng unit mock thay runtime probe.
4. `run` kiểm lại fresh preflight, gọi publisher verify rồi publish. Giữ toàn
   bộ blockers, 16-bucket ngoài phạm vi/duplicate proof và serving readbacks;
   giữ profile polling có version/memory cap và không auto-retry SQL. Tạo receipt đúng protocol
   ORC-07, kiểm journal readback và six current pins lần nữa.
5. Tạo record/checkpoint cho đúng contiguous target months, result trong session
   UUID **mới**. Worker ghi VALIDATED_LEASE_HELD; host kiểm service IDs/start/
   restart unchanged rồi mới finalizer kiểm lại proofs/quiescence và release
   own lease. Lỗi ở bất kỳ bước nào giữ artifacts/lease, không auto recovery.
6. Acceptance session mới tham chiếu failed session; thêm immutable
   `session-resolutions/<failed-session>.json`. Normal continuation mới chỉ
   nhận failed session qua resolution đã pin acceptance/result/execution/release
   thật và hashes của failed evidence còn nguyên. V2 liên kết cùng accepted
   result với danh sách primary/related failures và resolution riêng cho từng
   ID; continuation phải kiểm đúng toàn bộ pins, không rewrite old executions.
   Không gắn success vào session
   lỗi, không bỏ qua tất cả session thiếu acceptance.
7. Host chụp CAS observation, kiểm full progress rồi cập nhật latest accepted.
   Nếu capture lỗi, giữ descriptor/lock để chẩn đoán; không chạy lại publication.

## Lệnh thực tế

Build local bằng JDK17/21 trước, không build image hoặc chạy build trong test
runtime. Chạy unit/mock khi không có ETL active:

```bash
./mvnw -o --batch-mode --no-transfer-progress -pl spark -am -DskipTests package
make test-research-catalog-recovery
```

Lệnh prepare V2 đã thực hiện cho batch06–11/2009. Không chạy lại attempt đang
active hoặc attempt đã có execution; không dùng observation113 thiếu failure:

```bash
python3 -B scripts/research-catalog-recovery.py prepare --deployment-root /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl --catalog /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-catalog/b6db356fb30e46f9a26a419ebacb5393 --continuation /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-catalog-continuation/739e2ebafecd4da4a0a7b3cfd127b525 --observation /Users/hoaitam/Desktop/Learn/23521394/HK1-Nam-4/IE212/japan-earthquake-etl/staging/dat03-catalog/b6db356fb30e46f9a26a419ebacb5393/runtime-observation-e1cb0f4655a54a6c8cd7cbf4a7c32057 --failed-session 6af846903c964a46853536943d6e52ad --related-failed-session d94bfeca4c964205860709a6ec102b66
```

Prepare in ra attempt UUID mới. Truyền **đúng output đó** vào `--attempt` của
`preflight`, sau khi đạt mới dùng `run`, đều có cùng `--deployment-root` ở trên.
Hai mode không tự retry; không chạy lại attempt đã execution/partial mutation.
Không dùng worker/finalize trực tiếp để bỏ qua host service check.

Actual V2 attempt `99124e6d7af8477aa2e4a6b64aea9bf8`:13metadata checks và
probe58,637s/actual0 đã đạt; `run` đang verify/publish. Chưa accepted119 cho
đến khi result/release/acceptance/host CAS reporter đạt.
[Evidence V2](../evidence/DAT-03_CATALOG_VERIFY_RECOVERY_200906.md).

Lịch sử V1 attempt27/10: `231e61a839df4e5ca81477381af1bf6e`. Preflight đã đạt
13/13 trong19,991s, sau đó verify/publish/own release và fresh host capture đạt
[checkpoint107](../evidence/DAT-03_CATALOG_CHECKPOINT_107.json). Initial host
capture từng bị allowlist chặn sau runtime acceptance; đã chẩn đoán exact own
dead lock, lưu nguyên lock rồi recapture **chỉ đọc**, không replay publication.
[Evidence recovery](../evidence/DAT-03_CATALOG_RECOVERY_20261027.md) ghi đầy đủ
cả failure và kết quả. Không chạy lại attempt đã hoàn tất này.

## Giới hạn

**DAT-03 — Catalog nghiên cứu thật và handoff:** recovery của một batch không
thay full288-month coverage, official collection export/loader/rerun và ba máy
receipts. `research_ready=false`, `official_exported=false`, task In Progress.
**SEC-01 — Security review:** Drive recipient/identity acceptance còn thiếu,
không đổi ACL/upload hoặc suy cloud acceptance từ local recovery.
