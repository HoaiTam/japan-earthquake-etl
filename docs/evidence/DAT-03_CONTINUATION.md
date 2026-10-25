# DAT-03 — Khối 2 bounded recovery đạt (25/10/2026)

Sửa tiếp [PR #100](https://github.com/HoaiTam/japan-earthquake-etl/pull/100),
branch fix/dat-03-trino-recovery từ1910de8 theo yêu cầu owner; không mở PR/branch
khác, không merge. Assignee HoaiTam, reviewer unassigned. **Khối 2 PASSED;
DAT-03 vẫn In Progress.** [Metadata/SHA](./DAT-03_CONTINUATION.json),
[cơ chế và lệnh](../runbooks/DAT-03_CONTINUATION.md).

## Diễn tiến, không viết lại failure

1. Original run dat03-8ca2be1939db4111ae783f6566650fce ngày24/10 vẫn exit1;
   [partial receipt](./DAT-03_BOUNDED_E2E_PARTIAL.json) không sửa. Không đủ evidence
   để kết luận root cause restart gốc24/10.
2. Continuation9ff8e2bee9454ad69f63896dd8ebcc4c đã verify/publish/rerun2016-04,
   rồi chạy2023-12 và commit sáu Gold pins. Verify2023 bị mất response khi Trino
   OOM-kill exit137, Docker event `oom` timeNano1792893635539315510 được đọc live.
   Đây là quan sát CLI đã ghi metadata, không raw signed receipt. Event history
   rolling đã mất sự kiện khi export sau đó; không nói file export còn proof này.
   Attempt giữ exit1, wall438,685s/RSS1.307.340KiB và own lease; không tự retry.
3. Owner duyệt giảm memory budget, lưu retained stdout/stderr/access log archives
   và config vào deployment staging/dat03-trino-memory-HhIfRj3h (private, không
   commit). Recreate **chỉ Trino**, cached image483 cùng digest; no build/pull,
   Airflow/MinIO/REST giữ ID/StartedAt. Container mới heap1536/direct512MiB,
   query512/total768/headroom512MB trong limit3GiB.
4. Checkpoint ba46d523846e47e48036f5558c6e3ea4 pin18 metadata files của failed
   continuation, original39 files/captures và code compatibility. Fresh13-query
   preflight đúng2023 commit/own lease, source pins/cutoff/parent chains khớp.
   Chỉ execute verify/publish2023-12 rồi rerun; không replay Bronze/Silver/Gold.
5. Java exit0 lúc02:18:27Z; report đủ4cases/receipt SHA, retention và six pins.
   Worker/finalizer kiểm toàn bộ bằng chứng, idle và service state rồi **release
   own lease**. Offline verifier đạt; độc lập Trino CLI đọc đủ4publication PASSED.
   Không còn Java/DagRun active; owner.json không còn. Không tạo run/DAG mới.

## Kết quả

| Tháng đã thêm | Canonical cộng dồn | Source bridge cộng dồn | Serving natural/ROI cộng dồn | Reported checks | Rerun/retention |
|---|---:|---:|---:|---:|---|
| 2000-01 | 5.418 | 5.418 | 5.351 | 36 | Đạt |
| 2011-04 | 62.461 | 62.461 | 62.371 | 36 | Đạt |
| 2016-04 | 105.282 | 105.282 | 105.180 | 36 | Đạt |
| 2023-12 | 124.969 | 125.069 | 124.856 | 36 | Đạt |

36 là checks lưu trong mỗi publication, không nhân thành36 fresh SQL mỗi lần
reuse/rerun. Publisher vẫn chạy schema/blocker SQL trên exact snapshots; các
publication cũ được kiểm receipt/live journal, không publish lại hoặc rollback
serving alias. Stable serving cuối124.856 có predicate is_natural_earthquake AND
is_in_study_area;113 canonical còn lại vẫn được giữ trong Gold core.

2023-12: selected revisions19.787 = h2023:19.426 + boundary h2024:186 + USGS:175;
reconciliation balanced,0 rejects,19.687 canonical mới. Boundary h2024 phục vụ
tháng12 UTC, không mở rộng ML split sang2024. Có168 link candidates, không gọi
đó là168 canonical matches; bridge/canonical giữ lineage riêng.

Các pins2016 giữ nguyên;2023 verify/publish/rerun dùng đúng six commit pins trong
metadata evidence. Source cutoff vẫn2026-10-24T14:31:46.536Z, không recapture,
không latest, không thay context code_version. JAR recovery pin riêng;13.668
original entries giống byte, chỉ TrinoSqlClient.class đúng patch khối1 khác;
report không giả recovery binary là original JAR.

## Resource và kiểm thử

- Checkpoint Java wall300,642s, child peak RSS870.200KiB; local[1], heap768MiB/
  direct256MiB cho Spark. Không là timing của full4case ETL từ đầu.
- Shared volume free183.964.467.200→183.957.348.352 bytes, không exclusive
  lake growth hoặc ETA full catalog. Trino được quan sát~1,69GiB/3GiB khi verify,
  không là peak profiler; restart0, cgroup oom/oom_kill0 sau lần recovery.
- Scoped Java/Spark: **35 PASS**,0fail/error/skip; Maven offline package đạt.
- Python continuation12 + memory2 + preflight13 + E2E8 = **35 PASS**.
- make ci-docs-base, QRY-01 static config và git diff --check đạt; docs/status/
  config/CI policy guards chạy thật. Không coi đây là full Maven suite hoặc
  GitHub CI đã pass trước khi CI chạy trên commit mới.
- Original failure, first failed continuation, private logs/raw/JAR đều giữ
  trong staging; chỉ metadata vào Git. Backup có SHA inventory, quyền private.

## Đường dẫn artifact và giới hạn

Deployment staging/dat03-continuation/ba46d523846e47e48036f5558c6e3ea4/runtime/
chứa report.json, acceptance-recovery.json, validation.json, recovery-execution.json
và checkpoint pins; source staging nằm trong named volume, host/runtime là bản
copy kiểm offline. Không chạy lại attempt đã completed; controller fail closed.
Mount memory override dùng source config ở checkout PR #100; cần giữ checkout
này tồn tại cho deployment hiện tại, hoặc chuyển sang file đã merge khi triển
khai lần sau có phê duyệt. Không tự bỏ override/recreate lần nữa để cleanup.

**DAT-03 — Catalog thật/team handoff:** còn full225+63-month source proof,
Spark/Trino/export capacity và rollout budget, collection-aware official MLI-02
export/loader/rerun và receipts ba máy. Giữ research_ready=false,
full_research_catalog=false, official_exported=false. Không mở scientific
acceptance **EXP-02/03/04/05** bằng bốn tháng bounded này. **SEC-01 — Security:**
cloud/Colab/Drive chưa nghiệm thu; không upload hay thay ACL trong phiên này.
