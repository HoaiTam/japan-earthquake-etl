# DAT-03 — Read-only verification benchmark 27/10/2026

Branch `perf/dat-03-verification-benchmark` từ origin/main fe80830; giữ các
thay đổi recovery/CAS local có trước, không commit/push. Owner duyệt mở Docker,
sửa quy trình và test batch hiện tại; không mở batch mới/automation.

## Runtime trước test

Docker daemon đã mở; chỉ start năm container cũ: PostgreSQL, MinIO, Iceberg REST,
Trino, scheduler. Cả năm healthy; không build/pull/init/recreate, không bật Spark
hoặc UI/DAG processor. Active DagRuns=0, active Trino queries=0, Java cũ=0.

Recovery V2 session1547dae95a0c4ff5a9a2b063b075441d chỉ có intent/stdout/stderr,
dừng tại phase verify ordinal134; không execution/result/acceptance, không
local verify/publish receipt. Lease vẫn đúng catalog owner. Không gọi đây là
known exit1, không tự tạo execution/release hoặc chạy lại attempt. Accepted
vẫn113/288 (39,236%), không119. Lịch tự động Codex đã hủy.

## Input và kết quả thật

- Batch06–11/2009; gold run `orc07-256d04ae5f925034f2c433d3ece4c09b2ee8fbe0`.
- Local commit SHA `dc96eb5a219a8987c22a5b39ca131508b33eab8b14e05ee7d3643c311a47b8a2`.
- Event snapshot4210401894837627562; bridge snapshot1017634633979993447.
- Mỗi bảng1.274.695rows. Không thay snapshots/source pins/cutoff.
- Whole primary_provenance: **16,467889716s**, actual0.
- Đủ16bucket: **88,696164623s**, tất cả actual0; sum=whole=0.
- Whole nhanh khoảng **5,39 lần cho riêng check này**, giảm72,23s/81,4% so
  với tổng16query trong lượt đo này; không phải speedup toàn batch.
- Whole query `20261027_113549_00000_txbca`: FINISHED, peak user memory
  259.408.456bytes (~247,4MiB), CPU8,31s. Bucket peaks khoảng18,9–31,2MB.
- Benchmark17SELECT exit0; postflight active queries=0.
- Times bucket0..f (s): 7.880736419,7.099788961,5.519011794,6.002924586,
  5.579627670,4.021054627,4.348947502,4.379327711,3.998899293,5.426931836,
  5.501096002,10.070247505,5.600377211,3.675691710,4.852179669,4.739322127.

Chỉ một lượt A/B trên stack giảm tải, whole chạy trước buckets; chưa repeated
benchmark/cold-warm controls hoặc full-period capacity. Không so trực tiếp
thời gian này với stack9services cũ để quy toàn bộ cải thiện cho code.

## Thay đổi và kiểm thử

Thêm read-only Java benchmark và profile opt-in primary whole-query có giới
hạn1.300.000rows mỗi fact. Không bật profile mới cho pinned controller; giữ
legacy factories và mọi outside/duplicate/lineage/publication gates.
Unit/mock tests kiểm scalar nghiêm ngặt, allowlist, đủ bucket còn lại, violation
chặn writes, và capacity guard. Lần launch benchmark đầu thiếu Parquet do gọi
hash helper kéo dependency Spark: đã thay bằng JDK MessageDigest, build/test
lại và runtime17SELECT hoàn tất. Không chạy Spark fixture song song benchmark.

Kết quả cuối:26unit/mock tests PASS (benchmark2, fact buckets8, publication5,
outside6, verified reuse5); docs265files/2392links/0problems;77task statuses
đồng bộ; git diff --check PASS. Không tuyên bố full E2E hoặc full verification.

## Phần chưa hoàn thành

**DAT-03:** xử lý interrupted attempt bằng recovery có version/evidence riêng,
wiring profile được đo và full verify/publication/acceptance; sau đó catalog
288tháng, official ML export và receipts ba máy. Benchmark không Published,
không official export, không nâng research_ready. Drive vẫnpartial101.
Không đổi snapshot receipt để giả119accepted hoặc giải phóng lease bằng tay.
