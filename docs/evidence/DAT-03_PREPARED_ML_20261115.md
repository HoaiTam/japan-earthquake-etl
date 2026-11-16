# DAT-03 — Official collection adapter và resource gate, 15/11/2026

## Kết luận hiện tại

**Phần 3 chưa hoàn tất. DAT-03 giữ In Progress**, HoaiTam, reviewer unassigned.
Adapter có version đã resolve được actual Published PREPARED Gold và fresh source
proof; hai preflight thật kết thúc sạch với `BLOCKED_RESOURCE`. Chưa ghi ML,
chưa có official EXPORTED, paired release hoặc receipt mới của nhóm.

Đọc [evidence JSON](DAT-03_PREPARED_ML_20261115.json) để xem exact session/request/
runner/report/execution/acceptance SHA, toàn bộ blocked IDs và strict exclusions.
Không dùng virtual dataset ID trong preflight như một manifest đã persist.

## Input và kết quả dữ liệu thật

Input là [Gold phần 2](DAT-03_PORTABLE_GOLD_20261115.md): 288 tháng, sáu Published
snapshots đã qua independent rerun. Không chạy lại Bronze/Silver hoặc thay Gold.
Source cutoff gốc vẫn `2026-10-25T06:13:04.747Z`; cutoff ML của request v1 là
`2026-11-15T08:03:04.307Z`, không có nghĩa source đã cập nhật đến ngày đó.

| Preflight v1 | REPRODUCTION | EXTENSION |
|---|---:|---:|
| Coverage đủ tháng | 225 | 63 |
| Gold input rows | 4.488.650 | 4.488.650 |
| Eligible / excluded | 2.244.533 / 2.244.117 | 938.856 / 3.549.794 |
| Global MAXC Mc | 0,5 | 0,4 |
| Mainshocks trước / sau strict coverage | 200 / 194 | 48 / 45 |
| Candidates sau strict coverage | 156.710 | 233.664 |
| Windows vượt cap 10.000 | 3 | 5 |
| Window lớn nhất | 38.854 | 83.504 |
| SQL readbacks thực tế | 9/9 | 9/9 |
| Thời gian phiên (giây) | 995,209 | 870,677 |
| Terminal / exit / own lease | BLOCKED_RESOURCE / 0 / released | BLOCKED_RESOURCE / 0 / released |

Candidate rows là membership theo từng mainshock window, không phải số động đất
duy nhất. Một event có thể thuộc nhiều window; không coi đây là lỗi duplicate.
6 REPRODUCTION windows bị loại vì thiếu PRE/POST coverage (5 POST, 1 PRE),
3 EXTENSION windows thiếu POST. Không cắt ngắn window hoặc lấy dữ liệu chéo split.
Không loại các window lớn để vượt resource gate. Mỗi ID/required interval/reason
được giữ nguyên trong evidence JSON và original report.

## Profile 40.000 đã được duyệt

Owner chọn thiết kế/đo profile v2, giữ nguyên Mc/window/toàn bộ dữ liệu, không
sampling hoặc tune science. Profile opt-in mới pin cap và receipt vào identity;
legacy/default 10.000 không đổi. Các giới hạn khác vẫn 100.000 rows/shard,
128 mainshocks/shard, 64 shards và 4.096 mainshock metadata.

Benchmark Java 17/Spark dùng synthetic 40.000-row whole window, so candidate
fingerprint giữa hai cap, tạo feature, Parquet readback và immutable rerun.
Đây chỉ là component capacity test, **không là nghiên cứu/EXPORTED acceptance**.
Các lần benchmark chưa đạt được giữ evidence, không ghi S3/Gold:

1. `41d836f9659341c8ad23206f012371f2`: test class chưa được shade cùng production
   Jackson, `NoSuchFieldError: JSON`. Đã đưa benchmark entry point qua cùng Maven
   shaded runner; không thay classloader Spark hoặc thư viện runtime.
2. `9695a970c0ee4f1bb6bf8f89b72a26dc`: resolver selection còn khóa baseline 10.000.
   Đã thêm opt-in exact version/profile/scaling 40.000; regression kiểm resolver
   thực sự nhận cap mới và từ chối config không opt-in.
3. `c149b284c3e64c979cb34ba57a54bef0`: timeout600 giây trong readback/rerun,
   exit143, peak child RSS 1.142.372 KiB. First materialization đã có receipt
   PREPARED 40.000 rows/1 shard nhưng **không** là benchmark PASS. Stack chẩn đoán ở
   `FeatureExportBundle.validate` trong `ResearchCollectionJob.verify` của
   rerun; đã gộp 29 null-field checks thành 1 aggregation cho opt-in 40k. Giữ cùng
   timeout/quality rules và thêm 2 null fault injections, không auto mark success.

Phiên `73b5d4d1cd5b4804ae10202b858d83ae` **PROFILE_BENCHMARK_PASSED** sau tối ưu:
Java 17.0.19/Spark 3.5.9, **98,107 giây**, peak child RSS **1.099.236 KiB**
(khoảng 1,05 GiB), Parquet **1.031.017 bytes**, đủ 40.000 rows/1 whole-window shard.
Candidate fingerprint không đổi, immutable rerun giữ manifest/checksums và
2 null fault injections đạt. Giữ timeout 600 giây, heap 768 MiB/direct 256 MiB; không
đổi Docker profile. Không suy từ số đo này rằng HDBSCAN fit đã đạt capacity.

Receipt SHA `b6a9d6a2cb08438c078f468a8d18a90de1061d792601982abac5fe45e89a8af2`,
runner SHA `37f60e1456579ff97278e50f02e4ed6662f024ec87ef257fe57c146cf872e158`.
Đã prepare v2 metadata-only tại
`staging/dat03-prepared-ml/c4bd824ce3a741a78ae3f13ff0019a1f/`, request SHA
`aaeaab957fc0017dedce9f3f58c44c9ac558d066b248b30d09ab088b956b1720`.
**Chưa chạy fresh real v2 preflight hoặc official build**, không dùng kết quả
v1 BLOCKED để gọi v2 PASS. Số đo và toàn bộ SHA nằm trong evidence JSON.

**Ngay cả khi benchmark 40.000 đạt, EXTENSION vẫn bị chặn** bởi hai windows:

- `evt_54884e3745b8a26562151f5bd180ec34`: **83.504 candidates**.
- `evt_5a4e58cb9d43b94daf27a5d07095f967`: **76.260 candidates**.

Không tự nâng cap lên 90.000/100.000, tăng Mc, chia/cắt window hoặc sửa flags.
Cần quyết định resource profile riêng trước khi nghiệm thu official EXTENSION.

## Code, tests và phạm vi chưa nghiệm thu

- [Contract](../specs/PREPARED_ML_EXPORT.md),
  [runbook](../runbooks/DAT-03_PREPARED_ML.md).
- Coverage mới dùng original PREPARED/source/Gold proof, không giả legacy
  `registry_sha256`, FullMonthReady/SilverReady hoặc 288 records.
- Controller pin runner/config/source và capacity report; build gọi 10 bước
  MLI-02 collection lifecycle/exporter có sẵn, checkpoint và own lease.
- 51 Java metadata/regression tests PASS; 18 Python controller/Gold tests PASS;
  26 loader/team fixture tests PASS với PyArrow 21.0.0. Đây là fixture tests,
  không chứng nhận runtime official export.
- Docs/task/whitespace checks đạt tại lúc chạy; chạy lại trước commit.
- Postflight read-only 8/8 SQL đạt: Gold six pins không đổi, cả namespace ML v1/v2
  chưa có bảng, lease empty và không Java/DagRun/query active; tracked services
  IDs/start/restarts giữ nguyên. Không dừng Docker services khi bàn giao.

**Còn thiếu thuộc DAT-03 — Chuẩn bị catalog nghiên cứu thật và bàn giao dataset
dùng chung**: fresh v2 preflight, actual 10-step build/VALIDATED/EXPORTED cho cả
hai split, trusted registry, all-shard official loader, rerun, paired immutable
release và ba receipt máy thật. Các đường build/checkpoint/final exporter mới
chưa có runtime acceptance trên adapter này; không gọi phần 3 Done bằng unit test.
MLD-05/MLI-02 component Done không thay các acceptance runtime DAT-03 này.
