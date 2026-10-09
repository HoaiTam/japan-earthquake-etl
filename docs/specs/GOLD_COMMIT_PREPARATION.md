# GLD-03 — Chuẩn bị interface commit và test plan

**Phạm vi PR: interface/mock/offline; task In Progress, chưa có Iceberg writer.**
Hard dependency [GLD-01 — Xây canonical event, dimensions và bands](../task/tasks/GLD-01.md)
đang có implementation PR #50, cần merge/handoff. Theo file task, interface/mock
được chuẩn bị trước; không nhận mock là commit/readback thật.

## Interface

`GoldCommitRequest` khóa operation/run/window/date/backfill/config, exact Silver
manifest URIs, required table set, baseline snapshots, affected year/month và
expected counts. Baseline 0 chỉ biểu diễn table chưa tồn tại, không phải committed
snapshot. Count 0 vẫn có affected scope để writer tương lai xử lý việc partition
rỗng sau revision, không dựa riêng vào partitions có row trong output.

`GoldCommitAdapter` là SPI chưa có production implementation. Inspect durable
operation metadata trước retry; UNKNOWN không được replay. Commit phải hold cùng
whole-run lease của [ORC-05](./RECOVERY_AND_RESOURCES.md), compare baseline, ghi
đúng affected scope và lưu operation identity trong Iceberg snapshot properties/
receipt. CommitReceipt cần exact required table set và positive snapshot IDs,
readback; receipt này **không chứa Published**. Consumer chỉ được dùng bundle đã
verify/publication ở GLD-04. Không hứa transaction atomic cho nhiều bảng.

## Test offline đã có

```bash
./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=GoldCommitInterfaceTest -Dsurefire.failIfNoSpecifiedTests=false test
./scripts/check-task-status.sh
git diff --check
```

Mock nhỏ kiểm tra count0/new table, failed quality, incomplete/foreign scope,
deterministic identity, partial readback và rerun reuse. Không đọc/ghi lake.

## Test plan runtime còn phải thực hiện trong GLD-03

1. Chốt Iceberg runtime dependency tương thích Spark3.5/Scala2.12/Java17, dùng
   chung catalog/warehouse QRY-01; không downgrade/shadow bundled Spark Jackson.
2. DDL từ CON-03 cho current/bridge/dimensions cần publish; thống nhất affected
   scope của mỗi bảng (dimensions không có cùng partition transform với fact).
3. Nhận exact SilverReady bundle từ [SLV-09 — Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md).
4. First commit, same-input rerun và late revision di chuyển event giữa tháng;
   verify logical keys/counts và outside-scope baseline không đổi.
5. Empty affected partition xóa đúng current rows lỗi thời trong scope đã pin,
   giữ history/partition ngoài scope; không xóa warehouse/metadata để retry.
6. Fault giữa hai table commits, mất receipt, concurrent baseline change: inspect
   từng snapshot với operation hash; chỉ hoàn tất phần chưa commit có bằng chứng.
7. Gửi exact snapshot bundle cho [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md);
   verify fail/commit partial không tạo publication.

Máy hiện chưa có .env/Docker daemon. Không có lệnh business writer để chạy thật:
GLD-03 phải bổ sung CLI/Makefile/runbook khi triển khai writer, không dùng smoke-query
hạ tầng để nhận acceptance commit Gold. Reviewer unassigned.
