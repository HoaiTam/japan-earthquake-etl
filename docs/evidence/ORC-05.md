# ORC-05 — Evidence recovery/resource profile

## Nền và phạm vi

- Branch `feat/orc-05-recovery-resource-profile`, tạo từ merge commit PR #47
  `752cce27fc37969725551fe202e31efe774026e9` ngày2026-10-09.
- GitHub `main` tại lúc fetch là `1aec003` (PR #46). PR #47 đã merge vào
  branch ORC-03 sau #46, **chưa có ORC-04 trên main**. Không tự cập nhật main/
  push/merge để sửa topology; ORC-05 giữ nền mới nhất PR #47 theo yêu cầu.
- Hard dependency ORC-01 Done; HoaiTam assignee, reviewer unassigned.
- [Profile/matrix/operator notes](../specs/RECOVERY_AND_RESOURCES.md).
  Đây là acceptance local/pilot, không benchmark40năm hay Gold Published.

## Máy / runtime

- Host AppleM1, 8CPU, RAM8589934592bytes (8GiB).
- Docker daemon: 8CPU, memory5685809152bytes (~5.30GiB).
- JDK host cho checks: Azul21.0.11; image Java17, Spark3.5.9,
  Airflow3.3.2/Python3.13.
- Stack nền9service healthy đang chạy. Snapshot idle `docker stats`
  (không coi peak): worker240.3MiB, scheduler471.6MiB, API267.4MiB,
  dagprocessor263.9MiB, Trino974.6MiB, master187.4MiB, REST107.2MiB,
  MinIO119.9MiB, PostgreSQL78.98MiB (~2.65GiB total).
- Service đang chạy vẫn cấu hình cũ (scheduler limit1GiB, daemon heap cũ).
  Không restart/deploy scheduler globalparallelism mới trong kiểm thử này.
  New one-off services dùng profile mới; rebuild/recreate sau review mới áp
  dụng policy shared environment. Total container ceilings không bằng RAM sử dụng.

## Kiểm tra đã chạy

| Check | Kết quả | Boundary |
|---|---|---|
| `make test-airflow` | 174tests passed (11 ORC-05 cases) | Offline SDK doubles/control-plane; không scheduler evidence |
| Java `ResourcePilotJobTest` | 3tests passed, host JDK21 | Bounded line selection, CRLF/EOF, bad limit/width |
| `make check` với JDK21.0.11 | Exit0, 182 Java +174 Airflow +12 build-input tests; config/contract/static Compose passed | Regression, không cộng những lần wrapper chạy lại |
| Build Spark/Airflow image | Maven clean verify182tests passed; shaded JAR compatibility được test runtime | Java17 packaged runner; Spark dependencies vẫn provided |
| Native Airflow3.3.2 DagBag | No import errors; ORC-01/02/03/USG-04/JMA-04: max runs/tasks1, chỉ USGS verify retries1 | Actual parsed graph, **không** chứng minh scheduler env đã recreate |
| `make smoke-recovery` sau SDK relocation | Exit0, BronzeVerified; contention blocked, failed source recovered, same pins/counts rerun, lease released | Fresh Java raw/manifest readback, không scheduler trigger/replay receipts |
| `make smoke-resource-pilot` final | Exit0, 10015 observation shuffle, ~18s, driver/worker OOM counters0 | Actual standalone executor, actual Java parsers; không Silver/Gold write |
| Runtime maintenance preview exact QA run | Exit0, terminal=true, automatic cleanup=false, mutation=false, lease absent | Read-only exact-run diagnosis, giữ audit |
| Sau smoke | 9foundation services healthy | Không restart/unpause, không mất volume/data |

## Input, measurements và recovery

Exact source pins nằm tại `airflow/dags/fixtures/orc_03_reuse_sample.json`;
operation `orc03-reuse-pilot-v1`, scope SHA
`6d07b418c4d7e0ec66e549eedb21536a3c453949ce7f1a04a55a20130035968d`.
USGS UTC `[2023-01-01,2023-01-04)`; JMA2023 full-year ZIP/release đã pin.
Không GET nguồn HTTP, scan/list bucket hoặc ghi lake.

| Source / count | Full structural readback | Selected parse input | Observations | Parser rejects / ignored |
|---|---:|---:|---:|---:|
| USGS | 16 | 16 | 16 | 0 /0 |
| JMA_BULLETIN | 257020 | First10000 lines | 9999 | 1 /0 |

10015 observation thật qua Spark repartition4/groupBy/count. Đây không phải
quality/dedup/link/Gold canonical counts hoặc full-year JMA parse. Probe
không ghi Parquet/Iceberg, luôn published=false.

Final app `app-20261009082813-0001`, run
`orc05-resource-qa-61c8a5deecf44fe78a36980799836769`:

- Duration17.995116092s (readback/parse/Spark), driver requested512m,
  reported heap max518979584bytes; executor512m/1core.
- Driver cgroup lifetime peak491380736bytes (~468.62MiB), heap pool peak sum
  140161136bytes (upper bound pool peaks, không RSS).
- Worker lifetime peak trước261799936bytes, sau596291584bytes (~568.67MiB);
  đây là peak container gồm daemon và các probe trong đời container, **không
  reset counter hoặc gọi đây là isolated executor RSS**.
- Driver/worker `memory.events`: max/oom/oom_kill/oom_group_kill=0; worker
  healthy sau probe. Không đo simultaneous total-stack peak/host swap.
- First successful app `app-20261009082248-0000` có driver peak480485376bytes,
  duration26.253366262s; giữ lịch sử, không chọn số thấp nhất làm final report.

Final recovery run `orc05-recovery-qa-3eb111b50d6f418d9696c8373490bb21`,
duration3.493966376s, Java heap384m. Inject failure **trước store call** →
FAILED persisted → contender bị shared lease chặn → readback attempt2 →
rerun attempt3 giữ counts/pins → own lease released → BronzeVerified.
Không xóa/chỉnh raw hoặc giả lập network outage/partial Iceberg commit.

Reports trong shared staging:

- `/opt/pipeline/staging/backfill/qa/orc05-resource-qa-61c8a5deecf44fe78a36980799836769/resource_report.json`
- `/opt/pipeline/staging/run-summary/qa/f42ffaf5c65f29ccb30eeb3617c8bac3a576af2eda4317c904b18a3d1d7a8da8/run_summary.json`
- [Portable measured summary](./ORC-05-runtime.json).

Maintenance preview đã chạy trên recovery run trước
`orc05-recovery-qa-e0c084e51f2e47e9aea1ea90c069d2e4`, folder
`run-summary/qa/5e9e29188d75519be583b698e86d228d1b35510bd4cdfb69e7a1f8a7f099e73d`;
không xóa journal/cache hoặc release owner để tạo pass.

## Lỗi runtime đã phát hiện và sửa

1. Spark Java-only image chưa có Python: cài Python3 Ubuntu3.10 cho wrapper
   **control-plane**; Airflow vẫn Python3.13, không chuyển ETL records sang Python.
2. MinIO SDK gặp `NoSuchMethodError` dưới Spark parent-first HTTP classpath:
   shaded runner relocate HTTP/Kotlin/Jackson/Guava/Commons SDK deps vào
   `ie212.earthquake.internal.*`, không đổi Spark's Jackson/classloader policy.
   Final standalone pilot và Bronze packaged recovery đều pass sau sửa.
3. Temporary NSS identity cho shared Airflow UID dùng thư viện có sẵn trong
   Spark image, không sửa `/etc/passwd` hoặc chown shared volume.

## Acceptance / giới hạn

- No OOM: đạt trong bounded pilot và fresh full Bronze readback ở máy trên;
  không bảo đảm full40năm, mọi services peak đồng thời hoặc new workloads.
- Retry đúng tầng: native DAG graph + HTTP policy giữ riêng + controlled
  source recovery/rerun. Mutating tasks không replay tự động.
- Recovery không cần delete: byte-pinned inputs/count giữ nguyên, own lease
  release, preview giữ audit; no lake writes/source downloads/volume deletes.
- Full recovery writer Silver/Gold: **SLV-09 - Tích hợp và kiểm thử Silver đa
  nguồn**, **GLD-03 - Ghi Gold Iceberg và commit snapshot**, **GLD-04 - Tạo Trino
  views và verification SQL** và **QA-01 - Chạy E2E daily đa nguồn** còn cần
  integration thật; matrix không thay evidence partial commit/Published.
- **SEC-01 - Review secret và bề mặt truy cập** nhận resources, lease/volume
  permissions, internal/loopback endpoints và safe log scope; chưa security approval.
- Giữ `.env` và `.metals/` của user; không commit/push/open PR hoặc deploy/recreate
  stack. Reviewer unassigned; checklist review độc lập chưa tick.
