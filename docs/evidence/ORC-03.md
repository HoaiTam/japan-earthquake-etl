# ORC-03 — Evidence backfill/reuse/scoped handoff

## Baseline và phạm vi nghiệm thu

- Branch `feat/orc-03-backfill-reprocessing` từ latest fetched `origin/main`
  `ff4f460` (merge PR #45), ngày `2026-10-08`.
- Assignee `HoaiTam`; reviewer `unassigned`, chưa có approval độc lập.
- ORC-01 hard dependency Done. Acceptance ORC-03 là orchestration/parameters,
  bounded source workflow, immutable Bronze reuse và scoped adapter gates;
  **không thay acceptance physical Silver/Gold writers của downstream**.
- Python điều phối metadata; Java xử lý raw/checksum/structure; Bronze và
  ETL contract cũ không đổi. Scoped extension `orc-03-v1` tách ORC-01 `1.0`.

## Lệnh / kết quả thực tế

| Lệnh/check | Kết quả | Phạm vi chứng minh |
|---|---|---|
| `make backfill-preview` | PREVIEW, 3 USGS chunks + JMA oct-dec | Offline scope resolve, không service/write |
| `make backfill-preview BACKFILL_CONF=airflow/dags/fixtures/orc_03_reuse_sample.json` | PREVIEW | Exact historical release/raw+manifest SHA pins |
| `make backfill-preview BACKFILL_CONF=airflow/dags/fixtures/orc_03_reprocess_preview.json` | PREVIEW | Explicit Silver/Gold logical month scopes + bootstrap assertions |
| `make test-backfill` | 22 Python + 28 Java tests | Planner/lease/failure/idempotency protocol + verifier/source runners offline |
| `make test-airflow` | 111 tests | Toàn bộ Airflow metadata/graph tests offline |
| `make check` | Pass: 176 Java, 111 Airflow, 12 build-input regressions + 73 task status/static/config checks | Host JDK21.0.11, compiler release17; không nguồn thật |
| `docker compose build airflow-api-server` | Build success, 176 Java tests trên Java17 | Fresh image và verifier wrapper/COPY/.dockerignore |
| Airflow 3.3.2 DagBag | 5 tasks, sole completion leaf, manual/single concurrency | Import/graph thật, không trigger scheduler |
| `make smoke-backfill-readback` | VERIFIED, two exact inputs, first == rerun | Java17 → MinIO readback thật, không source/lake writes |
| `make smoke-backfill-pilot` | VERIFIED, 2 USGS day chunks + 1 JMA segment, first == rerun | Source workflow API thật → Bronze, outside pinned objects readback trước/sau |
| `git diff --check` | Pass | Whitespace của tracked changes |

`make check` lần đầu trong sandbox thất bại vì 7 HTTP mock tests không được
bind localhost (`Operation not permitted`); chạy lại với quyền bind phù hợp
đạt. Không skip tests/giảm gates để đổi failure thành pass. Cảnh báo Maven
dependency/shade overlap vẫn như baseline, không phải error.

## Live reuse

Report: `orc03-reuse-qa-1eb62ce8cedf43a5a51871b658a03733`.
Operation `orc03-reuse-pilot-v1`, scope SHA
`6d07b418c4d7e0ec66e549eedb21536a3c453949ce7f1a04a55a20130035968d`.

Exact USGS source window `[2023-01-01,2023-01-04)` từ ORC-02 first-run
manifest; JMA 2023/full-year release từ JMA-05. Java kiểm lại cả raw/manifest
hashes, flags, source scope, structure/count; first và rerun trả cùng pins.
`published=false`, `lake_writes=false`, không HEAD/GET nguồn hoặc list prefix.
Lease ACQUIRED/RELEASED đúng identity, không thay runner container foundation.

## Live source pilot

Report/operation `orc03-source-qa-017ec08e65cf4a3caf5a7271c4bbe6be`, scope SHA
`e2e7cede42f13980765cbb7c8c4fc62ee61d576e0ef964c8cce109567a73b110`.

| Input | Source scope | Kết quả rerun |
|---|---|---|
| USGS chunk 1 | `[2023-01-01,2023-01-02)` UTC | Cùng raw SHA, manifest byte SHA và URI |
| USGS chunk 2 | `[2023-01-02,2023-01-03)` UTC | Cùng raw SHA, manifest byte SHA và URI |
| JMA | 1997/oct-dec, native `[1997-10-01,1998-01-01)` JST | Reuse exact JMA-05 publication, không raw copy mới |

Hai USGS chunks tạo immutable Bronze objects mới trong đúng operation namespace;
rerun cùng ID/conf/processing date giữ nguyên cả 3 inputs. Java đọc lại hai
sample objects khác namespace trước/sau, hashes không đổi. Đây không phải
bằng chứng toàn bộ Silver/Gold partitions ngoài scope được scan/verify live.

Report JSON được copy **từ report metadata thật trên shared staging** tại
[ORC-03 runtime](./ORC-03-runtime.json), chứa exact pins; không GeoJSON/ZIP
payload, secret hoặc training data. `method=runtime_api_not_scheduler`:
one-off container gọi workflow helpers dưới lease, **không scheduler E2E**.
Fresh Docker image build đã đạt; foundation không bị restart/recreate,
không unpause DAG, xóa bucket/volume hoặc thay normal runner. Runtime Java
Temurin `17.0.19+10`.

## Acceptance mapping và giới hạn

- Rerun/no duplicate: live first==rerun pins (source pilot và read-only reuse),
  Java tests verify no lake writes; upstream runner tests cover revision/cache
  reuse. Không hứa exactly-once physical publication sau mất staging/cache.
- Outside scope: Bronze writer immutable + readback old exact objects; scoped
  adapter contract tests chặn scope/hash/baseline/idempotency/outside-scope false,
  Trino blocker chặn publish. **Chưa kiểm physical Silver/Gold scope thật**.
- Operator preview: cả ba actions, exact chunk/segment/release/checksums,
  existing-state/baseline/logical output scope tường minh. Preview không chứng
  minh coverage/checksum đã readback và không cấp Ready/Published.

Downstream còn thiếu, không chuyển trạng thái từ evidence này:

- [SLV-09 - Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md):
  real Silver adapter/bundle, existing-state resolution; SLV-06/07 còn
  dedup/revision/link và xác định old/new affected scope.
- [GLD-03 - Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md):
  scoped writer/baseline/idempotency ledger/physical snapshot commit.
- [GLD-04 - Tạo Trino views và verification SQL](../task/tasks/GLD-04.md):
  scope/count/lineage SQL và publication evidence thật.
- [ORC-04 - Chuẩn hóa logging và run summary](../task/tasks/ORC-04.md):
  counts/durations/reconciliation; ORC-03 summary chỉ có scope/refs/status.
- [ORC-05 - Chốt recovery, concurrency và tài nguyên](../task/tasks/ORC-05.md):
  stale-lease/cache-loss/partial commit recovery, all-writer/distributed locks,
  resource sizing; không kết luận hardware benchmark từ pilot.
- [QA-01 - Chạy E2E daily đa nguồn](../task/tasks/QA-01.md): còn scheduler
  ETL thật đến Published; [QA-03 - Kiểm thử JMA historical backfill](../task/tasks/QA-03.md):
  còn historical QA rộng, không full 40 năm ở đây.

Không commit/push/mở PR hoặc merge. `.env` và untracked `.metals/` được giữ
nguyên. Runbook: [backfill/reprocessing](../specs/BACKFILL_AND_REPROCESSING.md).
