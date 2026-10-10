---
task_id: "SLV-09"
status: "Done"
week: 4
block: "D - Silver đa nguồn"
workstream: "Integration QA"
scope: "Core"
priority: "P0"
effort_hours: 4
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["USG-05", "JMA-05", "SLV-02", "SLV-03", "SLV-04", "SLV-05", "SLV-06", "SLV-07", "SLV-08"]
---

# SLV-09 - Tích hợp và kiểm thử Silver đa nguồn

## Mục đích

Dùng làm mốc SilverReady của input được pin để Gold dùng dữ liệu thật; các
task SLV-02..08 vẫn phát triển độc lập bằng fixture.

## Phạm vi công việc

Exact manifests → verify Bronze → parse/lineage → quality → source dedup/revision
→ linking/canonical → four-dataset bundle → readback → Gold handoff.
Phân biệt full archive, selected, parsed, rejects, current và history counts.

## Thành phần cần có / Deliverable

- Offline suite: accepted/ambiguous/empty/duplicate/revised, blocked quality,
  idempotent rerun, corrupt/partial writes và stale marker.
- Run-level reconciliation và exact manifests/context của bốn datasets.
- Smoke Spark Java17/MinIO SDK thật, USGS DAT-01 16-event + JMA 2000
  (tái lập JMA-05) và JMA 2023 (mở rộng), ghi rõ selection/coverage bounded.
- Bàn giao Parquet đọc ngược cho GoldEventTransformer; không commit Gold.

## Tiêu chí hoàn thành

- [x] Logic không đổi khi rerun fixture; checksum/config mismatch bị chặn.
- [x] parsed/valid/rejected/duplicate/superseded/current/canonical đối soát được.
- [x] Hai nguồn bằng raw Bronze thật đã pin, coverage JMA 2000/2023 minh bạch.
- [x] Live MinIO readback bốn datasets/manifests/checksums/marker bundle;
  Spark Java17 standalone runtime có command/evidence tái lập.
- [x] GoldEventTransformer đọc dữ liệu persist/verify; current/bridge/canonical
  khớp report, rerun reuse bundle và không đổi logic/SHA.

## Hard dependency

- [USG-05](./USG-05.md), [JMA-05](./JMA-05.md).
- [SLV-02](./SLV-02.md), [SLV-03](./SLV-03.md), [SLV-04](./SLV-04.md),
  [SLV-05](./SLV-05.md), [SLV-06](./SLV-06.md), [SLV-07](./SLV-07.md), [SLV-08](./SLV-08.md).

## Cách triển khai và phối hợp

- Phần vá giao HoaiTam; giữ reviewer unassigned, không tự xác nhận review.
- Theo yêu cầu người dùng, branch vá tạo từ **PR #58**, không từ main cũ.
- Runner gate quality/reconciliation trước writes. Bundle immutable theo run,
  final SDK readback trước SUCCESS; dataset rỗng vẫn có schema/count 0.
- Whole-run lease chung với daily/backfill, không TTL takeover; timeout giữ
  khóa nếu chưa xác nhận Spark dừng. Không partition overwrite cho smoke.
- `make test-silver-integration` offline; `make smoke-silver-integration` live.
  Report có exact context/input, dataset URI/SHA/count và runtime identity.
- Handoff mẫu không đồng nghĩa full historical ingest/Published. Chỉ Done
  khi deliverable, acceptance, tests và live evidence/docs đã đạt.

## Ranh giới / phần còn thiếu thuộc task khác

- [GLD-03 - Ghi Gold Iceberg và commit snapshot](./GLD-03.md): commit snapshot.
- [GLD-04 - Tạo Trino views và verification SQL](./GLD-04.md): verify Published.
- [QA-01 - Chạy E2E daily đa nguồn](./QA-01.md): ghép adapter orchestration,
  nghiệm thu whole flow daily thật; smoke SLV-09 không thay thế task này.
- Không commit secrets/raw ZIP/data dumps/JAR; không xóa bucket/warehouse/volume.

## Theo dõi

- **Trạng thái:** Done
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** [Bằng chứng bản vá](../../evidence/SLV-09.md),
  [live receipt](../../evidence/SLV-09-live-readback.json), baseline
  [PR #58](https://github.com/HoaiTam/japan-earthquake-etl/pull/58).
- Vá claim in-memory = live và JMA synthetic = raw thật của PR gốc.
- Nghiệm thu 2026-10-10: 253 Java + 8 Python control tests pass; run
  `slv09-live-59a5e3725b14453e89a127eb2c8a70d2` có hai submit Java17/Spark3.5.9,
  528 valid/current/canonical/bridge, reject/history/link 0. Rerun reuse,
  bundle SHA/count không đổi; source lease đã nhả. Evidence chỉ thuộc mẫu bounded.

## Checklist bàn giao

- [x] Implementation/offline regression suite đã có.
- [x] Live acceptance/evidence đã kiểm tra.
- [x] Docs giải thích purpose, flow, bundle identity, recovery và giới hạn.
- [x] Check cuối/secret/diff hygiene đạt.
- [ ] Reviewer độc lập P0/P1 (khuyến nghị, không chặn Done sau nghiệm thu).

## Tài liệu liên quan

- [Silver integration](../../specs/SILVER_INTEGRATION.md)
- [Shared real samples](../../specs/SHARED_REAL_SAMPLE_DATA.md)
- [Silver/Gold logical model](../../specs/SILVER_GOLD_DATA_MODEL.md)
- [Recovery/resources ORC-05](../../specs/RECOVERY_AND_RESOURCES.md)
- [Phân công tuần 4](../WEEK_4_PARALLEL_PLAN.md)
- [Kế hoạch 8 tuần](../README.md), [Các khối công việc](../WORK_BLOCKS.md)
