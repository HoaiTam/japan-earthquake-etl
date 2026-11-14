# DAT-03 — Original follow-up và actual portable Silver readback14/11/2026

## Kết luận hiện tại

**In Progress**. Nhận đủ original follow-up của cả hai worker và đã chạy Java
readback đủ125tháng. Chưa có unified Gold288tháng, actual boundary/Trino acceptance
trên namespace mới, official ML export hoặc receipts ba máy.

Branch `feat/dat-03-portable-integration` từ main `f09ac66` (#127). Tái dùng
inventory/requests đã kiểm ở PR #131/c56f153. Ngày15/11 owner yêu cầu commit/push
phần đã làm vào cùng PR #131; không merge PR vào main hoặc chạy tiếp ETL.

## Original metadata đã nhận

| Worker | Follow-up pinned objects | Publication đối chiếu independent phase | ZIP SHA-256 |
| --- | ---: | ---: | --- |
| rosy179 | 24/24 | 23/23 | `517f8625ef7de46d50e81c79f22b1ad878a5820174104ee5c3daa27ce5cadc7c` |
| ThanhTris | 2/2 | 1/1 current-head;18publications đã kiểm trước | `0118968a5cfbecc27945c4fcdd970a467078280861855b57984e921684d353ed` |

Nguồn: [comment Trang #127](https://github.com/HoaiTam/japan-earthquake-etl/pull/127#issuecomment-6878507388),
[comment Trí #133](https://github.com/HoaiTam/japan-earthquake-etl/pull/133#issuecomment-6879847930).
Identity kiểm canonical payload digest, commit kiểm raw SHA. Publication raw SHA
mới ghi lúc nhận, nội dung liên kết với phase receipts trong data ZIP đã pin độc lập;
không coi checksum tự khai là source trust. Không yêu cầu gửi lại các files này.

Request pins giữ nguyên:

- Trang: `de22c1dc3f8d32442ddb8ae40a9f204b2c0bd67c29569c877090e99ca2faa2af`.
- Trí: `70ce0ef3a54cd1cfd05db89e1f381f0b2aca1f73eb31a0c42731c78a94938e4b`.

## Actual Java page/schema/count/relationship readback

Đọc trực tiếp ZIP streams, không giải nén dataset ra đĩa, không start Spark hoặc
ghi S3/Gold. Parse mọi trang Parquet; kiểm exact schema/footer/decoded count và
SHA/size từng part. Kiểm current IDs duy nhất, membership phủ đúng current IDs,
mỗi canonical có1primary, link endpoints có observation, rejects bằng0.

| Kết quả | rosy179 | ThanhTris |
| --- | ---: | ---: |
| Tháng UTC | 2018-10..2023-12 | 2013-08..2018-09 |
| Tháng đã đọc | 63/63 | 62/62 |
| Batch / run groups | 6 / 23 | 7 / 19 |
| source_observation rows | 1.362.812 | 1.122.941 |
| canonical_membership rows | 1.362.812 | 1.122.941 |
| source_link rows | 1.976 | 0 |
| reject_record rows | 0 | 0 |
| Tổng rows được giải mã | 2.727.600 | 2.245.882 |
| Canonical IDs cộng theo group | 1.361.639 | 1.122.941 |
| Payload reference bytes đã đọc | 635.408.427 | 522.659.149 |

Không gap/overlap trong affected months của từng worker; observed event months
phủ63/62tháng và không vượt tháng của batch. Counts canonical cộng theo group
không thay global dedup/boundary proof sau tích hợp. Tổng4.973.482rows là tổng
nhiều Silver datasets, không phải4.973.482trận động đất. Source links bao gồm
các quyết định candidate, không coi toàn bộ1.976links là accepted matches.

Checkpoint đợt đầu (giữ nguyên, không overwrite):

- Trang completed SHA: `ba4b3b0c667d27ed5ec41aafc531e572bae7f9ac18095d1bafb87d015e892809`;
  final checkpoint `f64726b62f8e97a2dee1aabf177e2cc3f6f7607d9cbde75fd552e5bd0fee2477`.
- Trí completed SHA: `8b4f6211b18943df7d2e6cb1fb29eb4e22c50d9c8aec3e4361aaa55b319d25ad`;
  final checkpoint `d4053b05b2a531ebb083f9cd4830212740c12a2e9c811696883453a0edc0f387`.

Artifacts/inputs/receipt-chain trong ignored `staging/dat03-integration-20261114/`.
Các lần rerun thuần đọc dùng directory mới; receipts không được cộng thành tháng
mới hoặc thay evidence failed. Không restore original S3keys hay tạo markers giả.

Rerun v2 với validation integral counts đã đạt cùng counts/coverage:

- Trang completed SHA: `617f630c6ea14a448b469622e9cdd727cd234023f2ed0584fa17d1f09da4bb89`.
- Trí completed SHA: `444d5cce731e52aa1506cbd337d9d99b408ca56dc4f8160ddc7d83093fefa1d8`.
- Final checkpoint SHA giữ nguyên như đợt đầu; không cộng thêm tháng từ rerun.

## Runtime và Gold capacity gate

Owner duyệt mở Docker Desktop, chỉ kiểm runtime trước; Docker tự khởi động5container
đã có: Trino, scheduler, MinIO, REST catalog, PostgreSQL. Tất cả healthy sau startup,
không DagRun active. Spark master/worker và Airflow API/DAG processor giữ stopped;
không build/pull/recreate hoặc bật thêm container.

Trino read-only xác nhận publication gốc:

- Namespace `iceberg.dat03_catalog_b6db356fb30e46f9a26a419ebacb5393`.
- Publication/bundle suffix: `57bf616cd76cd6e9ca6f417ec1b7182910a95da34059369a93dfb28dcb4ed19b`.
- Max event time `2013-07-31T23:59:29.340Z`.
- Physical event_current và event_source_bridge đều2.004.070rows theo snapshot pins;
  published natural/ROI event_count1.993.922. Không lấy natural count thay physical
  table count khi đo capacity.

Owner duyệt namespace **mới** `iceberg.dat03_unified_20261114` và own journal/lease
chỉ sau test/capacity/provenance gates. Chưa tạo namespace này trong đợt readback.

Capacity attempt1 chỉ đọc, runner riêng dưới scheduler staging, giữ deployed DAG
runner. Exit1 sau40,449s, không timeout/lake writes: Spark vectorized reader không
hỗ trợ `DELTA_LENGTH_BYTE_ARRAY` của publication_status do Trino ghi. Lỗi này
không chứng minh OOM hoặc thiếu dữ liệu. Giữ input/runner/logs/execution, không retry mù.
Đã bổ sung compatibility session-only row reader và regression test; không ALTER
properties hoặc nới gate. Kết quả attempt sau được bổ sung riêng khi terminal.

**Attempt v2 đã terminal, gate KHÔNG đạt:** controller trả `exit_code=143`,
`timed_out=true`, elapsed364,532s; không có `event-report.json`. Deadline360s
chỉ dừng process group do chính probe tạo; đã kiểm own process không còn.
Lỗi xuất hiện trong khởi tạo executor/BlockManager: `idWithoutTopologyInfo` null
và `RpcEndpointNotFoundException` cho `CoarseGrainedScheduler`. Chưa có kết quả
đo fingerprint hoặc workload. Không kết luận là OOM hay source thiếu dữ liệu;
container không bị OOMKilled không loại trừ mọi vấn đề tài nguyên trong JVM.
Root cause còn chưa xác định, không mở attempt khác hoặc bridge probe.

Giữ request, runner, private logs và terminal receipt riêng tại scheduler staging
`dat03-portable-capacity-20261114-v2`; không ghi đè attempt1. Runner SHA v2:
`dff151ea578d0f8cb8e13e78b05ecf743722d348f0c7cd2f32b49aae82fb43e2`.
Source event request SHA:
`84ef7c739a45c81f2c73a95deadf3a4448598ebc7f66a55ba9c1e8f5a3041242`.
Mọi thay đổi validation sau probe phải dùng runner pin mới khi nghiệm thu tiếp;
không coi artifact v2 đã chạy code chưa tồn tại lúc launch.

**Provenance trước importer:** worker Bronze URIs và run IDs khác owner dù
37 nội dung nguồn có SHA giống nhau. Cần mapping có version từ original URI
sang physical source đã xác minh, giữ cả hai identities trong journal. Không
chỉ dùng cùng SHA để bỏ qua request/window/catalog release hoặc giả original
S3 keys, `_SUCCESS`, Silver identity markers. Snapshot refs từ máy worker không
phải snapshot files đã restore trên máy coordinator.

Bước an toàn tiếp: chẩn đoán startup Spark hiện có bằng metadata/log đã lọc,
đo lại bằng attempt mới sau khi xác định nguyên nhân; hoàn thiện provenance
adapter và capacity gate trước mọi write vào namespace mới. Không tự restart,
build/pull/recreate, tăng memory/timeouts hoặc bật thêm service để vượt lỗi.

## Tests/checks và giới hạn

- 91Python unit tests metadata/planner/checkpoint/lineage/inventory PASS.
- 12Java portable readback +7capacity validation/reader-profile tests PASS.
- 42CI impact/suite/workflow tests PASS; docs/task checks được ghi theo lần chạy.
- Maven compile/package release17 đạt trên host JDK26; actual Spark capacity dùng
  Java17 trong scheduler hiện có. Unit tests không thay actual Spark/Gold acceptance.

**DAT-03 còn:** versioned Gold importer, full provenance/physical mapping/boundary/
revision/source-link/reconciliation, actual six-table commit/publish và Trino/rerun;
hai official ML exports và immutable release với verifier receipts của3máy thật.
Tất cả gold_published/portable_import_ready/official_export_ready/full closure flags
của readback receipts vẫn false. Không cấp scientific approval hoặc sửa lifecycle
để bỏ qua các gate này.
