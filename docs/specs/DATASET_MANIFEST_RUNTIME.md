# MLD-01 — Published snapshot và manifest runtime

`DatasetManifestJob` nhận một request JSON nhỏ, chạy SQL qua Trino, materialize
Iceberg `<catalog>.<ml_namespace>.dataset_manifest`. Identity và xử lý SQL nằm
trong Java; Python QA chỉ quản lý request, subprocess và shared run lease.

## Resolve và identity

- Caller chỉ định `gold_namespace`, `publication_id` dạng `pub_<sha256>` và
  `ml_namespace`; không chọn publication mới nhất.
- Resolve đúng một dòng `publication_status`, yêu cầu `PASSED`, bundle đủ sáu
  bảng và fact snapshot khớp bundle. Snapshot đã expire bị reject.
- Reserved namespace `operational_current_month` bị từ chối trước mọi SQL/write;
  exact event snapshot phải không có `gold.operational_policy_sha256`. Guard
  persisted này chặn cả metadata đổi tên/eligibility override. Xem
  [operational boundary](../runbooks/CURRENT_MONTH_GOLD_PUBLICATION.md); PASSED
  provisional không phải authorization cho official dataset/export.
- `gold_table_name` lưu logical serving name; `physical_gold_table_name` lưu
  bảng Iceberg có snapshot (`event_current`). Không time travel trên view.
- Identity canonical gồm snapshot, logical/physical table, publication/bundle
  checksum, split, period, cutoff, filter JSON và bảy version trong CON-03.
  Run ID và clock không tham gia hash. Hai namespace không dùng chung identity.
- Rerun đối chiếu toàn bộ identity với dòng đã có; giữ nguyên timestamps,
  build run và lifecycle. Nhiều dòng cùng ID hoặc payload khác bị reject.
- Downstream dùng `physical_gold_table_name FOR VERSION AS OF gold_snapshot_id`;
  không resolve alias `earthquake_event_current` hay `$refs.main` sau khi pin.

## Cấu hình và lifecycle

Ví dụ hai split nằm tại `config/ml/dataset_identity_v1.json`. Đây là cấu hình
QA; khi chạy pipeline phải đặt `code_version` bằng commit/tag thực tế,
`observation_cutoff_utc` và version bằng cấu hình đã khóa của run.
Cutoff là mốc quan sát nguồn, không phải `record_updated_at_utc` (thời gian ETL).
MLD-02 kiểm tra eligibility/source cutoff; MLD-01 chỉ pin và lưu cấu hình,
không tuyên bố đã xây catalog as-of lịch sử từ bảng current.

Manifest mới luôn `BUILDING`; fields audit/Mc/candidate/feature/export để null.
Không cung cấp lệnh nâng lifecycle trong smoke này. Chuyển `VALIDATED` cần đủ
gates MLD-02/03/04; `EXPORTED` cần bundle/checksum MLD-05; `REJECTED` terminal.
Logical schema và null policy đầy đủ xem `ML_DATA_MODEL.md` mục 4.
Physical table thêm `physical_gold_table_name`, `publication_bundle_sha256`,
`identity_payload_sha256` để kiểm tra pin/rerun; không lưu object data file path.

## Chạy kiểm tra

- `make test-dataset-manifest`: offline Java fixtures, không gọi nguồn.
- `make smoke-dataset-manifest`: opt-in integration, sau khi build JAR và
  publication GLD-04 QA còn đọc được. `GOLD_PUBLICATION_REFERENCE` chỉ định
  receipt; `DATASET_IDENTITY_CONFIG` chỉ định hai split JSON.
- `docker compose --env-file .env --profile gold run --rm --no-deps dataset-manifest-qa`
  là Compose command tương ứng. Service dùng Trino internal endpoint từ env.

Shared whole-run lease bắt buộc trước write, vì Iceberg không enforce primary
key. Mọi writer manifest phải dùng cùng lease. Nếu JVM/Trino timeout, ownership
được giữ lại: xác minh query đã dừng và đọc lại đúng dataset trước recovery;
không reset warehouse/metadata để xử lý lỗi.

## Phạm vi nghiệm thu

Hai manifest BUILDING từ Published pilot chứng minh identity/SQL/rerun.
USGS-only pilot không chứng minh coverage JMA hay Mc/window/feature. Phần còn
thiếu thuộc MLD-02 (audit/Mc), MLD-03 (candidate windows), MLD-04 (features),
MLD-05 (export); không được coi manifest này là Colab-ready.
