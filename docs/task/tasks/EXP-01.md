---
task_id: "EXP-01"
status: "Backlog"
week: 6
block: "H - Experiment"
workstream: "Colab harness"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "unassigned"
reviewer: "unassigned"
dependencies: ["CON-03", "MLD-05"]
---

# EXP-01 - Tạo Colab harness và khóa môi trường thí nghiệm

## Mục đích

Tạo notebook/harness tái lập được để mọi model đọc cùng bundle, ghi artifact
máy đọc được và không phụ thuộc output hiển thị trên màn hình.

## Phạm vi công việc

- Tạo notebook/module loader verify dataset manifest, schema, row count và checksum.
- Pin Python/package versions, feature columns, seed áp dụng được và runtime metadata.
- Chuẩn hóa `experiment_run_id`, config, metrics, membership/summary writer và `_SUCCESS.json` chỉ ghi cuối.
- Hỗ trợ Google Drive path bằng parameter; không hard-code account path/credential.

## Deliverable

- Colab notebook hoặc Python module dùng lại được cùng `requirements-lock.txt`.
- Feature bundle fixture và smoke run không cần dữ liệu thật lớn.
- Output skeleton cho memberships, sequence summary, metrics/config và completion manifest.

## Tiêu chí hoàn thành

- [ ] Bundle checksum/schema sai bị dừng trước fit model.
- [ ] Package/code/config version và dataset ID xuất hiện trong mọi run artifact.
- [ ] `_SUCCESS.json` không được ghi nếu output chưa hoàn tất/checksum chưa tính.
- [ ] Notebook không chứa MinIO/Airflow/Drive credential hoặc local absolute path.

## Hard dependency

- [CON-03](./CON-03.md)
- [MLD-05](./MLD-05.md)

## Cách làm song song

Notebook loader/writer được phát triển bằng fixture bundle trước `MLD-05`;
chỉ smoke với export contract thật mới chặn `Done`.

## Ranh giới

- Không chọn tham số tối ưu hoặc tuyên bố model tốt nhất trong task này.
- Không cho notebook ghi trực tiếp Gold/Iceberg/MinIO.
- Không dùng output cell làm artifact duy nhất.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** Chưa ghi lại
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Python, Colab, reproducible environments

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [MLD-05](./MLD-05.md)

