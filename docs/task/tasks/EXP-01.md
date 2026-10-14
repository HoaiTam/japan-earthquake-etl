---
task_id: "EXP-01"
status: "Backlog"
week: 5
block: "H - Experiment"
workstream: "Jupyter-first harness"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "rosy179"
reviewer: "unassigned"
dependencies: ["CON-03", "DAT-02"]
---

# EXP-01 - Tạo Jupyter harness và chuẩn bị chuyển sang Colab

## Mục đích

Tạo notebook/harness tái lập được để mọi model đọc cùng bundle, ghi artifact
máy đọc được và không phụ thuộc output hiển thị trên màn hình. Jupyter local
là runtime đầu tiên; dùng lại cùng module/notebook khi chuyển sang Colab sau
local gate EXP-02, không viết hai model pipelines.

## Phạm vi công việc

- Tạo shared module + thin Jupyter notebook; loader explicit development/official,
  verify pilot hoặc dataset manifest, schema, row count và checksum.
- Development chỉ nhận DAT-02 bundle hợp lệ; official chỉ nhận MLD-05 EXPORTED,
  không tự convert pilot_manifest thành dataset_manifest hoặc official result.
- Pin Python/package versions, feature columns, seed áp dụng được và runtime metadata.
- Tách pilot_run_id/pilot_result_manifest.json và official experiment_run_id/result
  writer; chỉ official run hợp lệ mới ghi _SUCCESS.json cuối. Pilot không ghi ml.*.
- Hỗ trợ Google Drive path bằng parameter; không hard-code account path/credential.

## Deliverable

- Jupyter notebook, shared Python module và requirements-lock.txt; Colab entry/path
  adapter dùng lại code, không chạy cloud trước EXP-02 local gate.
- Feature bundle fixture và smoke run không cần dữ liệu thật lớn.
- Output skeleton cho memberships, sequence summary, metrics/config và completion manifest.

## Tiêu chí hoàn thành

- [ ] Bundle checksum/schema sai bị dừng trước fit model.
- [ ] Package/code/config version và identity theo mode xuất hiện trong mọi artifact.
- [ ] Restart kernel → run all trên Jupyter đạt với DAT-02 pilot; không dựa state cũ.
- [ ] Loader/writer tách pilot/official; pilot không phát _SUCCESS hoặc status official.
- [ ] Colab path adapter có offline tests và yêu cầu local-smoke PASSED;
      smoke cloud sau EXP-02 local, không tạo dependency vòng cho harness.
- [ ] `_SUCCESS.json` không được ghi nếu output chưa hoàn tất/checksum chưa tính.
- [ ] Notebook không chứa MinIO/Airflow/Drive credential hoặc local absolute path.

## Hard dependency

- [CON-03](./CON-03.md)
- [DAT-02](./DAT-02.md)

MLD-05 không chặn phát triển/nghiệm thu local harness: official export loader
được contract-test bằng fixture hợp lệ/lỗi. Official research runs vẫn cần
MLD-05 và EXP-02 local gate; không nhận pilot là dataset EXPORTED.

## Cách làm song song

Viết loader/writer bằng DAT-02 schema/fixture ngay. Local acceptance dùng pilot
thật đã verify; Colab adapter contract-test offline. Không đợi EXP-02 hoặc
MLD-05 để hoàn tất local harness. Cloud smoke nằm trong EXP-02 giai đoạn sau.

## Ranh giới

- Không chọn tham số tối ưu hoặc tuyên bố model tốt nhất trong task này.
- Không cho notebook ghi trực tiếp Gold/Iceberg/MinIO.
- Không dùng output cell làm artifact duy nhất.

## Phân công tuần 5

- **Owner / effort:** rosy179, 5h Core; [kế hoạch và handoff](../WEEK_5_PARALLEL_PLAN.md).
- **Làm gì / có gì:** Loader/writer, môi trường/package lock, dataset/run/config metadata và completion marker ghi cuối; hỗ trợ path qua parameter.
- **Dùng để làm gì / output:** Harness tái lập cho EXP-02; development output tách riêng, official output đúng MLI-01 mới eligible importer; không chỉ lưu output cell.
- **Bắt đầu ngay / gate:** Làm bằng feature bundle fixture ngay; local smoke DAT-02 trước Done, official loader contract-test riêng. Cloud run chờ local gate EXP-02; không claim research run từ pilot.
- **Theo dõi:** Phân công không có nghĩa đã triển khai; chỉ đổi In Progress khi bắt đầu, Done khi đủ acceptance/evidence. Reviewer hiện unassigned, chưa có approval.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** rosy179
- **Reviewer:** Chưa ghi lại
- **Evidence / PR:** Chưa có
- **Kỹ năng phù hợp:** Python, Jupyter, Colab, reproducible environments

## Checklist bàn giao

- [ ] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance criteria đã được kiểm tra.
- [ ] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

- [DAT-02](./DAT-02.md)
- [Jupyter pilot workflow](../../specs/JUPYTER_PILOT_WORKFLOW.md)

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [MLD-05](./MLD-05.md)
