---
task_id: "DAT-02"
status: "Backlog"
week: 5
block: "A - Hợp đồng dữ liệu"
workstream: "Jupyter development pilot"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "HoaiTam"
reviewer: "unassigned"
dependencies: ["MLD-03", "MLD-04"]
---

# DAT-02 - Tạo dataset pilot nhỏ để test trên Jupyter

## Mục đích

Cung cấp bộ dữ liệu thật nhỏ, tái lập được để kiểm tra feature, loader và
Window/DBSCAN/HDBSCAN trên máy trước khi chuyển code/bundle sang Google Colab.
Không cần tải đủ 40 năm hoặc chờ dataset nghiên cứu toàn kỳ EXPORTED.

Khác DAT-01 (sample raw cho ETL), đây là **development feature bundle** có
candidate lineage. Không thay dataset export chính thức của MLD-05.

## Phạm vi công việc

- Chọn cố định 1–3 mainshock JMA UNIFIED, ưu tiên reproduction period;
  depth 50–200 km, magnitude >5.5. Ghi IDs và lý do chọn trước khi fit,
  không chọn sample dựa vào cluster “đẹp”.
- Pin Gold Published snapshot, MLD-03 candidate/audit/Mc artifacts và hashes.
  Reuse dữ liệu đã verify, không lấy raw Bronze rồi tự normalize trong notebook.
- Giữ mọi candidate của các mainshock đã chọn trong scope input có sẵn.
  Báo coverage pre/post thực tế so với window model; giữ COMPLETE/PARTIAL
  và missing ranges. Pilot 14 ngày không được gọi COMPLETE nếu window dài hơn.
- Dùng Java/Spark và feature implementation/config MLD-04 để tạo/test-select
  feature; không viết lại phép chuyển tọa độ/scaling trong Python.
- Giảm kích thước bằng giảm số mainshock, không random sample/drop candidate
  hoặc silent truncate window. Vượt resource guard thì reject/ghi reason
  và chọn lại scope có version trước fit.
- Export test-only bundle riêng với pilot_id/checksums/schema/counts, không
  cập nhật dataset_status chính thức hoặc registry EXPORTED.
- Tạo notebook kiểm tra dữ liệu Jupyter (load/count/schema/finite/zero-self/
  coverage), runbook và evidence. Fit ba thuật toán thuộc EXP-02.

## Thành phần cần có / Deliverable

- Versioned selection config + Spark pilot-export entry point.
- Bundle staging ngoài Git: features-part-*.parquet, mainshocks.parquet,
  pilot_manifest.json, coverage_report.json, checksums.sha256.
- Manifest có artifact_kind=DEVELOPMENT_PILOT, allowed_usage=DEVELOPMENT_ONLY,
  research_ready=false; exact pins/config/feature version và source_dataset_id
  chỉ là lineage, không giả dataset đó đã EXPORTED.
- Notebook inspection .ipynb không có raw dump/secret/output lớn; small synthetic
  fixtures cho checksum/schema/grain/coverage failure và loader tests.
- Runbook cách chuẩn bị nguồn, export một lần, mở Jupyter, đọc kết quả mong đợi
  và bàn giao bundle cho EXP-01/02. Chỉ thêm lệnh Make khi implementation tồn tại.

## Tiêu chí hoàn thành

- [ ] Có ít nhất một mainshock thật đúng rule, candidate IDs truy về pinned Gold.
- [ ] Feature/count/grain/checksum readback đạt; mỗi window giữ một self vector zero.
- [ ] Missing coverage công khai; PARTIAL chỉ DEVELOPMENT_ONLY, không nhận research-ready.
- [ ] Rerun cùng identity tái lập nội dung; changed input/config tạo pilot identity mới.
- [ ] Jupyter inspection chạy trên máy với bundle local, không gọi MinIO/Docker/API
      ở mỗi cell; loader reject checksum/schema/NaN/duplicate/missing-self lỗi.
- [ ] Không nâng source dataset BUILDING thành VALIDATED/EXPORTED; không tạo
      official _SUCCESS.json, RESULT_READY/CANDIDATE hoặc sửa Gold/ml.*.
- [ ] Handoff đủ cho EXP-01/02 chạy cùng bundle mà không cần pipeline credentials.

## Hard dependency

- [MLD-03](./MLD-03.md) — candidate windows và exact lineage.
- [MLD-04](./MLD-04.md) — shared feature/scaling implementation.

MLD-04 hiện Ready, chưa Done, nên DAT-02 Backlog. Có thể chọn scope/pin metadata,
viết manifest/layout/fixtures/inspection interface ngay; chỉ real feature
acceptance mới cần implementation upstream. Không chờ MLD-05 full research
export hoặc EXP-01 để hoàn thành dataset pilot, tránh dependency vòng.

## Phân công và phối hợp

HoaiTam, 6h trong [tuần 5](../WEEK_5_PARALLEL_PLAN.md). Nhận feature contract
fixture từ rosy179 sớm; giữ interface thống nhất với MLD-04. EXP-01 viết
loader/harness bằng fixture song song, EXP-02 viết thuật toán bằng synthetic
windows trước khi có pilot thật.

Branch triển khai: feat/dat-02-jupyter-pilot-dataset từ main mới nhất.
Mới thêm task, chưa chạy export/notebook; chỉ đổi In Progress khi thực hiện.

## Ranh giới

- Không coi sample đại diện toàn reproduction/extension hoặc ground truth.
- Không fit/tune model trong exporter; không tune bằng extension sample.
- Không thêm enum PILOT vào ML lifecycle hoặc đổi CON-03 schema/grain.
- MLD-05 vẫn sở hữu export nghiên cứu EXPORTED; EXP-01/02 sở hữu harness/
  algorithm smoke và local-to-Colab gate; MLI-03 vẫn reject pilot official import.
- Không tải full history, restart/build Docker trong mọi test hoặc commit data lớn.

## Theo dõi

- **Trạng thái:** Backlog
- **Assignee:** HoaiTam
- **Reviewer:** unassigned
- **Evidence / PR:** Chưa có; mới định nghĩa task, chưa tạo dataset thật.

## Checklist bàn giao

- [ ] Exporter/fixtures/notebook/runbook tồn tại và acceptance đạt.
- [ ] Evidence real pilot inspection có versions/pins/counts/checksums/coverage.
- [ ] Không secret/raw dump/build artifact; status/index đồng bộ.
- [ ] Giới hạn và downstream task IDs được ghi rõ.

## Tài liệu liên quan

- [Jupyter pilot trước Colab](../../specs/JUPYTER_PILOT_WORKFLOW.md)
- [ML data model/lifecycle](../../specs/ML_DATA_MODEL.md)
- [MLD-03 windows](../../specs/MAINSHOCK_AND_CANDIDATE_WINDOWS.md)
- [MLD-04](./MLD-04.md), [MLD-05](./MLD-05.md)
- [EXP-01](./EXP-01.md), [EXP-02](./EXP-02.md)
