---
task_id: "EXP-01"
status: "Done"
week: 5
block: "H - Experiment"
workstream: "Jupyter-first harness"
scope: "Core"
priority: "P0"
effort_hours: 5
assignee: "HoaiTam"
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

- [x] Bundle checksum/schema sai bị dừng trước fit model.
- [x] Package/code/config version và identity theo mode xuất hiện trong mọi artifact.
- [x] Restart kernel → run all trên Jupyter đạt với DAT-02 pilot; không dựa state cũ.
- [x] Loader/writer tách pilot/official; pilot không phát _SUCCESS hoặc status official.
- [x] Colab path adapter có offline tests và yêu cầu local-smoke PASSED;
      smoke cloud sau EXP-02 local, không tạo dependency vòng cho harness.
- [x] `_SUCCESS.json` không được ghi nếu output chưa hoàn tất/checksum chưa tính.
- [x] Notebook không chứa MinIO/Airflow/Drive credential hoặc local absolute path.

## Hard dependency

- [CON-03](./CON-03.md)
- [DAT-02](./DAT-02.md)

MLD-05 không chặn phát triển/nghiệm thu local harness: official export loader
được contract-test bằng fixture hợp lệ/lỗi. Official research runs vẫn cần
MLD-05 và EXP-02 local gate; không nhận pilot là dataset EXPORTED.

## Cách làm song song

Viết loader/writer bằng DAT-02 schema/fixture ngay. Local acceptance dùng pilot
thật đã verify; Colab adapter có contract-test offline. Không đợi EXP-02 hoặc
MLD-05 official acceptance để hoàn tất local harness. Tuần7 bổ sung cloud
portability trong EXP-01 sau local report thật của ba baseline; experiment
nghiên cứu/scientific vẫn nằm trong EXP-02 và các task sau, không tạo vòng chờ.

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

## Tuần 7 — HoaiTam: hoàn thiện môi trường Colab

Theo [phân công mới](../WEEK_7_COLAB_PLAN.md), chuyển owner từ rosy179 sang
HoaiTam và mở lại Needs Update cho cloud setup; local nghiệm thu cũ giữ nguyên.
Tuần metadata5 là lịch triển khai đầu, phần carryover tuần7 ghi ở kế hoạch mới.
Đọc [setup](../../runbooks/WEEK_7_COLAB_SETUP.md) trước khi triển khai.

- [x] Bootstrap Colab CPU dùng cùng shared modules/requirements/pilot pins;
      không synthetic fallback khi path input thật thiếu/sai.
- [x] Pin và ghi package/code/config thực tế; xử lý pip conflict và plotting
      dependency bằng PR có test, không đổi version im lặng.
- [x] Kiểm local report thật gắn đúng pilot/feature/code/config trước cloud fit;
      không tự gán flag fresh kernel hoặc tạo PASSED JSON.
- [x] Fresh Colab session → Run all, loader/checksum gate và output/readback
      có evidence; chưa có quyền/session thì ghi pending, không claim đã chạy.

Không chặn EDA hoặc fixture work của hai khối khác; reuse local harness đã
Done thay vì viết lại model pipeline. Không tự công nhận official research.

## Theo dõi

- **Trạng thái:** Done
- **01/11 — cloud implementation:** HoaiTam triển khai bootstrap CPU cô lập,
  exact source/config/package binding, local gate và notebook dùng lại shared
  modules trên branch `feat/exp-01-colab-bootstrap`. Đã nghiệm thu Colab CPU
  mới bằng Run all01/11; giữ riêng bằng chứng cloud và local cũ.
- **Evidence bổ sung:** [EXP-01 Colab01/11](../../evidence/EXP-01_COLAB_20261101.md),
  [runbook bootstrap](../../runbooks/EXP-01_COLAB_BOOTSTRAP.md). Local fresh
  kernel thật279rows/2mainshocks,71harness/bootstrap và19baseline tests đạt.
  Attempt cloud đầu dừng trước fit ở tạo venv; đã sửa bootstrap để không phụ
  thuộc ensurepip; v4 fresh cloud Run all đạt ba parity và25file output
  download/readback. Python local3.13.3/cloud3.13.18, không scientific approval.
- **Assignee:** HoaiTam
- **Reviewer:** unassigned — chưa có approval cho phần cloud mới
- **Evidence / PR:** Đã khắc phục toàn bộ review comments từ HoaiTam tại PR #83 ([comment-6229834764](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6229834764), [comment-6231696916](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6231696916), [comment-6244707869](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6244707869) và [comment-6244865924](https://github.com/HoaiTam/japan-earthquake-etl/pull/83#issuecomment-6244865924)). Đã giải quyết triệt để vấn đề ràng buộc retry run ID / config / seed, lineage guard trên memberships/summaries chạy trước retry reuse, xác thực marker status, checksums map không rỗng và bao phủ đủ standard required files, verify toàn bộ file hash trên đĩa. Bổ sung regression tests (tổng cộng 44 Python tests và 15 Java tests PASS). Chi tiết tại [docs/evidence/EXP-01.md](../../evidence/EXP-01.md).
- **Kỹ năng phù hợp:** Python, Jupyter, Colab, reproducible environments

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance local harness cũ đã được kiểm tra, evidence giữ nguyên.
- [x] Acceptance bổ sung cloud setup tuần7 đã đạt Run all/parity/readback thật.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập được khuyến nghị cho P0.

## Tài liệu liên quan

### Handoff DAT-02 — 2026-10-15

Inspection module `notebooks/pilot_bundle.py` có `load_bundle(exact_directory)`:
trả manifest/coverage, PyArrow features/mainshocks và summary sau khi kiểm
checksum/schema/grain/finite/self/coverage. Fixture nhỏ tạo trong
`tests/test_pilot_bundle.py`, không cần Docker/API; real bundle chia sẻ riêng
bằng `make pack-pilot` vì staging ngoài Git. Xem
[runbook và ranh giới](../../runbooks/DAT-02_JUPYTER_PILOT.md).

Reuse loader/fixture này thay vì viết lại feature math. Notebook DAT-02 chỉ
inspection; chưa thay shared experiment harness, official loader/result writer,
full dependency lock hoặc Colab adapter của EXP-01. Chưa fit model, chưa mở
Colab gate EXP-02; không có approval reviewer.

- [DAT-02](./DAT-02.md)
- [Jupyter pilot workflow](../../specs/JUPYTER_PILOT_WORKFLOW.md)

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [ML logical data model](../../specs/ML_DATA_MODEL.md)
- [MLD-05](./MLD-05.md)
