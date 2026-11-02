---
task_id: "EXP-03"
status: "In Progress"
week: 5
block: "H - Experiment"
workstream: "Adaptive HDBSCAN"
scope: "Core"
priority: "P0"
effort_hours: 6
assignee: "ThanhTris"
reviewer: "unassigned"
dependencies: ["EXP-02"]
---

# EXP-03 - Xây HDBSCAN adaptive và giải quyết multi-sequence

## Mục đích

So sánh rule thích nghi với global rule mà không chọn tham số thủ công cho từng
sequence, đồng thời tránh double count event xuất hiện trong nhiều window.

## Phạm vi công việc

- Xây adaptive rule từ candidate count, background rate, mainshock magnitude, window size và/hoặc `Mc` group.
- Khóa `model_config_version` trên reproduction period trước khi chạy extension.
- Giữ raw memberships cho mọi mainshock window; tạo resolution view/status theo probability → normalized distance → ambiguous.
- Gắn nested mainshock/parent evidence khi mainshock nằm trong sequence khác.

## Deliverable

- Versioned adaptive parameter resolver và tests.
- HDBSCAN adaptive run artifacts trên reproduction dataset.
- Multi-sequence/nested-mainshock resolution rules và before/after counts.

## Tiêu chí hoàn thành

- [x] Adaptive rule deterministic và áp dụng đồng nhất, không có per-sequence manual override.
- [x] Raw membership grain vẫn giữ mọi cặp mainshock/candidate.
- [x] Aggregate unique aftershock không double count mà không ghi resolution status.
- [x] Tie không giải được phải là `AMBIGUOUS_MULTI_SEQUENCE`, không chọn tùy ý.
- [x] Rule/config được khóa trước `EXP-05`.

## Hard dependency

- [EXP-02](./EXP-02.md)

## Cách làm song song

Resolution rule có thể phát triển/test bằng fixture multi-window trong lúc
adaptive parameter analysis đang chạy, miễn cùng output contract.

## Ranh giới

- Không tune adaptive rule bằng extension period.
- Không xóa raw membership chỉ vì event được resolve sang sequence khác.
- Không tuyên bố adaptive tốt hơn trước khi `EXP-04` đánh giá.

## Tuần 7 — ThanhTris: tiếp nhận adaptive và multi-sequence

Theo [kế hoạch tuần7](../WEEK_7_COLAB_PLAN.md), chuyển owner sang ThanhTris;
rosy179 trong evidence/PR#86 vẫn là tác giả mốc triển khai trước. Không sửa
evidence cũ thành nghiệm thu của người mới. Giữ In Progress và hard gate EXP-02.

1. Đọc [setup](../../runbooks/WEEK_7_COLAB_SETUP.md); làm fixture/unit tests
   ngay trên module có sẵn, không đợi baseline cloud hoặc official DAT-03.
2. Chạy local adaptive fresh kernel bằng pilot thật và baseline local gate
   trước cloud; không lấy isolated_exec/fallback làm fresh-kernel acceptance.
3. Chạy Colab adaptive, giữ raw membership, đối soát unique resolved/noise/
   ambiguous/nested counts; lưu config/SHA, session metadata và pilot marker.
4. COL-04 bổ sung regression còn thiếu; EXP-04 stability/evaluation và MLI-04
   report template riêng. Không tự nâng PARTIAL thành official reproduction.

## Theo dõi

- **Trạng thái:** In Progress
- **Assignee:** ThanhTris
- **Reviewer:** unassigned
- **02/11/2026 — local smoke gate update & real pilot identity verification:** Hoàn thiện theo review của @HoaiTam (PR #113):
  1. Loại bỏ fallback ngầm; gate fail ngay lập tức (`FileNotFoundError: PILOT_BUNDLE_NOT_FOUND`) nếu đường dẫn pilot thiếu hoặc không hợp lệ. Khắc phục lỗi CI runner bằng cách truyền fixture pilot rõ ràng trong unit tests notebook.
  2. Xác minh và ghi nhận danh tính pilot thật vào evidence JSON: `pilot_id` (`pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c`), 279 dòng đặc trưng, 2 mainshock windows, `features_parquet_sha256` (`3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6`), `checksums_index_sha256` (`310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502`), `is_real_pilot=true`.
  3. Siết chặt fail-closed hard gate EXP-02 (đối chiếu trực tiếp evidence report `docs/evidence/EXP-02_COLAB_PARITY_20261102.json`, pilot ID pin, feature SHA pin, và handoff archive pin `d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333`, `prerequisite_verified=true`) trước khi cấp phép `colab_migration_allowed=true`. Siết chặt `find_latest_experiment_run` kiểm tra khớp tuyệt đối `experiment_run_id` với subfolder và manifest thay vì sort theo mtime mù.
  4. Bổ sung 9 regression tests trong `tests/test_adaptive_hdbscan.py` (nâng tổng số lên 32 tests PASS).
  5. Thực thi fresh kernel `nbclient` Python 3.12.16 trên bundle pilot thật thành công; xuất bằng chứng tại [EXP-03-local-smoke.json](../../evidence/EXP-03-local-smoke.json). Giữ `In Progress` cho cloud run.
- **Evidence / PR:** [EXP-03 evidence](../../evidence/EXP-03.md), [PR #86](https://github.com/HoaiTam/japan-earthquake-etl/pull/86) đã merge implementation, [PR #113](https://github.com/HoaiTam/japan-earthquake-etl/pull/113) local smoke gate review round 2. Hard gate EXP-02 đã được liên kết với PR #114.
- **Kỹ năng phù hợp:** HDBSCAN, parameter policies, entity resolution

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [ ] Acceptance toàn task: unit/contract rules đã kiểm; còn reproduction run artifacts và nghiệm thu notebook fresh kernel trên input hợp lệ sau EXP-02.
- [x] Contract/docs đã cập nhật và không chứa secret/data dump.
- [ ] Reviewer độc lập xác nhận (khuyến nghị cho P0); reviewer unassigned, chưa có approval.

## Phân công tuần 5 — đồng bộ 22/10/2026

- Owner rosy179, 6h Core. Lịch ban đầu tuần 7; bảng phân công gần nhất kéo
  task lên tuần 5, không thêm task/effort hoặc giảm hard dependency.
- [EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](./EXP-02.md) còn Colab
  parity/official acceptance. Giữ task này In Progress; fallback report không
  được dùng để mở Colab gate hoặc tuyên bố adaptive tốt hơn.

## Tài liệu liên quan

- [Roadmap HDBSCAN](../HDBSCAN_WORKSTREAM.md)
- [EXP-02](./EXP-02.md)
