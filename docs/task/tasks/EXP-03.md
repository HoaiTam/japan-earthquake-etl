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

## Giao việc hiện hành — 11/11/2026

- **Owner:** ThanhTris; **status:** In Progress.
- **Làm phần gì / dùng để làm gì:** Adaptive và multi-sequence.
- **Bắt đầu độc lập:** Tái dùng #113/#116; bổ sung actual Colab notebook/output/session cho SEC-01 sau local gate.
- **Đầu ra và điều kiện Done:** Sau EXP-02 official: adaptive REPRODUCTION bundle, raw/resolved membership, ambiguous/nested counts; khóa config trước extension.
- **Lịch:** chuẩn bị 11–15/11, nghiệm thu tuần9 16–22/11; xem
  [kế hoạch và setup](../WEEK_9_REPLAN_20261111.md). Giữ hard dependencies,
  scope/effort lịch sử; các mục tuần cũ dưới đây là lịch sử.

Owner yêu cầu11/11: In Progress → Needs Update cho phần đã triển khai nhưng
chưa đủ acceptance. Không khẳng định từng Done toàn task hoặc mất code.
[Đối soát](../../evidence/TASK_STATUS_AUDIT_20261111.md).

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
- **03/11/2026 — gate hardening & exact run ID pinning (Round 3 review):** Hoàn thiện 2 điểm P2 theo review của @HoaiTam (PR #113):
  1. Phân biệt rõ chế độ nghiệm thu artifact vs report verification: Trong chế độ `REPORT_ONLY_VERIFIED` (`handoff_archive_exists=false`), gate kiểm tra chặt chẽ metadata, binding pilot/code/config/run và trạng thái `local_smoke_status=PASSED`. Nếu truyền `EXP02_HANDOFF_ARCHIVE` nhưng file không tồn tại, hoặc report EXP-02 có trạng thái `FAILED`, gate lập tức fail-closed.
  2. Loại bỏ hoàn toàn việc sort mtime mù khi resolve experiment output: Inspector tạo và truyền `experiment_run_id` xác định vào notebook kernel trước khi chạy; chỉ nghiệm thu đúng run đó và đối chiếu cấu trúc manifest JSON.
  3. Bổ sung thêm 8 regression tests nâng tổng số lên **40 tests PASS** với fresh Jupyter kernel (đạt 21/21 CI checks).
  4. Xuất evidence mới tại [EXP-03-local-smoke.json](../../evidence/EXP-03-local-smoke.json) với status `REPORT_ONLY_VERIFIED`. Giữ task `In Progress` chờ PR #114 merge và nghiệm thu Colab cloud.
- **17/11/2026 — Colab portability, host parity & SEC-01 Drive artifacts (`ThanhTris`):** Hoàn tất local `exp01-colab.py local-pack` (`001e33b`, `25.6583s`), local adaptive fresh-kernel inspection trên real pilot (`5.2503s`, `pilot_run_1a7b6edb9bb94ccf9caaac99e124f2b5`), chạy thực tế [exp_01_colab_bootstrap.ipynb](../../../notebooks/exp_01_colab_bootstrap.ipynb) trên Google Colab (`Python 3.13.18` Linux CPU, `COLAB_HARNESS_PASSED`, parity `WINDOW`/`DBSCAN`/`HDBSCAN_GLOBAL` `PASSED` với `279` rows / `2` mainshocks), recheck parity trên host và upload `4` artifact sạch vào Drive (`type=user` ONLY, `anyone=0`, `domain=0`) cho [SEC-01](./SEC-01.md) (PR #119). Ghi nhận tại [THANHTRIS_COLAB_PORTABILITY_20261117.md](../../evidence/THANHTRIS_COLAB_PORTABILITY_20261117.md) và [JSON](../../evidence/THANHTRIS_COLAB_PORTABILITY_20261117.json); giữ `Needs Update` (`scientific_approval=false`) vì vẫn chờ [EXP-02](./EXP-02.md) official và official adaptive run trên `DAT-03`.
- **18/11/2026 — Official REPRODUCTION v2 run & sibling MLI-03 boundary (`ThanhTris`):** Mở rộng `notebooks/adaptive_hdbscan.py` (`shard_output_dir` đồng cấp + `mli03_import_boundary`), `scripts/run-exp03-official.py` (clean provenance + fail-closed handoff verification + cảnh báo boundary import) và `ResultBundleContractTest.java`; chạy **REPRODUCTION v2 (`exp-03-official-v2`)** trên clean commit `451a615` (`194` mainshocks, `156.710` rows, `2` shards): thực thi `resolve_multi_sequence_and_nested_mainshocks` toàn cục xuyên shard rồi ghi full-split bundle (`exp_run_b851f45893dc1d640f5c5940e1c08bdd`, JSON SHA `9333d8c921c4d964aca4c86619796e29c1da0d811de6516dafd790b2f711f9b1`) và sibling shards `hdbscan_adaptive_shards/<shard_id>/` (`exp_run_ad99c797cc01023db409ea9242310878` / `exp_run_62508538ed0ff9b15ddde6cf4a072fd6`; `131` thành công / `63` `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `98.795` raw $\rightarrow$ `88.222` resolved unique POST aftershocks, `10.573` deduplicated, `0` ambiguous, `23` nested mainshocks). Full-split dùng để đánh giá khoa học (`EXP-04`, không đi qua `TrustedDatasetResolver` vì thiếu `collection_shard_id`), còn 2 sibling shards **mới đạt readback (`MlResultBundleReader.read`) và đối soát Parquet toàn cục, chưa được nghiệm thu import qua `MLI-03`** (`shard_00000` giữ `5.218` và `shard_00001` giữ `4.387` singleton có nhãn toàn cục `MULTIPLE`/`AMBIGUOUS`, nên `ResultBundleContract` chặn `IMP_SCHEMA_MISMATCH` khi kiểm đơn lẻ từng bundle — chứng minh qua Java test `crossShardAdaptiveOverlapPassesReaderWhileContractAndResolverBoundariesHold`, follow-up import thuộc [MLI-03](./MLI-03.md)). Giữ run v1 `exp_run_70637f429d590e2ba8258f56b618ba85` làm historical reference. Xem [EXP-03_OFFICIAL_REPRODUCTION.md](../../evidence/EXP-03_OFFICIAL_REPRODUCTION.md) và [JSON](../../evidence/EXP-03_OFFICIAL_REPRODUCTION.json). Chuyển `EXP-03` sang `Done` (`scientific_approval=false`; reviewer giữ `unassigned`).
- **Evidence / PR:** [EXP-03 evidence](../../evidence/EXP-03.md), [EXP-03 Official Reproduction](../../evidence/EXP-03_OFFICIAL_REPRODUCTION.md), [THANHTRIS Colab portability](../../evidence/THANHTRIS_COLAB_PORTABILITY_20261117.md), [PR #86](https://github.com/HoaiTam/japan-earthquake-etl/pull/86) đã merge implementation, [PR #113](https://github.com/HoaiTam/japan-earthquake-etl/pull/113) local smoke gate review round 2. Hard gate EXP-02 đã được liên kết với PR #114 và PR #139; follow-up import shard adaptive xuyên shard thuộc [MLI-03](./MLI-03.md).
- **Kỹ năng phù hợp:** HDBSCAN, parameter policies, entity resolution

## Checklist bàn giao

- [x] Deliverable và test/evidence tồn tại trong repository.
- [x] Acceptance toàn task: unit/contract rules, Colab/local fresh-kernel parity và official `REPRODUCTION` (`EXP-02` $\rightarrow$ `EXP-03`) đã hoàn tất.
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
