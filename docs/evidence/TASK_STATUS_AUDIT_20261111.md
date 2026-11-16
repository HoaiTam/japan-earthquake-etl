# Đối soát task/PR — 11/11/2026

## Nguồn và ranh giới

Main `35582a050b47d47c17e3163245e91cec65f0575b`; GitHub PR metadata thực ngày
11/11, acceptance/evidence trong task và kết quả review trong phiên chat này.
Đây là audit docs/trạng thái, không chạy ETL mới, actual Colab mới hoặc official
Trino readbacks. Không sửa evidence lịch sử để giả runtime nghiệm thu mới.

Owner yêu cầu: task đã merge và hoàn thiện chuyển Done; phần In Progress còn
thiếu chuyển Needs Update rồi phân lại. Acceptance vẫn quyết định Done, không
phải merge status. Needs Update của năm task dở là nhãn theo yêu cầu owner,
không khẳng định chúng từng Done toàn task hoặc có regression mới.

## Quyết định

| Task | Trước → sau | Bằng chứng đã có | Còn thiếu / lý do |
|---|---|---|---|
| COL-03 | In Progress → Done | [#117](https://github.com/HoaiTam/japan-earthquake-etl/pull/117) merged; reviewed head2fc5129,39/39 tests,21/21 CI; [pilot evidence](./COL-03_BASELINE_METRICS_PLOTS_20261102.md) | Đủ scope pilot; không nhận official scientific acceptance thay EXP-04 |
| DAT-03 | In Progress → Needs Update | [#122](https://github.com/HoaiTam/japan-earthquake-etl/pull/122), [#123](https://github.com/HoaiTam/japan-earthquake-etl/pull/123) merged; [288-month handoffs](./DAT-03_INTEGRATION_EVIDENCE_288_MONTHS.md) | Transitive closure/payload, unified catalog/Gold/Trino, hai official exports và receipts3máy chưa đủ |
| EXP-02 | In Progress → Needs Update | [#114](https://github.com/HoaiTam/japan-earthquake-etl/pull/114) merged; [Colab pilot parity](./EXP-02_COLAB_PARITY_20261102.md) | Official reproduction trên DAT-03 EXPORTED chưa có |
| EXP-03 | In Progress → Needs Update | [#113](https://github.com/HoaiTam/japan-earthquake-etl/pull/113), [#116](https://github.com/HoaiTam/japan-earthquake-etl/pull/116) merged; [local smoke](./EXP-03-local-smoke.json) | Actual Colab adaptive evidence của owner và official reproduction/resolution chưa đủ; bỏ ghi chú cũ phải chờ #114 merge vì PR đã merge |
| EXP-04 | In Progress → Needs Update | [#121](https://github.com/HoaiTam/japan-earthquake-etl/pull/121) merged; [pilot evaluation](./EXP-04_PILOT_EVALUATION.md) | Official reproduction comparison/stability/Omori/sensitivity và freeze phương pháp còn thiếu |
| MLI-04 | In Progress → Needs Update | [#124](https://github.com/HoaiTam/japan-earthquake-etl/pull/124) merged; reviewed head61c07da,35/35 tests MLI-04,21/21 impact,17/17 suite tests,21/21 CI | Actual official import/Trino SQL/report parity chưa có; task vốn để ô này chưa tick |
| SEC-01 | Needs Update → giữ nguyên | Local23/10 đã đạt; [#119](https://github.com/HoaiTam/japan-earthquake-etl/pull/119) vẫn Draft | Actual cloud artifacts/session, roster/ACL/recipient gates chưa đủ; không chờ DAT-03 Done để audit scope pilot |

PR #111/118/120 (EXP-01/COL-01/COL-02), #116 (COL-04) và các component trước
đã Done, giữ nguyên. Merged PR của component không đổi các task phụ thuộc ngược
lại Done. Không thay Hard dependency, scope/effort hoặc tạo task IDs mới.

Các lỗi đã review ở #117/#124 đã được sửa trước merge; không yêu cầu thành viên
làm lại module. Các counts/tests là kết quả review exact heads trong ngày,
không tuyên bố runtime hoặc official research mới.

## Drive checkpoint đã kiểm trong ngày

Scoped folder `data/dat03`:185files, không có ModTime/CreatedTime từ07/11,
`accepted/` trống. Trí lineage ZIP118851bytes đọc lại trong RAM: external SHA
`6324a540175502c6f549e6b8b444ccd4b4028486ea3095a75d829f9bc490874b`
khớp, CRC đạt,114entries; không có Parquet/identity.json/publication.json.
Đây là kiểm archive integrity, không chứng nhận complete transitive closure.
Trang chưa thấy gói lineage tầng tiếp theo trong folder đã kiểm; không kết luận
mọi tài khoản/thư mục khác đều không có. Không tải data lớn/đổi ACL/chạy ETL.

## Sau cập nhật

- 63 Done;6 Needs Update;12 Backlog;0 In Progress;81task,416h estimate lịch sử.
- 73Core:63Done/10còn;8Stretch:0Done/8còn.
- 18task phân đều6task/người theo [kế hoạch hiện hành](../task/WEEK_9_REPLAN_20261111.md).
- Assignee cập nhật trong từng task, reviewer vẫn chưa tự điền approval.
- Docs-only: không commit/push/merge trong phiên này; chưa có trạng thái mới trên
  remote main cho đến khi người dùng push/merge bản docs.

## Kiểm tra bản docs này

- `make check-task-status`: 81 task files/metadata/tracking/index đồng bộ.
- `make check-docs`: 293 Markdown files, 2698 links, 0 problems.
- `make test-docs`: 10 link-checker tests và 7 onboarding tests đạt.
- `make check-config ENV_FILE=/dev/null`: repository/example secret hygiene đạt;
  không có `.env` local, không tuyên bố runtime/config deployment đã audit lại.
- `git diff --check`: đạt. Không có thay đổi code/runtime/data.

