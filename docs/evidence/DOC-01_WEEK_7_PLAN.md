# DOC-01 — Phân công và hướng dẫn setup Colab tuần 7

Ngày: 29/10/2026, Asia/Ho_Chi_Minh. Branch `docs/doc-01-week-7-colab-plan`
từ main `5cc9e95501a9963ddda65866a14f179ce8a63258` (PR#108 đã merge).
Scope: lập kế hoạch/docs và regression cho tài liệu, **không thực thi Colab**.

## Thay đổi

- [Kế hoạch02–08/11](../task/WEEK_7_COLAB_PLAN.md): HoaiTam khối1,
  rosy179 khối2, ThanhTris khối3; 12task,4new/8carryover, budget54h.
- [Setup đầy đủ](../runbooks/WEEK_7_COLAB_SETUP.md): trusted pilot/code pins,
  local gate/macOS/Linux/Windows, Drive/CPU/dependencies, verifier/loader,
  baseline/adaptive APIs, output/readback, expected results và failure handling.
- COL-01..04 file riêng, owners/status/index thống nhất. EXP-01 mở lại cloud
  Needs Update, local acceptance giữ nguyên; EXP-03 chuyển ThanhTris.
- Tổng81task/416h (Core73/Stretch8), giữ baseline364h và các addendum lịch sử.
  Scientific/dependency/lifecycle gates không bị giảm để khớp lịch.
- Planning checks giữ tuần5/effort lịch sử nhưng kiểm current carryover owners;
  thêm regression chặn chuyển owner sai, compile setup cells và so API keywords.

## Kiểm tra thực tế

- `make check-docs`: 0 broken links; chỉ offline, không kiểm actual Drive ACL.
- `make check-task-status`: 81files, metadata/tracking/index khớp.
- `make check-week-3-plan check-week-5-plan check-mvp-baseline`: PASS,
  giữ addenda/accounting/scientific gates.
- `python3 -B -m unittest discover -s tests -p 'test_week7_plan.py'`:
  7tests PASS, compile các Python cell trong hướng dẫn và đối chiếu API hiện có.
- `PYTHONDONTWRITEBYTECODE=1 make test-docs test-week-5-plan`:
  documentation10tests + week7plan7tests + week5assignment21tests PASS,
  tổng38tests; test-docs đã nối thêm week7 để CI cũng chạy chúng.
- `ENV_FILE=/private/tmp/ie212-week7-no-local-env make check-config`: PASS,
  repo/example/secret-pattern hygiene; không đọc local credentials hoặc
  chứng nhận Google account/cloud session. Không phải full security audit.
- `git diff --check`: PASS; các test chỉ stdlib/temp fixtures, không runtime ETL.

## Giới hạn / không được suy ra

- **EXP-01/EXP-02/EXP-03:** chưa chạy Colab, chưa nghiệm thu package resolution/
  parity/adaptive fresh session trên cloud từ hướng dẫn này. Compile/API check
  không chứng minh pip hoặc runtime Colab tương thích.
- **DAT-03:** chưa nhận receipt hai máy còn thiếu/full catalog/official export.
  279candidate rows/2mainshock chỉ development pilot, không research release.
- **SEC-01:** chưa audit actual cloud session/quyền nhận; không đổi IAM/ACL,
  không mount Drive, upload hoặc cấp credentials.
- **EXP-04/MLI-04:** pilot metrics/templates không thay scientific evaluation
  hoặc Trino verification trên official imported results.
- Chưa commit/push/mở PR; không thay Docker/runtime/data hoặc chạy ETL/model.
