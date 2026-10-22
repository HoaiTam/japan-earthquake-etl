# QA-01 — Bổ sung thiết kế current-month, 22/10/2026

## Phạm vi và identity

- Base `origin/main` `2f040c4` (PR #95).
- Branch `test/qa-01-scheduled-runtime-acceptance`.
- Owner HoaiTam, reviewer unassigned; chưa có approval.
- Owner chọn **thiết kế thêm current-month để chạy daily 2026**, không chọn
  nghiệm thu scheduled historical trong môi trường bounded thay thế.
- [Contract mới](../specs/CURRENT_MONTH_AS_OF_CUTOFF.md) có trạng thái
  **DESIGN_ONLY**, không phải runtime acceptance. QA-01 vẫn **In Progress**.
- [Evidence 19/10](./QA-01.md) được giữ nguyên; không đổi NOT_READY thành PASSED.

## Thay đổi thực hiện

1. Policy sealed `current-month-asof-v1`, version/fields/source/limits rõ ràng;
   không có flag hợp lệ để bật publication, ML hoặc research eligibility.
2. Planner metadata thuần: UTC cutoff từ closed scheduled interval, bootstrap
   toàn phần tháng liên quan overlap ba ngày, seed clamp, month/year rollover;
   context/policy/plan SHA deterministic. Không có DAG hay data-plane side effect.
3. Observation cutoff để null trước capture, proof chưa có, output
   `DESIGN_PLAN_ONLY`, `deployable=false`, `published=false`.
4. Design tách USGS current khỏi JMA historical readiness, operational provisional
   khỏi historical/research namespace; thêm requirements capture/ID closure/
   deletion/halo/receipt/consumer gates cho implementation v2 sau này.
5. Đồng bộ task, runbooks, known issues và tuần 5; sửa mô tả upstream JMA
   “mới nhất 2023” thành baseline project pin 2023. Trang nguồn kiểm tra đã có
   link 2024; không tự thay inventory hoặc dataset nghiên cứu.

## Kiểm tra môi trường không thay đổi state

`docker ps` thấy MinIO, Catalog và Trino đang healthy; không có Airflow services
đang chạy tại thời điểm kiểm tra. Không start/rebuild/pull/init/unpause/trigger,
không sửa `.env`, release lease, ghi lake/catalog hoặc xóa dữ liệu. Không có
scheduled daily 2026 mới để nghiệm thu trong lần bổ sung thiết kế này.

## Tests

- `TMPDIR=/private/tmp make test-qa-01`: **44 tests passed** — 15 planner/policy,
  13 collector v1, 11 runtime v1 và 5 host CLI. Unit fixtures là synthetic;
  không coi calendar 2026 trong fixture là một live run.
- Các case mới: bootstrap không bị thay bằng delta ba ngày, ngày đầu tháng,
  hai tháng ở rollover, năm nhuận/đầu năm, seed, identity/input immutability,
  UTC/interval/clock/run metadata, duplicate/nonfinite/oversized JSON, typed
  limits, forbid policy promotion và v1 runtime vẫn disabled mặc định.
- `TMPDIR=/private/tmp make ci-airflow`: **428 tests passed**. Có warning
  sandbox từ Arrow `sysctlbyname` và deprecation từ dateutil, không có failure.
- `TMPDIR=/private/tmp make ci-docs-base`: đạt docs links/tests, contracts,
  76 task status/week plan, host CLI regressions, 31 security tests, 54 Docker
  runtime guard tests, config hygiene/Compose static và 38 CI workflow tests.
  Không gọi Docker daemon/start services hoặc nguồn dữ liệu trong các tests này.
- `git diff --check`: đạt. Không chạy Java build, notebook/Colab hoặc smoke
  current-month; bản này không sửa Java/runtime v1 và chưa có data-plane v2.

## Còn thiếu trước QA-01 Done

**QA-01 — Daily E2E**: implementation/deployment v2 và genuine scheduled daily
2026 với exact captures, receipts, reconciliation, six snapshots, provisional
publication và SQL readback; test blocker đúng tầng. Planner/tests không tạo
bằng chứng đó. Chưa chạy smoke current-month; chưa có lệnh/runtime v2.

**Chưa có task ID/effort riêng** cho work package implementation v2 trên các
boundary source capture, ORC-06 resolver, ORC-07 adapters, GLD-03/04 publication
và MLD-01/05/MLI-02 consumer gates; xem bảng trong contract mới. Các task Done
v1 không bị đổi trạng thái hay được tuyên bố đã hỗ trợ v2. JMA sau baseline
cũng chưa có task riêng. Không tự tăng effort tuần 5 hoặc đổi assignee khác.
