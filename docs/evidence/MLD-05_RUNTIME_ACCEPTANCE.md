# MLD-05 — Nghiệm thu validator, snapshot và export bundle

Ngày nghiệm thu: 2026-10-21. Branch `feat/mld-05-validated-feature-export`
từ main `ea4eef4` (PR #93). Assignee rosy179; reviewer unassigned, chưa có
approval độc lập. Task hoàn tất implementation/component acceptance, không
đồng nghĩa đã có dataset nghiên cứu thật toàn kỳ hoặc đã chạy model/Colab.

## Phạm vi và bằng chứng runtime

Run: `mld05-qa-b1b89cd2ba8b439390933445cbd66062`.

- Gold namespace: `iceberg.mld05_qa_ce10b138154447978d8007fa25b64ffe`.
- ML namespace: `iceberg.mld05_ml_qa_ce10b138154447978d8007fa25b64ffe`.
- Root volume: `/opt/pipeline/staging/mld-05/mld05-qa-b1b89cd2ba8b439390933445cbd66062/`.
- Kết thúc: `2026-10-21T12:35:16.964Z`; acceptance PASS, exact-owner lease RELEASED.
- [Metadata tóm tắt và checksum](./MLD-05-runtime-summary.json). Full request,
  runtime-report, evidence-capsule, original execution/retry logs và 10 bộ
  operation receipts giữ trong root; không commit archive/Parquet/private logs.

QA tạo 20 archive JMA **tổng hợp** 1999–2018 (1999 làm halo), ghi bằng production
Bronze writer rồi chạy real ETL/MinIO/Iceberg/Trino. Mười batch lần lượt bao phủ
2000-01..2001-12, 2002-01..2003-12, 2004-01..2005-12, 2006-01..2007-12,
2008-01..2009-12, 2010-01..2011-12, 2012-01..2013-12, 2014-01..2015-12,
2016-01..2017-12 và 2018-01..2018-09: **225 tháng** REPRODUCTION. Mỗi write
batch vẫn tối đa 24 tháng; registry tích lũy không dùng nhầm giới hạn đó.
Đây là completeness của fixture temporal scope, không là completeness địa chấn.
Report giữ `synthetic_component_qa=true`, `research_ready=false`,
`scientific_approval=false`; Mc=4,0 chỉ là kết quả trên fixture tổng hợp.

## Kết quả đối chiếu

| Gate | Kết quả thực tế |
|---|---|
| Coverage thiếu sau batch đầu | Bị chặn trước export; dataset riêng vẫn BUILDING, không có bundle |
| Gold publication | 10/10 PASSED/Published; mỗi operation có 6 phase receipts được validator kiểm |
| Gold cuối | 227 canonical events / 227 source-bridge rows trong namespace QA |
| Feature snapshot | 2 mainshocks, 10 feature rows, 29 cột; MAINSHOCK=2, POST=8 |
| Pinned Trino readback | Registry=1 row EXPORTED, mainshock=2, candidate=10 |
| Retry | Mỗi bảng mainshock/candidate chỉ 1 snapshot mang dataset identity; ba pins và ba SHA không đổi |
| Portable bundle | 1 Parquet part + 2 checksum indexes + manifest; không copy warehouse |
| Trusted handoff | `TrustedDatasetResolver` đọc đúng 10 candidate identities từ registry đã pin |
| Gold trước/sau exporter | Cả 6 snapshot IDs giữ nguyên |
| Handoff local | `VERIFIED_EXPORTED_BUNDLE`, 10 rows, credentials_required=false |

Kiểm local chạy thật bằng `make verify-ml-export`, Python 3.13/PyArrow 21 và
EXP-01 OFFICIAL loader, trên bản copy trong `staging/mld-05-offline/b1b89cd2/`.
Hai expected SHA lấy từ receipt ngoài bundle, không tự tin checksum đi kèm.
Không gọi Docker/API/MinIO trong lần verify. Helper copy tự hết hạn `--rm`;
không xóa volume, bundle, namespace hoặc evidence.

## Recovery và provenance

Lượt đầu exit 1 sau 718,39 giây; retry đầu exit 1 sau 169,74 giây; retry cuối
exit 0 sau 33,43 giây. Không dùng lượt lỗi làm PASS hoặc xóa/ghi đè log cũ.
History checkpoint và exact owner được giữ để tiếp tục exporter, không ingest
lại 225 tháng. Lượt kết thúc có `resumed_from_checkpoint=true`, cả first/rerun
`reused=true`: artifact đã EXPORTED ở execution trước, sau đó mới hoàn tất
readback/handoff. Không sửa report để giả first là export mới.

Trong quá trình sửa có hai điểm tương thích: lifecycle SQL phải nhận tên cột
`*_sha256`; pinned readback dùng Iceberg row reader vì vectorized reader
Spark 3.5 không đọc được DELTA_LENGTH_BYTE_ARRAY do Trino sinh. Không sửa file
metadata hay table property toàn cục. Capsule lưu riêng history/export/final
readback source hashes; không gộp các lần chạy thành một source attestation.

## Test đã chạy

- `make test-ml-export`: **11 Python control + 4 CLI + 17 Java tests PASS**,
  Java 21.0.11/Spark 3.5.9; 0 Java failures/errors/skips. Java tests dùng
  temporary local Iceberg/Parquet, không kết nối catalog production.
- Java17 build/scoped regression trước runtime: 60 tests PASS trên 8 classes
  feature/export/full-month/adapter; runtime chạy Java17. Lượt local Java21
  trên là kiểm lại riêng exporter sau chỉnh đường dẫn temp trong test.
- `TMPDIR=/private/tmp python3 -m unittest discover -s airflow/tests`:
  **413 PASS**. Lần chạy không đặt TMPDIR trước đó lỗi vì macOS `/var` là
  symlink; rerun canonical temp đạt, không nới guard runtime.
- EXP-01 harness: **45 PASS**; bao gồm file nằm dưới `dataset_id=...` không
  bị PyArrow tự thêm Hive partition column trùng cột vật lý.
- CI policy: **38 PASS**; Docker lifecycle: **54 PASS**.
- ORC-06/07 regression chạy lại Java21: **25 PASS** (17 resolver + 8 adapter),
  0 failures/errors/skips; giới hạn 24 affected months vẫn giữ.
- `TMPDIR=/private/tmp make ci-docs-base`: PASS, gồm contracts/task status,
  link check (213 Markdown files, 1.890 links, 0 problems), static Compose,
  config/secret hygiene regressions, CI policy và shell syntax/whitespace.
- Source/history audit: **7 findings / 3 pending**, exit 1, không phải PASS.
  Hai worktree findings đã có trong harness/test fixture, năm findings trong
  Git history; không tự sửa allowlist hoặc nhận SEC-01 Done.
- Full `make test-pilot` chưa PASS: môi trường hiện thiếu scikit-learn/model
  dependencies. Loader/handoff MLD-05 đã kiểm riêng; không nhận đây là model gate.

Regression exporter có tamper/extra part/duplicate/grain/missing self/Mc/finite/
identity/lease/symlink cases; partial snapshot retry giữ dataset khác; 10.001
rows tạo nhiều parts ổn định. `@TempDir` của test được canonical hóa để không
nhầm alias hệ thống macOS là symlink do người dùng đưa vào.

## Giới hạn / handoff

- [MLI-02 — Tạo Airflow DAG build ML dataset](../task/tasks/MLI-02.md): nối
  real exporter vào step runner và nghiệm thu live build DAG; CLI không thay DAG.
- [EXP-02 — Chạy Window, DBSCAN và HDBSCAN global](../task/tasks/EXP-02.md):
  official fit/local gate/Colab parity còn riêng; verify không tạo scientific approval.
- [MLQ-01 — Chạy E2E Gold snapshot đến ML report](../task/tasks/MLQ-01.md):
  còn whole-flow official acceptance, không nhận Done từ QA này.
- [QA-05 — Profile dữ liệu lịch sử và tài nguyên local](../task/tasks/QA-05.md):
  chưa có benchmark lớn; exporter v1 tối đa 100.000 rows/128 mainshocks, part 64 MiB.
- Nghiệm thu backfill **catalog thật toàn kỳ** chưa có task riêng; ORC-03/06
  cung cấp execution/state, QA-03 chỉ nghiệm thu bounded scenarios. Không đổi
  DAT-02 PARTIAL/BUILDING thành EXPORTED để lách gate.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): giữ
  findings/coverage theo evidence security riêng; không tự gắn approval.
