# Trang — Week 4 PR handoff

Ngày 2026-10-09. Tất cả branch từ origin/main 9a8854c. Mỗi PR chỉ chứa task tương ứng; trạng thái dưới đây nằm trên từng branch PR, chưa merge vào main. Owner docs: Trang; reviewer unassigned.

| Task | PR | Trạng thái | Evidence |
|---|---|---|---|
| GLD-01 | [#50](https://github.com/HoaiTam/japan-earthquake-etl/pull/50) | Done, ready review | 188 Spark tests, 6 Gold tests |
| GLD-03 | [#51](https://github.com/HoaiTam/japan-earthquake-etl/pull/51) | In Progress, draft | 5 interface/mock tests |
| GLD-04 | [#52](https://github.com/HoaiTam/japan-earthquake-etl/pull/52) | In Progress, draft | 5 SQL/mock harness tests |
| MLD-01 | [#53](https://github.com/HoaiTam/japan-earthquake-etl/pull/53) | In Progress, draft | 5 identity/metadata tests |
| MLI-01 | [#55](https://github.com/HoaiTam/japan-earthquake-etl/pull/55) | Done, ready review | 189 Spark tests; final 7 contract tests; committed fixture checksum check |
| SEC-01 | [#56](https://github.com/HoaiTam/japan-earthquake-etl/pull/56) | In Progress, draft | 8 tests, scoped static audit and CFG-01 checks |

## Files / phạm vi

- GLD-01: gold/GoldRunContext, GoldEventTransformer, GoldTransformationResult; test, test-only POM compatibility, GOLD_TRANSFORMATION.md, task/index, Spark README.
- GLD-03: GoldCommitRequest/Adapter SPI; mock test; GOLD_COMMIT_PREPARATION.md, task/index.
- GLD-04: GoldVerificationPlan; harness test; GOLD_VERIFICATION_PREPARATION.md, task/index.
- MLD-01: DatasetIdentityPlan; metadata test; DATASET_IDENTITY_PREPARATION.md, task/index.
- MLI-01: ResultBundleContract/Validator, machine-readable JSON contract, Java test and seven tiny Parquet bundles, Git attributes, ML_RESULT_BUNDLE_CONTRACT.md, task/index.
- SEC-01: scripts/audit-security.py, tests/test_security_audit.py, docs/security checklist/triage, docs/evidence report, task/index và handoff này.

## Giới hạn / Rủi ro / Bước tiếp theo

- [SLV-07 — Liên kết observation và chọn canonical event](../task/tasks/SLV-07.md), [SLV-09 — Tích hợp và kiểm thử Silver đa nguồn](../task/tasks/SLV-09.md): cần upstream hiện hành trên main và exact SilverReady input/readback; không tự merge task của thành viên khác.
- [GLD-03 — Ghi Gold Iceberg và commit snapshot](../task/tasks/GLD-03.md): còn production Iceberg writer/DDL, scoped write, real retry/partial-commit/readback. PR #51 chỉ prep.
- [GLD-04 — Tạo Trino views và verification SQL](../task/tasks/GLD-04.md): còn actual SQL execution/schema/scope/full blockers và Published gate. PR #52 chỉ prep.
- [MLD-01 — Pin Gold snapshot và tạo dataset manifest](../task/tasks/MLD-01.md): còn publication resolver và Iceberg materialization/integration trên snapshot thật. PR #53 chỉ prep.
- [SEC-01 — Review secret và bề mặt truy cập](../task/tasks/SEC-01.md): Docker daemon chưa chạy, .env chưa có; còn live permission allow-deny/log/config và bootstrap credential process arguments.
- [EXP-01 — Tạo Colab harness và khóa môi trường thí nghiệm](../task/tasks/EXP-01.md), [MLD-05 — Validate feature snapshot và export Parquet bundle](../task/tasks/MLD-05.md), [MLI-03 — Validate/import kết quả và commit bảng ML Iceberg](../task/tasks/MLI-03.md): model/export/production reader-registry/import và security artifacts thật chưa tồn tại trong phạm vi này.
- Reviewer độc lập chưa được gán; không có approval hoặc Gold/ML Published giả lập. Draft chưa đủ acceptance Done.

Review #50 và #55 trước. Draft có thể review interface/test plan; để hoàn tất cần dependencies/live evidence nêu trên. Sau mỗi merge, cập nhật/rebase các PR còn lại và đồng bộ task index; không merge tự động trong phiên này.

## Git commands cho SEC-01

Các commit/push/PR đã thực hiện. Nếu review tạo thay đổi mới, từ C:\Code\IE212\japan-earthquake-etl:

    git switch chore/sec-01-security-audit
    git diff
    git add scripts/audit-security.py tests/test_security_audit.py docs/security/SEC-01-CHECKLIST.md docs/security/SEC-01-synthetic-uris.json docs/evidence/SEC-01-static.json docs/task/tasks/SEC-01.md docs/task/tasks/README.md docs/evidence/TRANG_WEEK4_HANDOFF.md
    git diff --cached --check
    git commit -m "chore(security): address scoped audit review"
    git push -u origin chore/sec-01-security-audit

## Mô tả PR tiếng Việt để copy

### GLD-01 — PR #50

```markdown
## Mục tiêu / Task
GLD-01 — Xây canonical event, dimensions và bands. Assignee trong docs: Trang; reviewer: unassigned.

## Thay đổi chính
- Spark Java/DataFrame API cho Gold current, source bridge, natural/ROI view và dimensions/bands.
- Giữ ID/PRIMARY của Silver membership, không matching hoặc tạo canonical ID lần hai.
- Field selection có provenance; magnitude/depth nullable, tsunami unknown, UNKNOWN/Offshore và ambiguous events được giữ.
- Kiểm tra unique keys, membership cardinality, required fields, finite values và count reconciliation.
- Jackson Scala dependency chỉ scope test và Java17 module flags cho Spark local tests; SDK runtime isolation giữ nguyên.

## Docs
- docs/specs/GOLD_TRANSFORMATION.md: API, output, tests, handoff và giới hạn.
- docs/task/tasks/GLD-01.md, task index và spark/README.md.

## Test / Check thực tế
- Maven -pl spark -am test: 188 tests, 0 failures/errors/skipped; GoldEventTransformerTest 6/6.
- check-data-model-contract.sh, check-repository-layout.sh và check-task-status.sh: đạt.
- git diff --check: đạt.
- Fixtures nhỏ/synthetic/offline, thực thi Spark local trên Java17; không gọi mạng/source/storage.

## Giới hạn / Review / Recovery
- SLV-07 — Liên kết observation và chọn canonical event: producer chịu policy chọn PRIMARY; GLD-01 kiểm tra cấu trúc/cardinality.
- SLV-09 — Tích hợp và kiểm thử Silver đa nguồn: còn chờ SilverReady manifests/readback/reconciliation thật.
- GLD-03 — Ghi Gold Iceberg và commit snapshot: chưa ghi Iceberg hoặc fault-inject writer thật.
- GLD-04 — Tạo Trino views và verification SQL: chưa verify/publish Gold thật.
- GLD-02 — Tạo aggregate phục vụ dashboard: Stretch, ngoài phạm vi.
- Máy chưa có .env và Docker daemon chưa chạy. Unit acceptance GLD-01 đạt; không nhận fixture là Gold Published.
- Không có lake write trong PR; rollback bằng revert commit sau review, không xóa data/volume.
```

### GLD-03 — PR #51

```markdown
## Mục tiêu / Task
GLD-03 — Ghi Gold Iceberg và commit snapshot. Assignee Trang; reviewer unassigned.
PR draft: chỉ chuẩn bị interface/mock/test plan được task cho phép trước dependency.

## Thay đổi chính
- Immutable commit request: exact Silver manifests, operation/run/config/window/date/backfill, baseline snapshots, affected partitions và expected counts.
- SPI inspect/commit và receipt guard: quality failed, incomplete readback, stale operation/table scope đều bị chặn.
- Deterministic request hash và mock rerun reuse; không có production Iceberg writer hoặc publication side effects.

## Docs
GOLD_COMMIT_PREPARATION.md, task GLD-03 và index (In Progress).

## Test / Check thực tế
GoldCommitInterfaceTest: 5/5 pass, BUILD SUCCESS; changed-file secret check và git diff --check đạt.
Mock/offline, không ghi lake hoặc chạy Docker.

## Giới hạn / Bước tiếp theo
- GLD-01 — Xây canonical event, dimensions và bands: nhận/merge output PR #50.
- SLV-09 — Tích hợp và kiểm thử Silver đa nguồn: cần exact SilverReady bundle/readback.
- GLD-03 còn thiếu Spark Iceberg dependency/DDL/writer, affected-scope write, real rerun/partial commit/fault injection và snapshot readback.
- GLD-04 — Tạo Trino views và verification SQL: verify bundle đúng snapshot trước publication.
- Máy chưa có .env/Docker daemon; interface/mock không đủ acceptance Done. Không xóa data/volume để recovery.
```

### GLD-04 — PR #52

```markdown
## Mục tiêu / Task
GLD-04 — Tạo Trino views và verification SQL. PR chuẩn bị interface và fixture, giữ In Progress.
## Thay đổi chính / Docs
Pinned snapshot SQL và sáu blocker checks; fixture report luôn MOCK_TRINO và không được publish. Xem docs/specs/GOLD_VERIFICATION_PREPARATION.md; cập nhật task/index, assignee Trang, reviewer unassigned.
## Test / Check thực tế
5 tests Maven đạt; changed-file secret scan và git diff --check đạt. Chưa chạy SQL qua Trino thật.
## Giới hạn / Bước tiếp theo
- GLD-03 — Ghi Gold Iceberg và commit snapshot: còn writer/readback và snapshot thật (PR #51).
- SLV-09 — Tích hợp và kiểm thử Silver đa nguồn: còn input ready-for-Gold.
- GLD-04 — Tạo Trino views và verification SQL: còn live adapter, schema/type, scope toàn bộ bảng và publication gate.
```

### MLD-01 — PR #53

```markdown
## Mục tiêu / Task
MLD-01 — Pin Gold snapshot và tạo dataset manifest. Chuẩn bị identity/metadata fixtures; In Progress.
## Thay đổi chính / Docs
Canonical JSON/SHA-256, identity gồm snapshot/filter/version/cutoff, half-open split và rerun conflict; BUILDING manifest mock không materialize/publish. Docs: DATASET_IDENTITY_PREPARATION.md, task/index. Owner Trang, reviewer unassigned.
## Test / Check thực tế
5 tests Maven đạt, secret scan file thay đổi và git diff --check đạt.
## Giới hạn / Bước tiếp theo
- GLD-04 — Tạo Trino views và verification SQL: cần Published snapshot thật.
- MLD-01 — Pin Gold snapshot và tạo dataset manifest: còn resolver publication, schema/materialization Iceberg và integration evidence; view phải map physical snapshot rõ ràng.
- MLD-02/03/04/05: audit/Mc, candidate, feature và export gates chưa triển khai trong PR này; không fake VALIDATED/EXPORTED.
```

### MLI-01 — PR #55

```markdown
## Mục tiêu / Task
MLI-01 — Khóa result bundle và experiment lifecycle. Contract/interface/fixtures hoàn tất; task Done, assignee Trang, reviewer unassigned.
## Thay đổi chính
Machine-readable layout/schema/grain/null/type/enum; checksum trên bytes, count/lineage, probability/noise/role và run reuse. Validator receipt chỉ RESULT_READY; CANDIDATE cần commit verify, APPROVED cần review khoa học. Bảy bundle Parquet synthetic nhỏ.
## Docs
ML_RESULT_BUNDLE_CONTRACT.md; fixture README; task và index đồng bộ Done.
## Test / Check thực tế
Full Spark suite: 189 tests, 0 fail/error/skip. Final contract retest: 7 tests đạt. Task-status 73 tasks, changed-file secret scan và git diff --check đạt.
## Giới hạn / Bước tiếp theo
- EXP-01 — Tạo Colab harness và khóa môi trường thí nghiệm: chưa chạy notebook/model thật.
- MLI-03 — Validate/import kết quả và commit bảng ML Iceberg: còn production reader, durable run registry và Iceberg/Trino publication. SPI cần đọc đúng rows từ bytes đã checksum; không tin assertion do bundle ngoài cung cấp.
- SEC-01 — Review secret và bề mặt truy cập: cần audit artifacts/permissions thật khi downstream tồn tại.
Không ghi lake/Gold hoặc tạo approval trong PR này. Reviewer độc lập còn unassigned.
- Checksum từ committed Git blobs: 6 bundles đúng, fixture mismatch có chủ ý; generator JSON LF và Git attributes bảo toàn bytes. Final tests đọc committed fixture 7/7 đạt.
```

### SEC-01 — PR #56

```markdown
## Mục tiêu / Task
SEC-01 — Review secret và bề mặt truy cập. Audit có phạm vi; giữ In Progress/draft. Owner Trang, reviewer unassigned.
## Thay đổi chính / Docs
Read-only redacted checker cho source, reachable Git blobs, local logs, Compose port/role boundaries; content-hash triage cho URI synthetic tests. SEC-01-CHECKLIST.md, SEC-01-static.json, task/index. Không in secret, sửa quyền hoặc xóa dữ liệu.
## Test / Check thực tế
8 unit tests đạt. Audit: 374 files, 1102 Git blobs, 9 logs; 0 untriaged pattern findings, 8 synthetic URI findings đã đối chiếu code. Bốn port khai báo loopback; audit exit 2 là incomplete coverage. CFG-01/check-config, task-status 73 tasks, changed-file secret scan/diff check đạt.
## Giới hạn / Bước tiếp theo
- SEC-01 — Review secret và bề mặt truy cập: Docker daemon không chạy, local .env chưa có; cần live env/log/default/permission allow-deny evidence và review mc bootstrap credential arguments. Không xác nhận runtime an toàn chỉ từ static scan.
- EXP-01 — Tạo Colab harness và khóa môi trường thí nghiệm: chưa có artifact/quyền notebook thật để audit.
- MLD-05 — Validate feature snapshot và export Parquet bundle: chưa có export thật để audit.
- MLI-03 — Validate/import kết quả và commit bảng ML Iceberg: cần audit bundle/registry/import boundaries khi triển khai.
```
