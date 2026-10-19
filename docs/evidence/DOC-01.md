# DOC-01 — Đồng bộ tài liệu và reproduction

## 1. Mốc và phạm vi

- Ngày: **19/10/2026**, Asia/Ho_Chi_Minh.
- Base: `origin/main` **09d81a1**, PR #88 đã merge; không có PR mở khi bắt đầu.
- Branch: `docs/doc-01-cap-nhat-tai-lieu-theo`.
- Assignee HoaiTam, reviewer unassigned; không tự ghi approval.
- Machine-readable reproduction metadata: [DOC-01-reproduction.json](./DOC-01-reproduction.json).

DOC-01 đồng bộ docs theo code/contracts hiện có, không đổi schema, thuật toán,
dataset/result lifecycle, DAG, deployment mode hoặc task status của người khác.
Không build/start/reset Docker, ingest nguồn, export official hoặc import ML.

## 2. Nội dung đã đối chiếu

- Kiến trúc: source/Bronze → Silver revisions/linking → full-month state → sáu
  Gold snapshots → Trino verify/publish → dataset/experiment/import/report.
- Entry points: ORC-02 daily 8 task/default source-only, ORC-07 manual 5 task và
  local[1] adapter; không suy worker ALIVE hoặc DAG.test thành scheduled acceptance.
- ML: hai DAG build/import độc lập ETL, dataset/experiment lifecycle,
  development pilot khác official EXPORTED/result marker.
- Jupyter-first, real/synthetic input, expected counts/noise/failure reasons,
  restart/run-all, bundle ZIP handoff và Colab gate.
- Core static report / Stretch Power BI; không prediction/causal/scientific approval.
- Tách Docker preparation/start/smoke, bỏ hướng dẫn xóa lease thủ công trong
  runbook MLI-02; giữ source owner/journal và scope recovery.
- Sửa 14 đường dẫn Markdown sai trong evidence/CI guide; không sửa kết quả
  lịch sử thành bằng chứng mới. Thêm checker offline cho links/heading anchors,
  từ chối target local-only và lệnh onboarding không tồn tại.

## 3. Reproduction thực tế — không credential notebook

### Môi trường mới

Tạo venv mới `staging/venvs/doc01`, Python 3.13.3; cài
`notebooks/requirements-lock.txt`, pip check không broken requirements.
Direct package versions trong JSON evidence. HDBSCAN 0.8.40 được build wheel
trên máy này; không claim package lock có mọi transitive hash/OS wheel.

Lần chạy harness unit trong sandbox bị từ chối bind cổng Jupyter loopback;
chạy lại với quyền mở local kernel đạt. Không tắt nbclient để dùng fallback
hoặc sửa report. Không đọc/source `.env` để chạy notebook.

### Dataset và hai lần experiment trên pilot JMA thật

1. Xác nhận ZIP 45,907 bytes, SHA256
   `b53cd7bfdc3768c3fd0ee0bcc5e037ef1c9c9b007d0df900ec223c8dc139bc3e`;
   inventory đúng năm file, không path escaping.
2. Chạy EXP-02 từ bundle gốc DAT-02, output mới
   `staging/doc01-reproduction.7Po4CX/`: fresh nbclient kernel, 7 cells,
   RESTART_RUN_ALL_PASSED, local_smoke_status=PASSED, 72.5593s.
3. Giải nén ZIP vào `staging/doc01-handoff.OV5zit/bundle` và chạy lại EXP-02:
   fresh kernel, 7 cells, PASSED, 2.5422s. Hai lần có cùng input fingerprint,
   algorithm counts và case_results; run ID/time/runtime khác là bình thường.
4. `inspect-pilot-notebook.py` trên bundle giải nén: 3 cells fresh kernel,
   PILOT_INSPECTION_PASSED, 279 features/2 mainshocks, self/vector zero đúng,
   PARTIAL/research_ready=false; source notebook không bị sửa output.

Các entry point đã chạy (venv Python, output scopes riêng theo từng lần):

```bash
python scripts/inspect-exp02-notebook.py --pilot-bundle "$PILOT_BUNDLE" --output-json "$DOC01_HANDOFF_ROOT/exp02-inspection.json"
python scripts/inspect-pilot-notebook.py "$PILOT_BUNDLE" --report "$DOC01_HANDOFF_ROOT/dataset-inspection.json"
```

Biến/đầu vào/output setup chạy được ghi đầy đủ tại
[onboarding](../runbooks/DOC-01_TEAM_ONBOARDING.md), không copy riêng hai dòng
trên khi chưa setup. Reports runtime nằm trong staging ignored, chỉ metadata
nhỏ không credential được đưa vào Git.

| Algorithm | Candidate/window | Sequence members | POST candidates | Noise |
|---|---|---:|---:|---:|
| WINDOW | 279 / 2 | 257 | 255 | 0 |
| DBSCAN | 279 / 2 | 251 | 240 | 9 |
| HDBSCAN_GLOBAL | 279 / 2 | 0 | 0 | 108 |

DBSCAN có một mainshock-noise window; HDBSCAN có hai mainshock-noise windows.
Technical report PASSED vì cases/counts/lineage đúng, không phải mọi window có
productive sequence hoặc HDBSCAN đã tốt hơn. Dataset chỉ khoảng 14 ngày input
JMA, không full-year/40-year completeness. Không có official _SUCCESS marker.

### Đường clone mới không có data

Chạy EXP-02 với PILOT_BUNDLE rỗng, output mktemp riêng: fixture **synthetic**
2 candidates/1 mainshock, 7 cells fresh nbclient kernel, PASSED, 2.5669s.
Như vậy thành viên mới có thể chạy từ source trước khi nhận ZIP thật, nhưng
không được trình bày synthetic report như real pilot evidence.

## 4. Regression / kiểm tra tài liệu

- `make check-docs`: **205 Markdown files, 1.783 links, 0 problems**;
  file/heading nội bộ offline, không fetch HTTP.
- `make test-docs`: 10 checker/onboarding/safety tests passed.
- Venv `unittest discover`: EXP-01 **44**, EXP-02 **19**, EXP-03 **23** tests passed.
- `make test-pilot PILOT_PYTHON=staging/venvs/doc01/bin/python`: toàn target
  đạt, gồm 16 pilot loader/staging + 44 harness + 19 baseline + 23 adaptive tests.
  Có ResourceWarning về socket cleanup từ notebook harness tests; không có
  error/failure và không sửa upstream implementation trong task tài liệu.
- `TMPDIR=/private/tmp make test-contracts test-ci-workflow test-security-audit test-docker-runtime`:
  contracts/task index/fixture/inventory/build input checks đạt; CI **38**,
  security **31**, Docker lifecycle **50** tests passed.
- `TMPDIR=/private/tmp make ci-docs-base`: **PASS**, gồm docs guards mới,
  contracts, CLI/full-month/security/lifecycle, config/Compose/MinIO/Trino static,
  CI regression và whitespace. Không build/start Docker hoặc lấy dữ liệu nguồn.
- `git diff --check`: đạt. Không sửa notebook source, không commit venv,
  Parquet/raw ZIP, .env, logs hoặc model runtime results.

## 5. Đối chiếu acceptance DOC-01

| Tiêu chí | Evidence |
|---|---|
| Thành viên mới hiểu làm gì/gồm gì/dùng để làm gì và chạy theo runbook | Architecture + block table + lifecycle/runtime separation + synthetic/ZIP walkthrough đã chạy |
| Không còn mô tả mâu thuẫn trong tài liệu hiện hành được đồng bộ | Bỏ scaffold-only/BI-mandatory/ML-out-of-scope narrative; known issues có ngày/task; tài liệu evidence cũ giữ mốc lịch sử |
| Tái lập một dataset/experiment nhỏ không credential notebook | Fresh venv, dataset inspection, hai real-pilot runs ở hai đường dẫn và synthetic clone path; counts/fingerprint khớp |

## 6. Giới hạn / task kế tiếp

DOC-01 Done ở phạm vi tài liệu/reproduction development, không phải release MVP
hoặc nghiệm thu toàn trình. [Known issues](../KNOWN_ISSUES.md) ghi chi tiết:
QA-01 còn scheduled deployment, MLD-05 official export, MLI-02/03 live build/import,
EXP-02 Colab/official, EXP-04/05 evaluation/extension, MLI-04/MLQ-01 report/E2E,
SEC-01 retained-log/actual handoff audit, OPS-01 restore (Stretch).
Chưa có task current-month/as-of-cutoff/JMA sau 2023. Không đổi các task đó Done.
Colab thực tế chưa chạy; local migration_allowed chỉ mở gate portability test.
