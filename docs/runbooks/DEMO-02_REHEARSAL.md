# DEMO-02 — Rehearsal và release candidate

[DEMO-02](../task/tasks/DEMO-02.md) đang chuẩn bị buổi chạy thử. Dùng gói,
manifest và preflight của [DEMO-01](./DEMO-01_DEMO_PACKAGE.md). Tiêu chí bàn
giao Core theo [MVP Definition of Done](../specs/MVP_SCOPE_KPI_AND_DOD.md).

## Chạy trước trên máy trình bày

Tách rõ hai chế độ chuẩn bị theo kết quả `overall_status` của preflight; **không**
trộn report pilot `DEVELOPMENT_ONLY` với buổi diễn tập official.

### 1. Tập offline (`PREPARED_PENDING_OFFICIAL_GATES` — dùng pilot)

1. Checkout đúng commit chuẩn bị (hiện là nhánh
   [PR #143](https://github.com/HoaiTam/japan-earthquake-etl/pull/143) có manifest v2)
   và ghi `git rev-parse HEAD` vào biên bản.
2. Chạy preflight, unit tests và dựng report pilot gắn nhãn `DEVELOPMENT_ONLY`
   trong `staging/demo-02/` (thuộc Git ignore):

~~~bash
mkdir -p staging/demo-02
python -B scripts/demo-preflight.py --json > staging/demo-02/preflight.json
python -B -m unittest tests.test_demo_preflight
python -B scripts/generate-experiment-report.py --mode development --input-json docs/evidence/EXP-04_PILOT_EVALUATION.json --output-md staging/demo-02/MLI-04_DEVELOPMENT_ONLY.md
~~~

3. Nếu dùng receipt DAT-03 cá nhân, chạy lại preflight với `--local-receipt`
   và `--expected-receipt-sha256` theo runbook DEMO-01. Để kiểm lại bytes của
   kit trên đĩa, chạy verifier theo [hướng dẫn DAT-03](./DAT-03_OFFICIAL_RESEARCH_HANDOFF.md).

### 2. Diễn tập official (`PASS_OFFICIAL_READY` — sau khi chốt `#150` và `#143`)

1. Checkout đúng commit đã chốt đủ `#150` (`MLQ-01`) và `#143` (`DEMO-01`), ghi
   `git rev-parse HEAD` vào biên bản và xác nhận `demo-preflight.py --json` trả
   `overall_status=PASS_OFFICIAL_READY`, `official_release_ready=true`,
   `pending_official_gates=[]`.
2. Mở trực tiếp [báo cáo official MLI-04 đã ghim trên main](../evidence/MLI-04_OFFICIAL_READBACK_20261124.md)
   cùng receipt E2E `MLQ-01` đúng pin trong `demo_package_manifest_v2.json`. Nếu
   cần render lại bản kiểm chứng vào `staging/demo-02/` theo đúng SHA-256 LF của
   [evidence JSON MLI-04](../evidence/MLI-04_OFFICIAL_READBACK_20261124.json)
   (quy trình trong [runbook MLI-04](./MLI-04_OFFICIAL_READBACK.md)):

~~~bash
mkdir -p staging/demo-02
python -B scripts/demo-preflight.py --json > staging/demo-02/preflight.json
python -B scripts/generate-experiment-report.py --mode official --input-json docs/evidence/MLI-04_OFFICIAL_READBACK_20261124.json --evidence-sha256 d2d1ed3082cbcdc2b1c835a7ad315ca7d85f608d0fc32dab0aaee97efe6ef175 --output-md staging/demo-02/MLI-04_OFFICIAL_READBACK.md
~~~

*(Lưu ý trên Windows với `core.autocrlf=true`: `generate-experiment-report.py`
hash raw file bytes, nên nếu file checkout có CRLF, chuẩn hóa bản sao LF vào
`staging/demo-02/MLI-04_OFFICIAL_READBACK_20261124.json` bằng Python binary
`write_bytes(...replace(b"\r\n", b"\n"))` — không dùng `>` của PowerShell để
tránh lệch UTF-16LE — rồi truyền bản sao LF đó vào `--input-json`.)*

3. Khi đã có kết quả diễn tập ba người sau `#150`/`#143`, ghi commit, checksum,
   thời lượng, người trình bày và kết quả chuyển fallback vào biên bản thật;
   giữ nguyên `NOT_RUN` cho tới khi thực sự thực hiện.

## Trình tự đề xuất cho ba vai

| Mốc | Vai đề xuất | Việc trình bày và bàn giao |
|---|---|---|
| 00:00–05:00 | Điều phối ETL | Nêu phạm vi rồi mở bản ghi QA-01. Chỉ run ID, publication ID và các snapshot Gold. Chuyển bằng tên dataset và Gold snapshot. |
| 05:00–11:00 | Dữ liệu và thí nghiệm | Mở evidence DAT-03 với nhãn EXPORTED. Trình bày dataset pin và kết quả EXP-04 theo đúng phạm vi. Ghi rõ `DEVELOPMENT_ONLY` khi dùng pilot. Bàn giao dataset ID và run ID cho phần đọc lại. |
| 11:00–16:00 | Kiểm chứng và báo cáo | Chỉ cách nối receipt, snapshot, Trino query và static report. Khi tập offline, dùng report pilot `DEVELOPMENT_ONLY` và nêu các gate còn chờ; khi diễn tập official (`PASS_OFFICIAL_READY`), mở [report official MLI-04](../evidence/MLI-04_OFFICIAL_READBACK_20261124.md) và receipt E2E `MLQ-01` đúng pin. Diễn giải kết quả là aftershock candidate hồi cứu. |
| 16:00–18:00 | Cả nhóm | Thử chuyển qua một phương án dự phòng, ghi thời lượng, lỗi, người xử lý và kết quả kiểm lại. Điều phối viên chốt blocker. |

Điều phối viên xác nhận người đứng từng vai trước buổi tập. Ba thành viên tự
đi qua phần của mình để xác nhận tiêu chí rehearsal.

## Phương án dự phòng

| Tình huống | Mở sẵn trên máy demo | Nhãn trình bày |
|---|---|---|
| Mất mạng hoặc nguồn không phản hồi | [QA-01 JSON](../evidence/QA-01_CURRENT_MONTH_20261023.json) và [QA-03 JSON](../evidence/QA-03-runtime.json) | `PINNED_REPLAY_EVIDENCE` |
| Drive hoặc Colab không mở được | [EXP-03 local smoke](../evidence/EXP-03-local-smoke.json) và receipt DAT-03 đã kiểm cục bộ | `LOCAL_VERIFIED_FALLBACK` |
| Docker/Trino thiếu tài nguyên | [Gold verification SQL](../evidence/GLD-04-verification.sql), [ML verification SQL](../../sql/ml/ml_verification_queries.sql) và report pilot | `STATIC_EVIDENCE_NO_STAGE_ETL` |
| Power BI/ODBC lỗi | [Báo cáo tĩnh MLI-04](../evidence/MLI-04_STATIC_REPORT.md) và ML verification SQL | `CORE_ONLY_NO_POWER_BI_DEPENDENCY` |

Khi chuyển sang evidence tĩnh, đọc nhãn và phạm vi trước con số đầu tiên.
Giữ màn hình chia sẻ ở các file đã kiểm. Giữ nguyên dữ liệu và môi trường
pipeline trong suốt buổi trình bày.

## Điều kiện ghi nhận release candidate

| Cổng | Kiểm trước khi chốt |
|---|---|
| ETL và recovery | [QA-01](../evidence/QA-01_CURRENT_MONTH_ACCEPTANCE.md), [QA-02](../evidence/QA-02_RUNTIME_ACCEPTANCE.md) và [QA-04](../evidence/QA-04_RUNTIME_ACCEPTANCE.md) truy được về đúng scope, run hoặc publication và kết quả kiểm tra. |
| Import và báo cáo ML | [MLI-03](../task/tasks/MLI-03.md) (`#149`, [rerun adaptive](../evidence/MLI-03_ADAPTIVE_REPRODUCTION_20261124.json)) và [MLI-04](../task/tasks/MLI-04.md) (`#144`, [readback 8/8 shard](../evidence/MLI-04_OFFICIAL_READBACK_20261124.md)) đã `Done` trên `main`. [MLQ-01](../task/tasks/MLQ-01.md) (`#150`) còn cần receipt E2E Gold đến static report (`EXTENSION WINDOW`, reject bundle và rerun). |
| Security và runbook | [SEC-01](../evidence/SEC-01_CLOUD_20261119.md) và [DOC-01](../evidence/DOC-01.md) còn phù hợp với phạm vi demo; asset mở trên màn hình đã được kiểm. |
| Walkthrough | [DEMO-01](../task/tasks/DEMO-01.md) hoàn tất các gate official. Ba người hoàn thành walkthrough 15–20 phút, kiểm fallback và chốt không còn blocker Core. |
| Phát hành | HoaiTam xác nhận release candidate và tag đề xuất sau review. |

Sao chép [mẫu biên bản](./DEMO-02_REHEARSAL_RECORD.md) vào staging trước buổi tập.
Sau walkthrough, cập nhật thời lượng thực tế, người trình bày, checksum và
evidence tương ứng. Tiếp tục ghi `NOT_RUN` ở các cổng chưa thực hiện.
Preflight, tệp dự phòng và report pilot dùng để chuẩn bị máy demo. Ghi quyết
định phát hành và tag khi đủ bằng chứng chính thức và HoaiTam xác nhận.
