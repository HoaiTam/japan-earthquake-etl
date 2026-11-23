# DEMO-02 — Rehearsal và release candidate

[DEMO-02](../task/tasks/DEMO-02.md) đang chuẩn bị buổi chạy thử. Dùng gói,
manifest và preflight của [DEMO-01](./DEMO-01_DEMO_PACKAGE.md). Tiêu chí bàn
giao Core theo [MVP Definition of Done](../specs/MVP_SCOPE_KPI_AND_DOD.md).

## Chạy trước trên máy trình bày

1. Checkout đúng commit được chọn để demo và ghi `git rev-parse HEAD` vào
   biên bản. Với buổi tập offline hiện tại, dùng branch của
   [PR #143](https://github.com/HoaiTam/japan-earthquake-etl/pull/143), nơi đã
   có manifest v2.
2. Chạy các lệnh dưới đây từ root của checkout. Lưu preflight JSON và report
   vào `staging/demo-02/` thuộc Git ignore.

~~~bash
mkdir -p staging/demo-02
python -B scripts/demo-preflight.py --json > staging/demo-02/preflight.json
python -B -m unittest tests.test_demo_preflight
python -B scripts/generate-experiment-report.py --mode development --input-json docs/evidence/EXP-04_PILOT_EVALUATION.json --output-md staging/demo-02/MLI-04_DEVELOPMENT_ONLY.md
~~~

3. Nếu dùng receipt DAT-03 cá nhân, chạy lại preflight với `--local-receipt`
   và `--expected-receipt-sha256` theo runbook DEMO-01. Preflight đối chiếu
   receipt và pin đã ghi. Để kiểm lại bytes của kit trên đĩa, chạy verifier
   theo [hướng dẫn DAT-03](./DAT-03_OFFICIAL_RESEARCH_HANDOFF.md).
4. Đọc `overall_status`, `official_release_ready` và `pending_official_gates`
   trong preflight. `PREPARED_PENDING_OFFICIAL_GATES` cho phép tập offline.
   Chỉ dùng chế độ official sau `PASS_OFFICIAL_READY` và khi có đúng evidence
   MLI-04/MLQ-01 trên máy trình bày.

## Trình tự đề xuất cho ba vai

| Mốc | Vai đề xuất | Việc trình bày và bàn giao |
|---|---|---|
| 00:00–05:00 | Điều phối ETL | Nêu phạm vi rồi mở bản ghi QA-01. Chỉ run ID, publication ID và các snapshot Gold. Chuyển bằng tên dataset và Gold snapshot. |
| 05:00–11:00 | Dữ liệu và thí nghiệm | Mở evidence DAT-03 với nhãn EXPORTED. Trình bày dataset pin và kết quả EXP-04 theo đúng phạm vi. Ghi rõ `DEVELOPMENT_ONLY` khi dùng pilot. Bàn giao dataset ID và run ID cho phần đọc lại. |
| 11:00–16:00 | Kiểm chứng và báo cáo | Chỉ cách nối receipt, snapshot, Trino query và static report. Khi tập offline, dùng report pilot và nêu các gate còn chờ. Diễn giải kết quả là aftershock candidate hồi cứu. |
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
| Import và báo cáo ML | [MLI-03](../task/tasks/MLI-03.md) có receipt chính thức cần dùng. [MLI-04](../task/tasks/MLI-04.md) có đối soát Trino trên exact snapshots. [MLQ-01](../task/tasks/MLQ-01.md) có receipt Gold đến static report. |
| Security và runbook | [SEC-01](../evidence/SEC-01_CLOUD_20261119.md) và [DOC-01](../evidence/DOC-01.md) còn phù hợp với phạm vi demo; asset mở trên màn hình đã được kiểm. |
| Walkthrough | [DEMO-01](../task/tasks/DEMO-01.md) hoàn tất các gate official. Ba người hoàn thành walkthrough 15–20 phút, kiểm fallback và chốt không còn blocker Core. |
| Phát hành | HoaiTam xác nhận release candidate và tag đề xuất sau review. |

Ghi các mục sau vào biên bản sau buổi tập:

- Thời gian, commit của checkout, config version, manifest và SHA-256.
- Người thực hiện từng vai, thời lượng thực tế, phần trình bày đạt hoặc cần tập lại.
- Gold publication và snapshot IDs, dataset IDs, experiment run IDs, receipt
  checksum, Trino query IDs và SHA của static report.
- Phương án dự phòng đã thử, lỗi quan sát được, người xử lý và kết quả kiểm lại.
- Blocker Core, phần Stretch để sau, người xác nhận và tag đề xuất.

Ghi `NOT_RUN` cho mục chưa thực hiện. Hiện lần kiểm offline chỉ xác nhận preflight,
tệp dự phòng và report pilot. Kết quả team rehearsal, quyết định phát hành và
tag được ghi sau khi các cổng trên đạt.
