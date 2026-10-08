# Airflow DAGs

Đặt DAG production và helper chỉ phục vụ DAG tại đây. Mỗi DAG phải khai báo rõ
schedule/timezone, data interval, retry, dependency và run context theo tài
liệu flow. Không đọc secret trực tiếp từ file trong repository.

`orc_01_etl_pipeline.py` là khung DAG manual/paused đến Gold, 6 task groups,
strict `all_success`. `etl_pipeline_runtime.py` kiểm tra versioned context và
phase metadata, không xử lý raw records. Fixture ở `fixtures/orc_01_mock_v1.json`
chỉ trả `MockComplete`; real mode thiếu adapter/scope sẽ fail closed. Chi tiết
handoff và task runtime còn thiếu ở
[ETL orchestration contract](../../docs/specs/ETL_ORCHESTRATION_CONTRACT.md).

`afl_01_smoke.py` là DAG thủ công của foundation. DAG luôn được unpause khi tạo,
không catchup và không truy cập hệ thống bên ngoài để smoke test phản ánh riêng
khả năng parse, schedule và execute task của Airflow.

`usg_04_usgs_ingest.py` là DAG USGS (manual mặc định ORC-02, daily ở profile
`usgs-only`) có task group `usgs_ingest` gồm
resolve/fetch/validate/upload/verify và `bronze_ready_gate`. DAG chỉ gọi runner
được cấu hình qua `USGS_INGEST_RUNNER_COMMAND`; context/summary nằm trong
staging volume, không đưa raw payload vào XCom. Chi tiết protocol nằm trong
[USGS Airflow ingest contract](../../docs/specs/USGS_AIRFLOW_INGEST_CONTRACT.md).
Runner thật được đóng gói trong custom Airflow image bởi `USG-06`; fixed-window
operator flow và evidence xem tại
[USGS live Bronze runbook](../../docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md).

`jma_04_year_backfill.py` là DAG manual JMA, paused khi tạo, không catchup.
Task group map exact năm/segment đã preview, giữ hai archive 1997, không đưa
ZIP vào XCom. Helper `jma_backfill_runtime.py` quản lý context/subprocess và
failure summary; Java xử lý HTTP/ZIP/MinIO. Hướng dẫn ở
[JMA year backfill](../../docs/specs/JMA_YEAR_BACKFILL.md).

`orc_02_daily_sources.py` là daily owner trong profile `multi-source`: explicit
UTC data interval, JMA watchlist/change/weekly checksum checks và USGS Bronze
thật. `source_schedule_runtime.py` chỉ xử lý metadata; Java giữ mọi source
I/O/validation. `source_run_guard.py` khóa whole run giữa cả ba source DAG;
all_done cleanup không che failure vì có strict completion leaf. `make
test-source-schedule` / `make smoke-source-readiness` và ranh giới SourcesReady
vs Published xem [contract ORC-02](../../docs/specs/SOURCE_SCHEDULE_AND_READINESS.md).
