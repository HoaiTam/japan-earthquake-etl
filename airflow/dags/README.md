# Airflow DAGs

Đặt DAG production và helper chỉ phục vụ DAG tại đây. Mỗi DAG phải khai báo rõ
schedule/timezone, data interval, retry, dependency và run context theo tài
liệu flow. Không đọc secret trực tiếp từ file trong repository.

`afl_01_smoke.py` là DAG thủ công của foundation. DAG luôn được unpause khi tạo,
không catchup và không truy cập hệ thống bên ngoài để smoke test phản ánh riêng
khả năng parse, schedule và execute task của Airflow.

`usg_04_usgs_ingest.py` là DAG daily USGS có task group `usgs_ingest` gồm
resolve/fetch/validate/upload/verify và `bronze_ready_gate`. DAG chỉ gọi runner
được cấu hình qua `USGS_INGEST_RUNNER_COMMAND`; context/summary nằm trong
staging volume, không đưa raw payload vào XCom. Chi tiết protocol nằm trong
[USGS Airflow ingest contract](../../docs/specs/USGS_AIRFLOW_INGEST_CONTRACT.md).
Runner thật được đóng gói trong custom Airflow image bởi `USG-06`; fixed-window
operator flow và evidence xem tại
[USGS live Bronze runbook](../../docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md).
