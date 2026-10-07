# Airflow module

Module này chứa orchestration code của pipeline.

- `dags/`: DAG và helper được Airflow import.
- `tests/`: unit test và DAG import test.

`AFL-01` cung cấp runtime Airflow 3.3.2 dùng `LocalExecutor`, PostgreSQL
metadata DB và DAG `afl_01_smoke`. DAG smoke chỉ xác nhận scheduler thực thi
được task; nó không gọi USGS, MinIO hoặc Spark. Contract service, trình tự init
và cách kiểm thử nằm tại
[Airflow local contract](../docs/specs/AIRFLOW_LOCAL.md).

`USG-04` thêm DAG `usg_04_usgs_ingest` để điều phối cửa sổ USGS daily, gọi
runner của `USG-02`/`USG-03`, kiểm tra `BronzeReady` và ghi run summary. DAG
không truyền raw payload qua XCom; contract runner nằm tại
[USGS Airflow ingest contract](../docs/specs/USGS_AIRFLOW_INGEST_CONTRACT.md).

`USG-06` đóng gói runner Java ngay trong custom Airflow image và truyền
pipeline-scoped MinIO credential cho LocalExecutor. Fixed live smoke trigger
window `[2023-01-01, 2023-01-04)`, xác minh `BronzeReady` rồi rerun cùng
identity; quy trình nằm tại
[USGS live Bronze runbook](../docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md).

Log, metadata database, credential và dữ liệu staging là runtime state, không
được commit vào module. Compose chỉ bind `dags/` tới `/opt/airflow/dags` ở chế
độ read-only; log và staging dùng named volume riêng.

`JMA-04` thêm DAG manual `jma_04_year_backfill` và task group
`jma_year_backfill`: explicit years/range, preview mặc định, mapped archive
tasks giới hạn concurrency, retry và summary chạy cả khi archive lỗi. Java
runner nối downloader/writer, verify exact publication rồi mới trả Ready;
preview không tạo BronzeReady giả. Thành phần, protocol và lệnh vận hành xem
[JMA year backfill](../docs/specs/JMA_YEAR_BACKFILL.md).

`JMA-05` bổ sung `make smoke-jma-live`: profile QA trigger preview/first/rerun
trên DAG JMA thật cho 1997/2000/2023, khôi phục pause state, giữ evidence trên
shared staging; Java đối soát exact raw/manifest và DAT-01. Test harness
offline không thay live gate. Xem [JMA Bronze QA](../docs/specs/JMA_BRONZE_QA.md).
