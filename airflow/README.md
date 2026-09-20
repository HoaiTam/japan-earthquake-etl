# Airflow module

Module này chứa orchestration code của pipeline.

- `dags/`: DAG và helper được Airflow import.
- `tests/`: unit test và DAG import test.

`AFL-01` cung cấp runtime Airflow 3.3.2 dùng `LocalExecutor`, PostgreSQL
metadata DB và DAG `afl_01_smoke`. DAG smoke chỉ xác nhận scheduler thực thi
được task; nó không gọi USGS, MinIO hoặc Spark. Contract service, trình tự init
và cách kiểm thử nằm tại
[Airflow local contract](../docs/specs/AIRFLOW_LOCAL.md).

Log, metadata database, credential và dữ liệu staging là runtime state, không
được commit vào module. Compose chỉ bind `dags/` tới `/opt/airflow/dags` ở chế
độ read-only; log và staging dùng named volume riêng.
