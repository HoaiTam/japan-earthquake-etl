# Airflow module

Module này chứa orchestration code của pipeline.

- `dags/`: DAG và helper được Airflow import.
- `tests/`: unit test và DAG import test.

Log, metadata database, credential và dữ liệu staging là runtime state, không
được commit vào module. Compose chỉ bind `dags/` tới `/opt/airflow/dags` ở chế
độ read-only; log và staging dùng named volume riêng.

