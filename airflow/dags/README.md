# Airflow DAGs

Đặt DAG production và helper chỉ phục vụ DAG tại đây. Mỗi DAG phải khai báo rõ
schedule/timezone, data interval, retry, dependency và run context theo tài
liệu flow. Không đọc secret trực tiếp từ file trong repository.

`afl_01_smoke.py` là DAG thủ công của foundation. DAG luôn được unpause khi tạo,
không catchup và không truy cập hệ thống bên ngoài để smoke test phản ánh riêng
khả năng parse, schedule và execute task của Airflow.
