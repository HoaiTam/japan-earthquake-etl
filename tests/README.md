# Cross-module tests

- `fixtures/`: fixture nhỏ dùng chung cho nhiều module.
- `integration/`: test xuyên module hoặc service.

Unit test riêng của DAG nằm trong `airflow/tests/`; unit test Spark nằm trong
`spark/src/test/`. Không commit output, data lake snapshot hoặc credential.

