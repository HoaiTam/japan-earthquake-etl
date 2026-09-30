# Cross-module tests

- [`fixtures/`](./fixtures/README.md): fixture synthetic USGS/JMA dùng chung,
  có machine-readable cases, test matrix, checksum và offline validator.
- `integration/`: test xuyên module hoặc service.

Unit test riêng của DAG nằm trong `airflow/tests/`; unit test Spark nằm trong
`spark/src/test/`. Không commit output, data lake snapshot hoặc credential.

Kiểm tra fixture trước khi chạy test module:

```bash
./scripts/check-shared-fixtures.sh
```
