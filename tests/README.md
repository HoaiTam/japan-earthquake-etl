# Cross-module tests

- [`fixtures/`](./fixtures/README.md): fixture synthetic USGS/JMA dùng chung,
  có machine-readable cases, test matrix, checksum và offline validator.
- `integration/`: test xuyên module hoặc service.
- `test_java_build_inputs.py`: regression test đầu vào Maven trong Docker
  Spark/Airflow. Tạo repository tạm nhỏ để kiểm tra thiếu COPY/input/allowlist,
  COPY sau Maven hoặc sai stage, comment và đường dẫn có khoảng trắng; không
  thay đổi fixture thật, đọc `.env`, gọi Docker/Maven hay nguồn dữ liệu.

Unit test riêng của DAG nằm trong `airflow/tests/`; unit test Spark nằm trong
`spark/src/test/`. Không commit output, data lake snapshot hoặc credential.

Kiểm tra fixture trước khi chạy test module:

```bash
./scripts/check-shared-fixtures.sh
```

Kiểm tra đầu vào Docker builder và 12 regression test (cũng nằm trong
`make test-contracts`, `make test` và `make check`):

```bash
make check-java-build-inputs
```

Test này chỉ kiểm tra packaging contract; Docker build thật vẫn cần chạy để
kiểm chứng môi trường Java 17. Chi tiết tại
[Spark standalone contract](../docs/specs/SPARK_STANDALONE.md#41-đầu-vào-kiểm-thử-trong-docker-builder).
