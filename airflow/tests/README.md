# Airflow tests

Đặt DAG import test và unit test cho orchestration tại đây. Test xuyên service
hoặc cần toàn bộ Compose stack thuộc `tests/integration/`.

Chạy static contract test mà không cần cài Airflow trên host:

```bash
python3 -m unittest discover -s airflow/tests -p 'test_*.py'
```

Import và scheduler execution thật được kiểm tra trong container bằng
`./scripts/smoke-airflow.sh`.
