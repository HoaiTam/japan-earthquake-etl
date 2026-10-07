# Airflow tests

`test_etl_pipeline_runtime.py` kiểm tra ORC-01 context/phase gates, checksum/
scope/snapshot bundle, real subprocess protocol bằng mock và việc mock không
Published. `test_etl_dag_contract.py` dựng graph bằng SDK double để assert
edges/groups/strict final leaf, không chỉ tìm chuỗi trong source. Chạy riêng
bằng `make test-orchestration`; import bằng Airflow thật và giới hạn nghiệm
thu xem [ETL orchestration contract](../../docs/specs/ETL_ORCHESTRATION_CONTRACT.md).

Đặt DAG import test và unit test cho orchestration tại đây. Test xuyên service
hoặc cần toàn bộ Compose stack thuộc `tests/integration/`.

Chạy static contract test mà không cần cài Airflow trên host:

```bash
python3 -m unittest discover -s airflow/tests -p 'test_*.py'
```

Import và scheduler execution thật được kiểm tra trong container bằng
`./scripts/smoke-airflow.sh`.

`test_jma_backfill_runtime.py` kiểm tra planner, preview, process protocol,
retry evidence, partial/missing results và strict gate bằng mock.
`test_jma_dag_contract.py` kiểm tra mapping/concurrency/dependency và image
wiring qua source AST/static; không import Airflow hoặc chạy scheduler thật.
`make test-jma-backfill` gom cả test Java và Python riêng JMA-04; HTTP mock
Java cần quyền bind localhost, không cần nguồn thật hoặc Docker daemon.
