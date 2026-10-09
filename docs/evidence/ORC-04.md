# ORC-04 — Evidence logging và run summary

## Phạm vi

- Ngày 2026-10-09 (Asia/Ho_Chi_Minh), branch
  `feat/orc-04-structured-run-summary` đã rebase từ nền ban đầu `ff4f460`
  lên PR #46 head `97f9ac58d52c2fee787b5d57d1f974f83264752b` theo yêu cầu.
  [PR #47](https://github.com/HoaiTam/japan-earthquake-etl/pull/47) gồm ORC-04
  và integration dispatcher ORC-03; dependency PR #46 phải merge trước.
  Backup local của head cũ: `backup/orc-04-before-pr46-20261009` (`5084207`).
- Dependency ORC-01 Done; assignee HoaiTam, reviewer unassigned. Không tự ghi
  approval độc lập. User cho phép cập nhật branch/PR #47; không merge PR.
- [Contract và runbook](../specs/RUN_OBSERVABILITY_CONTRACT.md),
  [runtime report](./ORC-04-runtime.json).
- Nghiệm thu **telemetry runtime/contract**, không nghiệm thu business Silver/
  Gold adapters hoặc E2E Published thay cho các task downstream.

## Evidence ban đầu trước cập nhật nền (lịch sử)

| Lệnh / kiểm tra | Kết quả | Loại evidence |
|---|---|---|
| `make test-observability` | 34 telemetry + 25 ORC-01 tests, passed | Offline synthetic metadata/process mocks |
| `env JAVA_HOME=/Users/hoaitam/Library/Java/JavaVirtualMachines/azul-21.0.11/Contents/Home make check` | Exit 0, 167 Java + 123 Airflow + 12 Docker-build-input tests; contract/config/static Compose passed | Regression; Java HTTP mocks localhost, không gọi nguồn thật |
| `make observability-smoke` | Mock success → injected failure → same-run rerun, passed | Host runtime API; chỉ metadata staging |
| `make smoke-observability` | Exit 0, 4 checks true, published=false | Python runtime trong Airflow 3.3.2; không scheduler trigger |
| Native Airflow DagBag import tất cả DAG files | No import errors; ORC-01 8/ORC-02 7 tasks, strict leaves và failure callback tồn tại | Actual runtime graph, không SDK double |
| `run_summary_cli --source-evidence` với hai exact files dưới đây | JMA 423222/4 manifests; USGS 16/1 manifest; parsed=null/published=false | Saved real Bronze receipts, **không fresh source/storage readback** |

Không cộng test chạy lặp thành số test độc lập. `make check` chạy lại Java/
Python qua nhiều wrapper; số bên trên là test cases riêng tại baseline này.
Không đổi `.env`, image/JAR, restart service, trigger/unpause DAG, tải nguồn,
scan bucket hoặc sửa/delete business data để tạo evidence.

## Cập nhật nền PR #46 và tích hợp ORC-03

Evidence mới phân biệt với phần lịch sử bên trên:

| Kiểm tra | Kết quả | Ý nghĩa |
|---|---|---|
| `make test-observability` | 34 telemetry + 18 backfill telemetry + 25 ORC-01 tests, passed | Scoped receipts/equations, failure từng phase, unknown/empty counts, lease/release gate, tampered result, retry/rerun và preview không invalidate run cũ |
| `make check` với JDK Azul 21.0.11 | 179 Java + 163 Airflow + 12 build-input tests; config/contract/static checks passed | Toàn repo trên nền PR #46; không cộng những lần wrapper chạy lại |
| `make smoke-observability` | 4 checks true, published=false | Mock success/failure/rerun qua runtime hiện có |
| Native Airflow DagBag | No import errors; ORC-01/02/03 = 8/7/5 tasks; callbacks và strict leaves đạt | ORC-03 vẫn manual, cleanup all_done, sole leaf all_success |
| `make smoke-backfill-observability` | Exit 0; JMA 257020, USGS 16; same-run rerun counts unchanged; lease released | Fresh exact raw/manifest readback qua Java, không replay receipts |

Runtime mock mới: `orc04-smoke-ff65800fbb804d49bb43923339f084d7`.
Runtime Bronze QA: `orc04-backfill-qa-0ca071e38e2348bdac4d0f7acc48d2b5`,
operation `orc03-reuse-pilot-v1`, scope SHA
`6d07b418c4d7e0ec66e549eedb21536a3c453949ce7f1a04a55a20130035968d`.
Hai exact manifests được pin trong `airflow/dags/fixtures/orc_03_reuse_sample.json`;
JMA sample này là full-year 2023, không phải cả bốn manifests của replay ORC-02.
JMA structural count/USGS feature count không phải tổng canonical earthquakes.

QA summary mới:
`/opt/pipeline/staging/run-summary/qa/f2e6cd709e2d2ef2ba36c3ebac62b63e858907ddcb34804c3c6d5a8bce68a5ac/run_summary.json`.
Trạng thái `BronzeVerified`, parsed/fetched/Gold counts null, published=false.
Không có Silver/Gold commit hoặc scheduler E2E từ evidence này.

Target mới build Java/image và chạy container one-off (`--no-deps`), không
restart các service đang chạy, không unpause/trigger DAG, gọi HTTP nguồn,
scan prefix hoặc ghi/xóa object lake. Chỉ ghi metadata QA/control staging;
normal runner của container đang chạy không bị thay. Muốn deploy JAR mới cho
scheduler sau review cần `make up-airflow` lúc không có source run đang chạy.

Đọc summary QA (read-only):

```bash
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python -m run_summary_cli --inspect --root /opt/pipeline/staging/run-summary/qa --dag-id orc_03_backfill --run-id orc04-backfill-qa-0ca071e38e2348bdac4d0f7acc48d2b5
```

## Acceptance mapping

| Acceptance | Evidence |
|---|---|
| Counts đối soát từ run ID | Synthetic phase receipts chạy qua runtime validators/journal/final gate, bốn source equations + primary reject + Bronze/Silver + Silver/Gold + verify/publish; saved real Bronze counts giữ exact URI/SHA/release/source scope |
| Không log secret/payload lớn | Unknown secret fields và credential URI fail trước projection; raw exceptions/stderr/conf/env không log; nested persistence allowlist, bounded JSON, secret sentinel assertions cả files/logs |
| Lỗi có reason rõ | Failure mỗi phase, failed resolve, timeout/process/quality/count reason registry; status/phase/not-executed được persist rồi raise, DAG callback fallback |
| Empty/rerun | Zero là count hợp lệ khác null; same-run rerun không cộng input hoặc giữ stale snapshots; stale attempt/context/summary từ interrupted write bị reject |

Full linked/current/revision fixtures có 10 input/source = 8 parsed + 1
parser error + 1 ignored; 8 parsed = 6 valid + 2 quality reject; 6 valid = 1
duplicate + 2 superseded + 3 current; 3 current = 2 linked memberships + 1
unlinked. Hai nguồn cho Gold 6 bridge memberships / 4 canonical events. Đây
là **fixture unit**, không count business từ catalog thật.

## Runtime/API smoke lặp lại

```bash
make test-observability
make observability-smoke
make smoke-observability
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python -m run_summary_cli --inspect --root /opt/pipeline/staging/run-summary/qa --dag-id orc_01_etl_pipeline --run-id orc04-smoke-f375c4243add4529bf34ef1aa34aa6b6
```

Để kiểm tra graph thực (read-only, không trigger):

```bash
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python -c 'from airflow.dag_processing.dagbag import DagBag; b=DagBag(dag_folder="/opt/airflow/dags",safe_mode=False); assert not b.import_errors; d=b.dags["orc_01_etl_pipeline"]; assert len(d.tasks)==8 and {t.task_id for t in d.leaves}=={"publication.contract_gate"}; assert d.on_failure_callback.__name__=="dag_failure_summary"; d=b.dags["orc_02_daily_sources"]; assert len(d.tasks)==7 and {t.task_id for t in d.leaves}=={"completion_gate"}; assert d.on_failure_callback.__name__=="dag_failure_summary"; print("ORC-04 DagBag passed")'
```

### Saved source metadata test

Exact input files của run ORC-02 đã chạy ngày 2026-10-08:

- `/opt/pipeline/staging/readiness/daily-8094129e926f712194fbb9e8285fbc42/plan.json`
- `/opt/pipeline/staging/readiness/daily-8094129e926f712194fbb9e8285fbc42/run_summary.json`

```bash
docker compose exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor python -m run_summary_cli --source-evidence --plan-file /opt/pipeline/staging/readiness/daily-8094129e926f712194fbb9e8285fbc42/plan.json --source-summary-file /opt/pipeline/staging/readiness/daily-8094129e926f712194fbb9e8285fbc42/run_summary.json --root /opt/pipeline/staging/run-summary/qa
```

JMA 1997 jan-sep/oct-dec + 2000 + 2023 tổng structural count estimate 423222,
USGS window `[2023-01-01,2023-01-04)` count 16. Exact manifest/raw/release/
checksums giữ trong projected persisted QA summary; metadata input gốc xem
[ORC-02 evidence](./ORC-02.md). Hai file metadata không tồn tại thì command
fail, không fallback download. Mode replay và QA ID mới, không sửa summary/
manifest của run cũ hoặc giả lập completed scheduler run.

## Giới hạn / task tiếp theo

- **SLV-06 - Deduplicate và xử lý revision trong từng nguồn**, **SLV-07 - Liên
  kết observation và chọn canonical event**, **SLV-09 - Tích hợp và kiểm thử
  Silver đa nguồn**: emit real counts/primary reasons/memberships vào contract.
- **GLD-01 - Xây canonical event, dimensions và bands**, **GLD-03 - Ghi Gold
  Iceberg và commit snapshot**, **GLD-04 - Tạo Trino views và verification SQL**:
  commit/readback/query counts + freshness metadata thật. Chưa có Gold
  Published hoặc real full-layer reconciliation trong evidence này.
- **ORC-03 - Implement backfill và reprocessing**: dispatcher/scoped-phase
  observer đã nối trên PR #46; đây là dependency branch, merge #46 trước #47.
  Scoped adapter thật vẫn cần SLV-09/GLD-03/GLD-04, không dùng double như live proof.
- **ORC-05 - Chốt recovery, concurrency và tài nguyên**: abandoned RUNNING/
  stale lease/admission; callback không bảo đảm chạy khi kill/manual state change.
- **QA-01 - Chạy E2E daily đa nguồn**: scheduler E2E, không thay bằng API smoke.
- **SEC-01 - Review secret và bề mặt truy cập**: security review tổng thể.
- Log retention/metrics backend ngoài JSON **chưa có task** được chốt riêng.
  Không benchmark full history hoặc tuyên bố Prometheus/Grafana đã có.

Reviewer chưa có; review additive count contract và scope semantics trước
khi bật real adapter emits. PR hiện tại: #47.
