# Command wrappers only: scripts remain the source of validation/runtime logic.
# Compatible with GNU Make 3.81 (the default on macOS).
SHELL := /bin/sh
.DEFAULT_GOAL := help
.NOTPARALLEL:

ENV_FILE ?= .env
CHECK_ENV_FILE ?= .env.example
COMPOSE_FILE ?= compose.yaml
WAIT_TIMEOUT ?= 300
DOCKER_CHECK_TIMEOUT ?= 20
export DOCKER_CHECK_TIMEOUT
SERVICE ?=
TAIL ?= 100
DAT01_AIRFLOW_CONTAINER ?=
JMA_YEARS ?=
BACKFILL_CONF ?= airflow/dags/fixtures/orc_03_ingest_preview.json
SILVER_RUNNER_JAR ?=
RUN_SUMMARY_ROOT ?= staging/run-summary
RUN_SUMMARY_DAG ?= orc_01_etl_pipeline
RUN_SUMMARY_RUN_ID ?=
SECURITY_REPORT ?= staging/sec-01-report.json
QA_02_MATRIX ?= airflow/dags/fixtures/qa_02_rerun_revision_matrix.json
QA_02_JSON ?= staging/qa-02/qa-02.json
QA_02_MARKDOWN ?= staging/qa-02/qa-02.md
QA_02_RUNTIME_REQUEST ?=
QA_02_RUNTIME_COMMAND ?=

COMPOSE = docker compose --env-file "$(ENV_FILE)" -f "$(COMPOSE_FILE)"
RUNTIME_ENV = ENV_FILE="$(ENV_FILE)" COMPOSE_FILE="$(COMPOSE_FILE)"
RUNTIME_CHECK = $(RUNTIME_ENV) DOCKER_CHECK_TIMEOUT="$(DOCKER_CHECK_TIMEOUT)" python3 scripts/docker-runtime.py check
CHECK_ENV = ENV_FILE="$(CHECK_ENV_FILE)" COMPOSE_FILE="$(COMPOSE_FILE)"
FOUNDATION_SERVICES := minio airflow-postgres airflow-api-server \
	airflow-scheduler airflow-dag-processor spark-master spark-worker iceberg-rest trino

CONTRACT_CHECKS := check-mvp-baseline check-repository-layout \
	check-source-coverage check-bronze-contract check-data-model-contract \
	check-shared-fixtures check-real-sample-catalog check-week-3-plan check-jma-inventory check-task-status
STATIC_CHECKS := check-compose check-minio check-airflow check-spark \
	check-query check-usgs-live
COMPONENT_SMOKES := smoke-minio smoke-airflow smoke-spark smoke-query smoke-usgs-live smoke-jma-live

.PHONY: help env-init require-env check-config check-config-local config \
	test test-contracts test-java test-jma-bronze test-jma-parser test-jma-backfill test-jma-qa jma-preview test-airflow test-orchestration etl-preview etl-mock package-java check \
	$(CONTRACT_CHECKS) $(STATIC_CHECKS) check-java-build-inputs check-foundation check-jma-inventory-live build-shared-fixtures \
	build up start up-minio up-airflow up-spark up-query status ps logs logs-follow \
	restart stop down smoke smoke-foundation $(COMPONENT_SMOKES) \
	verify-samples verify-real-samples

.PHONY: test-source-schedule smoke-source-readiness
.PHONY: docker-build docker-up docker-check test-docker-runtime
.PHONY: test-recovery smoke-recovery smoke-resource-pilot maintenance-preview
.PHONY: test-silver-integration smoke-silver-integration
.PHONY: test-ml-audit smoke-ml-audit
.PHONY: test-qa-02 smoke-qa-02
.PHONY: test-ml-window smoke-ml-window resume-ml-window
.PHONY: prepare-ml-local stage-ml-windows test-ml-feature ml-features-local ml-pilot-local pilot-env test-pilot inspect-pilot jupyter-pilot
.PHONY: pack-pilot
.PHONY: prepare-full-month test-full-month qa-full-month-live full-month-local
FULL_MONTH_ROOT ?= staging/orc06-full-month
FULL_MONTH_REQUEST ?=

prepare-full-month: ## ORC-06 chuẩn bị JAR/classpath host một lần; không Docker, không lấy dữ liệu
	@python3 scripts/local-ml.py prepare

test-full-month: ## ORC-06 synthetic coverage/revision/closure/state tests; Spark local, không nguồn thật/Docker
	@python3 -m unittest discover -s tests -p 'test_full_month_*.py'
	@SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost ./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=FullMonthStateResolverTest -Dsurefire.failIfNoSpecifiedTests=false

qa-full-month-live: ## ORC-06 public captures đủ tháng 2023-09, file-backed QA; cần prepare trước, không MinIO/Gold write
	@python3 scripts/full-month-state.py live --root "$(FULL_MONTH_ROOT)"

full-month-local: ## ORC-06 replay request exact trên file-backed stores trong FULL_MONTH_ROOT, không tải/build/start
	@python3 scripts/full-month-state.py local --request "$(FULL_MONTH_REQUEST)" --root "$(FULL_MONTH_ROOT)"
ML_WINDOW_INPUT ?= staging/ml-window-inputs/reproduction-v1
ML_TRINO_URL ?= http://127.0.0.1:8081
ML_SCALING ?= config/ml/feature_scaling_global_dev_v1.json
PILOT_SELECTION ?= config/ml/dat02_reproduction_v1.json
PILOT_OUTPUT ?= staging/ml-pilot
PILOT_PYTHON ?= staging/venvs/dat02/bin/python
PILOT_BUNDLE ?=
PILOT_ZIP ?= staging/dat02-pilot-handoff.zip

prepare-ml-local: ## Chuẩn bị JAR/classpath Spark local một lần; cần JAVA_HOME=JDK17/21, không Docker
	@python3 scripts/local-ml.py prepare

stage-ml-windows: ## Copy/read/hash đúng MLD-03 receipt/parts đã có; không ingest/start/build
	@python3 scripts/stage-ml-windows.py --output "$(ML_WINDOW_INPUT)"

test-ml-feature: ## Offline Spark tests feature/scaling/Parquet; không Docker hoặc API
	@SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost ./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=FeatureScalingConfigTest,SpacetimeFeatureEngineTest,FeatureParquetCrossProcessTest -Dsurefire.failIfNoSpecifiedTests=false test

ml-features-local: ## Read-only Trino pinned Gold + shared MLD-04 features; không tự prepare
	@python3 scripts/local-ml.py features --input "$(ML_WINDOW_INPUT)" --scaling "$(ML_SCALING)" --trino "$(ML_TRINO_URL)"

ml-pilot-local: ## DAT-02 export fixed selection development-only; không build/start hoặc EXPORTED
	@python3 scripts/local-ml.py pilot --input "$(ML_WINDOW_INPUT)" --scaling "$(ML_SCALING)" --selection "$(PILOT_SELECTION)" --output "$(PILOT_OUTPUT)" --trino "$(ML_TRINO_URL)"

pilot-env: ## Chuẩn bị venv notebook riêng một lần; cần Internet, không cài global
	@python3 -m venv staging/venvs/dat02
	@staging/venvs/dat02/bin/python -m pip install -r notebooks/requirements-pilot.txt

test-pilot: ## Synthetic loader/staging regression; cần pyarrow, không Docker/network/notebook kernel
	@$(PILOT_PYTHON) -m unittest discover -s tests -p 'test_pilot_*.py'

inspect-pilot: ## Fresh Jupyter kernel Run All; PILOT_BUNDLE=exact local directory, không Docker/API
	@test -n "$(PILOT_BUNDLE)" || { echo 'Set PILOT_BUNDLE=staging/ml-pilot/pilot_id=<exact-id>'; exit 2; }
	@$(PILOT_PYTHON) scripts/inspect-pilot-notebook.py "$(PILOT_BUNDLE)"

pack-pilot: ## Đóng đúng 5 file pilot đã verify để gửi team; PILOT_BUNDLE và PILOT_ZIP, không overwrite khác bytes
	@test -n "$(PILOT_BUNDLE)" || { echo 'Set PILOT_BUNDLE=staging/ml-pilot/pilot_id=<exact-id>'; exit 2; }
	@$(PILOT_PYTHON) scripts/pack-pilot.py "$(PILOT_BUNDLE)" "$(PILOT_ZIP)"

jupyter-pilot: ## Mở Jupyter loopback với bundle local đã chọn; Ctrl+C để dừng, không tự export
	@test -n "$(PILOT_BUNDLE)" || { echo 'Set PILOT_BUNDLE=staging/ml-pilot/pilot_id=<exact-id>'; exit 2; }
	@PILOT_BUNDLE="$(abspath $(PILOT_BUNDLE))" $(PILOT_PYTHON) -m jupyterlab --ip=127.0.0.1 --no-browser notebooks/dat_02_inspection.ipynb
export QA_RUN_ID

test-ml-window: ## MLD-03 bucket physical plan, distributed invariants, microseconds và fail-fast Parquet tests
	@python3 -m unittest discover -s airflow/tests -p 'test_mainshock_window_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=WindowModelResolverTest,MainshockWindowEngineTest,MainshockWindowIntegrationTest,MainshockWindowRegressionTest,MainshockRuntimeTest,JmaMainshockPilotSamplerTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

smoke-ml-window: check-config-local ## MLD-03 runtime QA; cần docker-build/docker-up, không tự build/start
	@$(RUNTIME_CHECK) --component ml
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never mainshock-window-qa

resume-ml-window: check-config-local ## MLD-03 own retained QA lease + Published Gold, cần QA_RUN_ID và runner đã package
	@$(RUNTIME_CHECK) --component ml
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never -e QA_RUN_ID mainshock-window-qa \
		'exec python3 /opt/airflow/dags/mainshock_window_qa.py --resume-windows "$$QA_RUN_ID"'

test-ml-audit: ## MLD-02 Gold input audit, MAXC completeness estimator, sensitivity & reconciliation tests
	@python3 -m unittest discover -s airflow/tests -p 'test_gold_input_audit_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=MagnitudeCompletenessEstimatorTest,MagnitudeHistogramSparkTest,GoldAuditAcceptanceTest,GoldInputAuditEngineTest,GoldInputAuditIntegrationTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

smoke-ml-audit: check-config-local ## MLD-02 Published JMA audit/Mc QA; không tự build/start
	@$(RUNTIME_CHECK) --component ml --client gold-input-audit-qa
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never gold-input-audit-qa

test-qa-02: ## QA-02 matrix/fixture/report checks; không gọi nguồn hoặc runtime
	@python3 -m unittest discover -s airflow/tests -p 'test_qa_02_rerun_revision.py'
	@python3 airflow/dags/qa_02_rerun_revision.py --matrix "$(QA_02_MATRIX)" --json "$(QA_02_JSON)" --markdown "$(QA_02_MARKDOWN)"

smoke-qa-02: check-config-local ## QA-02 Bronze-to-Published rerun/revision; cần ORC-07 và request runtime đã pin
	@test -n "$(QA_02_RUNTIME_REQUEST)" || { echo 'Set QA_02_RUNTIME_REQUEST=/opt/pipeline/staging/.../request.json'; exit 2; }
	@test -n "$(QA_02_RUNTIME_COMMAND)" || { echo 'Set QA_02_RUNTIME_COMMAND to the QA adapter runner'; exit 2; }
	@$(RUNTIME_CHECK) --component ml --client qa-02-rerun-revision
	@$(COMPOSE) run --rm --no-deps --pull never -e PYTHONPATH=/opt/airflow/dags \
		-e QA_02_RUNTIME_COMMAND="$(QA_02_RUNTIME_COMMAND)" \
		airflow-dag-processor python3 /opt/airflow/dags/qa_02_rerun_revision.py --live "$(QA_02_RUNTIME_REQUEST)"
.PHONY: test-gold-storage smoke-gold-storage
.PHONY: test-gold-publication smoke-gold-publication test-dataset-manifest smoke-dataset-manifest

test-dataset-manifest: ## MLD-01 exact Published snapshot, deterministic identity, rerun và reject fixtures
	@python3 -m unittest discover -s airflow/tests -p 'test_dataset_manifest_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am -Dtest=DatasetManifestTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-dataset-manifest: check-config-local ## MLD-01 manifest BUILDING; cần exact publication, không tự build/start
	@$(RUNTIME_CHECK) --component gold --client dataset-manifest-qa
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never dataset-manifest-qa

test-gold-publication: ## GLD-04 offline publication blockers, Trino HTTP protocol và control plane
	@python3 -m unittest discover -s airflow/tests -p 'test_gold_publication_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=GoldPublicationTest,TrinoSqlClientTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-gold-publication: check-config-local ## GLD-04 exact snapshots qua Trino; không tự build/start
	@$(RUNTIME_CHECK) --component gold --client gold-verification-qa
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never gold-verification-qa

test-gold-storage: ## GLD-03 real Iceberg snapshot/recovery tests và control plane offline
	@python3 -m unittest discover -s airflow/tests -p 'test_gold_storage_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=GoldIcebergWriterTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-gold-storage: check-config-local ## GLD-03 six QA Iceberg snapshots, chưa Published; không tự build/start
	@$(RUNTIME_CHECK) --component gold
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never spark-gold-qa

.PHONY: ci ci-docs ci-java ci-airflow test-ci-workflow test-security-audit audit-security audit-security-runtime
.PHONY: ci-plan ci-docs-base ci-java-impact ci-airflow-impact ci-pilot-impact ci-suite

ci-suite: ## Chạy nhóm test chức năng/task; đặt CI_COMPONENT=java|airflow và CI_SUITE=<id từ ci-plan>
	@python3 scripts/ci-impact.py run-suite

ci-plan: ## In impact plan; CI_EVENT_NAME=pull_request CI_BASE_SHA=<base> CI_HEAD_SHA=HEAD để xem PR local
	@python3 scripts/ci-impact.py plan

ci-java-impact: ## Java theo impact (leaf feature/ML), default/fallback full; không Docker
	@python3 scripts/ci-impact.py run --component java

ci-airflow-impact: ## Airflow khi bị ảnh hưởng; default/fallback full, không Docker
	@python3 scripts/ci-impact.py run --component airflow

ci-pilot-impact: ## Pilot synthetic checks khi bị ảnh hưởng; cần tests/ci/requirements.txt
	@python3 scripts/ci-impact.py run --component pilot

ci: ci-docs ci-java ci-airflow ## Cùng ba nhóm check CI; cần PyYAML, JDK17, Compose CLI, jq/rg; không start service

ci-docs: ## Docs/contracts/secret hygiene/Compose tĩnh; không đọc .env riêng hoặc gọi nguồn thật
	@$(MAKE) ci-docs-base
	@$(MAKE) test-pilot PILOT_PYTHON=python3

ci-docs-base: ## Always-on CI guards; pilot tests được chọn riêng theo impact, không Docker daemon
	@$(MAKE) test-contracts
	@python3 -m unittest discover -s tests -p 'test_full_month_*.py'
	@$(MAKE) test-security-audit
	@$(MAKE) test-docker-runtime
	@$(MAKE) check-config ENV_FILE=/dev/null
	@$(MAKE) check-compose check-minio check-query CHECK_ENV_FILE=.env.example
	@$(MAKE) test-ci-workflow
	@for script in scripts/*.sh; do \
		case "$$(head -n 1 "$$script")" in \
			*bash*) bash -n "$$script" ;; \
			*) sh -n "$$script" ;; \
		esac || exit; \
	done
	@git diff --check

ci-java: ## Maven clean verify, toàn bộ Java tests và runner JAR; Spark mock chỉ bind loopback
	@SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost $(MAKE) package-java

ci-airflow: test-airflow ## Toàn bộ Python control-plane/DAG tests; không cài Airflow runtime

test-ci-workflow: ## YAML duplicate-key/safety/trigger/job regression; cần tests/ci/requirements.txt
	@python3 -m unittest discover -s tests/ci -p 'test_*.py'

test-security-audit: ## SEC-01 offline redaction/role/port/permission/stdin regression tests
	@python3 -m unittest discover -s tests -p 'test_security*.py'

audit-security: ## SEC-01 source/history/config scan; exit 2 = còn coverage, không có nghĩa Done
	@python3 scripts/audit-security.py --output "$(SECURITY_REPORT)"

audit-security-runtime: check-config-local ## SEC-01 security inspection + exact-key permission QA; không build/start/init
	@test "$(ENV_FILE)" = .env && test "$(COMPOSE_FILE)" = compose.yaml || \
		{ echo 'SEC-01 runtime audit only supports the project .env and compose.yaml.' >&2; exit 1; }
	@python3 scripts/audit-security.py --output "$(SECURITY_REPORT)" --runtime --permission-probes

test-docker-runtime: ## FND-01 lifecycle/preflight/freshness regression; Docker/HTTP mock, không start service
	@python3 -m unittest discover -s tests -p 'test_docker_runtime.py' -b

test-silver-integration: ## SLV-09 offline bundle/failure/revision/reconciliation/Gold tests (không gọi MinIO)
	@python3 -m unittest discover -s airflow/tests -p 'test_silver_integration_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=SilverMultiSourceIntegrationTest,SilverBundlePublisherTest,SilverParquetCrossProcessTest,SilverParquetWriterTest,SilverBronzeIntegrationJobTest,MinioSilverObjectStoreTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

smoke-silver-integration: check-config-local ## SLV-09 exact real Bronze -> Silver QA + rerun; không tự build/start
ifeq ($(strip $(SILVER_RUNNER_JAR)),)
	@$(RUNTIME_CHECK) --component silver
	@$(COMPOSE) --profile smoke run --rm --no-deps --pull never spark-silver-integration
else
	@case "$(SILVER_RUNNER_JAR)" in /*) ;; *) echo 'SILVER_RUNNER_JAR must be an absolute path'; exit 1 ;; esac
	@test -f "$(SILVER_RUNNER_JAR)"
	@$(RUNTIME_CHECK) --component silver --runner-jar "$(SILVER_RUNNER_JAR)"
	@$(COMPOSE) --profile smoke run --rm --no-deps --pull never \
		-v "$(SILVER_RUNNER_JAR):/opt/spark/jobs/japan-earthquake-etl-runner.jar:ro" spark-silver-integration
endif

test-recovery: ## ORC-05 retry/profile/lease/recovery/maintenance tests offline
	@python3 -m unittest discover -s airflow/tests -p 'test_recovery*.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=ResourcePilotJobTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-recovery: check-config-local ## ORC-05 exact Bronze recovery QA; không tự build/start
	@$(RUNTIME_CHECK) --component backfill
	@$(COMPOSE) run --rm --no-deps --pull never -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/recovery_qa.py

smoke-resource-pilot: check-config-local ## ORC-05 real parsers + standalone Spark shuffle; không tự build/start
	@$(RUNTIME_CHECK) --component resource
	@$(COMPOSE) --profile smoke run --rm --no-deps --pull never spark-resource-pilot

maintenance-preview: ## ORC-05 read-only QA staging diagnosis; không xóa/di chuyển/release lease
	@PYTHONPATH=airflow/dags python3 -m staging_maintenance --root "$(RUN_SUMMARY_ROOT)" \
		--dag-id "$(RUN_SUMMARY_DAG)" --run-id "$(RUN_SUMMARY_RUN_ID)"
.PHONY: test-observability observability-smoke smoke-observability observability-read observability-read-runtime

test-observability: ## ORC-04 counts/reasons/secret/failure/empty/rerun tests offline
	@python3 -m unittest discover -s airflow/tests -p 'test_run_observability.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_backfill_observability.py'
	@$(MAKE) test-orchestration

.PHONY: smoke-backfill-observability
smoke-backfill-observability: check-config-local ## ORC-04/03 Bronze reuse + summary; không tự build/start
	@$(RUNTIME_CHECK) --component backfill
	@$(COMPOSE) run --rm --no-deps --pull never -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/backfill_readback_qa.py --observability

observability-smoke: ## ORC-04 metadata-only mock success/failure/rerun; ghi staging local, không gọi nguồn
	@PYTHONPATH=airflow/dags python3 -m run_summary_cli --smoke --root "$(RUN_SUMMARY_ROOT)"

smoke-observability: check-config-local ## ORC-04 metadata smoke trong Airflow đang chạy; không restart/unpause/ingest
	@$(RUNTIME_CHECK) --component observability
	@$(COMPOSE) exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
		python -m run_summary_cli --smoke --root /opt/pipeline/staging/run-summary/qa

observability-read: ## Đọc summary local bằng DAG/run ID; đối chiếu journal state, không đọc payload
	@PYTHONPATH=airflow/dags python3 -m run_summary_cli --inspect --root "$(RUN_SUMMARY_ROOT)" \
		--dag-id "$(RUN_SUMMARY_DAG)" --run-id "$(RUN_SUMMARY_RUN_ID)"

observability-read-runtime: require-env ## Đọc summary của một DAG run trong staging volume Airflow
	@$(COMPOSE) exec -T -e PYTHONPATH=/opt/airflow/dags airflow-dag-processor \
		python -m run_summary_cli --inspect --root /opt/pipeline/staging/run-summary \
		--dag-id "$(RUN_SUMMARY_DAG)" --run-id "$(RUN_SUMMARY_RUN_ID)"

.PHONY: backfill-preview test-backfill smoke-backfill-readback smoke-backfill-pilot

backfill-preview: ## ORC-03 preview offline, không gọi nguồn/storage; BACKFILL_CONF=<json>
	@python3 scripts/preview-backfill.py --conf "$(BACKFILL_CONF)"

test-backfill: ## ORC-03 planner/retry/scope/lease + Java exact Bronze readback offline
	@python3 -m unittest discover -s airflow/tests -p 'test_backfill*.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=BronzeReuseVerifierTest,JmaYearIngestRunnerTest,UsgsIngestRunnerTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

smoke-backfill-readback: check-config-local ## ORC-03 exact reuse/rerun; không tải nguồn hoặc tự build/start
	@$(RUNTIME_CHECK) --component backfill
	@$(COMPOSE) run --rm --no-deps --pull never -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/backfill_readback_qa.py

smoke-backfill-pilot: check-config-local ## ORC-03 LIVE 2 USGS ngày + 1 segment JMA; không tự build/start
	@$(RUNTIME_CHECK) --component backfill
	@$(COMPOSE) run --rm --no-deps --pull never -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/backfill_readback_qa.py --source-pilot

test-source-schedule: ## ORC-02 profile/UTC/JMA change/lease/DAG tests offline
	@python3 -m unittest discover -s airflow/tests -p 'test_source*.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaHeadRetryTransportTest,JmaYearIngestRunnerTest,UsgsIngestRunnerTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-source-readiness: check-config-local ## ORC-02 scheduler + USGS/JMA/MinIO; không tự build/start
	@$(RUNTIME_CHECK) --component source
	@$(COMPOSE) --profile live run --rm --no-deps --pull never source-readiness-qa

help: ## Hiện toàn bộ lệnh (mặc định, không khởi động service)
	@printf 'Chạy từ thư mục gốc repository: make <target> [VARIABLE=value]\n\n'
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_-]+:.*## / { printf "  %-29s %s\n", $$1, $$2 }' Makefile
	@printf '\nBiến: ENV_FILE=.env, CHECK_ENV_FILE=.env.example, COMPOSE_FILE=compose.yaml\n'
	@printf '      SERVICE=<service>, TAIL=100, WAIT_TIMEOUT=300 (giây)\n'
	@printf '      DAT01_AIRFLOW_CONTAINER=<container> (readback không cần .env)\n'
	@printf '\nVí dụ: make test; make up; make logs-follow SERVICE=airflow-scheduler\n'

env-init: ## Tạo ENV_FILE từ .env.example nếu chưa có; không ghi đè
	@if [ -e "$(ENV_FILE)" ] || [ -L "$(ENV_FILE)" ]; then \
		printf 'Giữ nguyên cấu hình đã có: %s\n' "$(ENV_FILE)"; \
	else \
		umask 077; cp -n .env.example "$(ENV_FILE)" && \
		printf 'Đã tạo %s. Thay toàn bộ change-me-* trước khi chạy runtime.\n' "$(ENV_FILE)"; \
	fi

require-env:
	@test -f "$(ENV_FILE)" || { \
		printf 'Thiếu %s. Chạy make env-init hoặc đặt ENV_FILE=/path/to/local.env.\n' "$(ENV_FILE)" >&2; \
		exit 1; \
	}

check-config: ## Kiểm tra cấu hình mẫu, cấu hình local nếu có và secret hygiene
	@$(RUNTIME_ENV) ./scripts/check-config.sh

check-config-local: ## Bắt buộc ENV_FILE hợp lệ, không còn placeholder secret
	@$(RUNTIME_ENV) ./scripts/check-config.sh --require-local

config: check-config-local ## Validate toàn bộ Compose profile; không in secret
	@$(COMPOSE) --profile smoke --profile live --profile validation config --quiet

test: test-contracts test-java test-airflow ## Toàn bộ contract và unit test; không cần Docker daemon/nguồn thật

test-contracts: $(CONTRACT_CHECKS) check-week-5-plan test-week-5-plan check-java-build-inputs ## Kiểm tra docs, contract, fixture và catalog mẫu; không cần mạng

check-java-build-inputs: ## Kiểm tra đầu vào Maven trong Docker và regression test; không cần Docker/JDK/env
	@./scripts/check-java-build-inputs.sh
	@python3 -m unittest discover -s tests -p 'test_java_build_inputs.py'

test-java: ## Unit test Spark/USGS Java bằng Maven Wrapper (HTTP/storage mock)
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test

test-jma-bronze: ## Kiểm thử riêng JMA-03 ZIP validator, Bronze writer và handoff bằng fixture/mock
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaArchiveValidatorTest,JmaBronzeWriterTest -Dsurefire.failIfNoSpecifiedTests=false test

test-jma-parser: ## Kiểm thử riêng SLV-03 JMA fixed-width, code mapping và handoff Silver offline
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaFixedWidthParserTest,JmaParserIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test

test-jma-backfill: ## Kiểm thử JMA-04 downloader/runner/planner/DAG bằng fixture/mock, không tải nguồn
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaHeadRetryTransportTest,JmaArchiveDownloaderTest,JmaYearIngestRunnerTest -Dsurefire.failIfNoSpecifiedTests=false test
	@python3 -m unittest discover -s airflow/tests -p 'test_jma_*.py'

test-jma-qa: ## JMA-05 offline error/readback/revision/resume QA, không gọi nguồn hoặc MinIO thật
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaHeadRetryTransportTest,JmaArchiveDownloaderTest,JmaArchiveValidatorTest,JmaBronzeWriterTest,JmaYearIngestRunnerTest,JmaBronzeQaVerifierTest \
		-Dsurefire.failIfNoSpecifiedTests=false test
	@python3 -m unittest discover -s airflow/tests -p 'test_jma_*.py'

jma-preview: ## Preview offline các năm chỉ định; ví dụ JMA_YEARS=1997,2023
	@python3 scripts/preview-jma-backfill.py --years "$(JMA_YEARS)"

test-airflow: ## Unit test DAG/runner Airflow bằng unittest; không cần Airflow runtime
	@python3 -m unittest discover -s airflow/tests -p 'test_*.py'

test-orchestration: ## ORC-01 phase/gate/DAG structure bằng fixture/mock, không cần mạng hoặc Airflow
	@python3 -m unittest discover -s airflow/tests -p 'test_etl_*.py'

etl-preview: ## Preview ORC-01 run context và exact scope từ fixture; không ghi hoặc gọi service
	@python3 scripts/preview-etl-pipeline.py

etl-mock: ## Chạy sáu phase ORC-01 offline; chỉ MockComplete, không Published
	@python3 scripts/preview-etl-pipeline.py --execute-mock

package-java: ## Maven clean verify + fingerprint runner/Iceberg JAR; không gọi Docker
	@python3 scripts/docker-runtime.py package

check: test-contracts check-config check-foundation check-usgs-live ## Full static readiness, gồm Compose và Maven verify; không start service

check-mvp-baseline: ## Kiểm tra baseline PLN-01, scope/KPI/DoD và task index
check-repository-layout: ## Kiểm tra scaffold, module và mount source
check-source-coverage: ## Kiểm tra range/ROI/timezone/overlap USGS và JMA
check-bronze-contract: ## Kiểm tra object path, manifest, checksum và retry contract
check-data-model-contract: ## Kiểm tra logical model Silver/Gold/ML và publish gate
check-shared-fixtures: ## Kiểm tra fixture synthetic USGS/JMA, ZIP và checksum
check-real-sample-catalog: ## Kiểm tra metadata DAT-01 offline; không đọc MinIO
check-week-3-plan: ## Kiểm tra kế hoạch chia việc và data-readiness gate
check-week-5-plan: ## Kiểm tra owner/tuần/effort và real-adapter gates tuần 5
	@sh scripts/check-week-5-plan.sh

.PHONY: check-week-5-plan test-week-5-plan

test-week-5-plan: ## Regression kế hoạch tuần 5 bằng metadata tạm, không gọi runtime
	@python3 -m unittest discover -s tests -p 'test_week5_plan.py'
check-jma-inventory: ## Kiểm tra inventory 40 năm/41 archive JMA offline
check-task-status: ## Đối chiếu trạng thái metadata, Theo dõi và danh mục của mọi task

$(CONTRACT_CHECKS):
	@./scripts/$@.sh

check-compose: ## Validate Compose foundation bằng CHECK_ENV_FILE; không start service
check-minio: ## Kiểm tra static contract MinIO
check-airflow: ## Kiểm tra static contract Airflow và unit test DAG
check-spark: ## Kiểm tra static contract Spark và Maven clean verify
check-query: ## Kiểm tra static contract Iceberg REST/Trino
check-foundation: ## Toàn bộ static foundation check bằng CHECK_ENV_FILE
check-usgs-live: ## Kiểm tra USG-06 runner/profile cùng unit test mock; không gọi USGS

$(STATIC_CHECKS):
	@$(CHECK_ENV) ./scripts/$@.sh

check-foundation:
	@$(RUNTIME_ENV) CHECK_ENV_FILE="$(CHECK_ENV_FILE)" ./scripts/check-foundation.sh

check-jma-inventory-live: ## Đối soát HTTP header 41 archive JMA; cần mạng, không tải ZIP
	@./scripts/check-jma-inventory.sh --live

build-shared-fixtures: ## Chủ động tái tạo fixture synthetic/checksum (có sửa file tracked)
	@./scripts/build-shared-fixtures.sh

docker-build: check-config-local check-java-build-inputs ## Build bằng Java17 trong Docker, export JAR + chuẩn bị images; không start/init service
	@$(RUNTIME_ENV) python3 scripts/docker-runtime.py build

docker-up: check-config-local ## Start foundation, reuse init đúng config đã exit 0; không build/pull
	@$(RUNTIME_ENV) WAIT_TIMEOUT="$(WAIT_TIMEOUT)" python3 scripts/docker-runtime.py up

docker-check: check-config-local ## Read-only foundation readiness + image freshness; timeout mặc định 20s
	@$(RUNTIME_CHECK)

build: docker-build ## Alias docker-build, chỉ chuẩn bị artifact/images

up: docker-up ## Alias docker-up, không còn tự build

start: up ## Alias của up

up-minio: check-config-local ## Start MinIO + init khi cần; không build/pull
	@$(RUNTIME_ENV) WAIT_TIMEOUT="$(WAIT_TIMEOUT)" python3 scripts/docker-runtime.py up --component minio

up-airflow: check-config-local ## Start Airflow/MinIO/PostgreSQL + init khi cần; không build/pull
	@$(RUNTIME_ENV) WAIT_TIMEOUT="$(WAIT_TIMEOUT)" python3 scripts/docker-runtime.py up --component airflow

up-spark: check-config-local ## Start Spark + staging init khi cần; không build/pull
	@$(RUNTIME_ENV) WAIT_TIMEOUT="$(WAIT_TIMEOUT)" python3 scripts/docker-runtime.py up --component spark

up-query: check-config-local ## Start MinIO/Catalog/Trino + init khi cần; không build/pull
	@$(RUNTIME_ENV) WAIT_TIMEOUT="$(WAIT_TIMEOUT)" python3 scripts/docker-runtime.py up --component query

status: require-env ## Xem toàn bộ container, gồm init đã thoát; SERVICE để lọc
	@$(COMPOSE) ps -a $(SERVICE)

ps: status ## Alias của status

logs: require-env ## Xem TAIL dòng log gần nhất; SERVICE để lọc
	@$(COMPOSE) logs --tail "$(TAIL)" $(SERVICE)

logs-follow: require-env ## Theo dõi log realtime; Ctrl+C chỉ thoát xem log
	@$(COMPOSE) logs --follow --tail "$(TAIL)" $(SERVICE)

restart: check-config-local ## Restart container đã có; SERVICE để chọn service, không rebuild
	@$(COMPOSE) restart $(SERVICE)

stop: require-env ## Dừng container, giữ nguyên container/network/named volume
	@$(COMPOSE) stop $(SERVICE)

down: require-env ## Gỡ container/network của stack; giữ nguyên named volume dữ liệu
	@$(COMPOSE) down

smoke: smoke-foundation ## Alias full foundation runtime smoke; không gồm gọi USGS thật

smoke-foundation: ## Test stack đã chạy; wrapper kiểm tra config, không build/start/init hoặc gọi USGS
	@$(RUNTIME_ENV) ./scripts/smoke-foundation.sh

smoke-minio: ## Test ghi/đọc object trên MinIO đã sẵn sàng
smoke-airflow: ## Trigger DAG afl_01_smoke trên Airflow đã sẵn sàng
smoke-spark: ## Submit HelloWorldJob trên Spark đã sẵn sàng
smoke-query: ## Tạo/ghi/đọc/xóa đúng table smoke trên Trino đã sẵn sàng
smoke-usgs-live: ## Gọi USGS thật cho 2023-01-01..04 UTC, ghi Bronze và kiểm tra rerun
smoke-jma-live: ## JMA-05 live DAG 1997/2000/2023, readback DAT-01 và rerun; cần Docker/.env/Internet

# Each shell wrapper validates local config before preflight. Do not scan the
# entire repository twice; direct script execution must retain the same gate.
$(COMPONENT_SMOKES):
	@$(RUNTIME_ENV) ./scripts/$@.sh

verify-samples: verify-real-samples ## Alias đọc lại hai sample DAT-01 từ MinIO

verify-real-samples: ## Readback checksum/count USGS/JMA đã có; không tải nguồn hoặc tự start
	@$(RUNTIME_ENV) DAT01_AIRFLOW_CONTAINER="$(DAT01_AIRFLOW_CONTAINER)" ./scripts/verify-real-samples.sh
