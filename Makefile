# Command wrappers only: scripts remain the source of validation/runtime logic.
# Compatible with GNU Make 3.81 (the default on macOS).
SHELL := /bin/sh
.DEFAULT_GOAL := help
.NOTPARALLEL:

ENV_FILE ?= .env
CHECK_ENV_FILE ?= .env.example
COMPOSE_FILE ?= compose.yaml
WAIT_TIMEOUT ?= 300
SERVICE ?=
TAIL ?= 100
DAT01_AIRFLOW_CONTAINER ?=
JMA_YEARS ?=
BACKFILL_CONF ?= airflow/dags/fixtures/orc_03_ingest_preview.json
SILVER_RUNNER_JAR ?=
RUN_SUMMARY_ROOT ?= staging/run-summary
RUN_SUMMARY_DAG ?= orc_01_etl_pipeline
RUN_SUMMARY_RUN_ID ?=

COMPOSE = docker compose --env-file "$(ENV_FILE)" -f "$(COMPOSE_FILE)"
RUNTIME_ENV = ENV_FILE="$(ENV_FILE)" COMPOSE_FILE="$(COMPOSE_FILE)"
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
.PHONY: test-recovery smoke-recovery smoke-resource-pilot maintenance-preview
.PHONY: test-silver-integration smoke-silver-integration
.PHONY: test-ml-audit test-ml-window

test-ml-audit: ## MLD-02 Gold input audit, MAXC completeness estimator, sensitivity & reconciliation tests
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=MagnitudeCompletenessEstimatorTest,GoldInputAuditEngineTest,GoldInputAuditIntegrationTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

test-ml-window: ## MLD-03 Mainshock candidate selection, versioned window resolver & range join tests
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=WindowModelResolverTest,MainshockWindowEngineTest,MainshockWindowIntegrationTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

test-silver-integration: ## SLV-09 offline bundle/failure/revision/reconciliation/Gold tests (không gọi MinIO)
	@python3 -m unittest discover -s airflow/tests -p 'test_silver_integration_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=SilverMultiSourceIntegrationTest,SilverBundlePublisherTest,SilverParquetCrossProcessTest,SilverParquetWriterTest,SilverBronzeIntegrationJobTest,MinioSilverObjectStoreTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

smoke-silver-integration: check-config-local check-java-build-inputs ## SLV-09 exact real Bronze -> immutable Silver -> Gold transform + rerun; ghi bundle QA riêng
ifeq ($(strip $(SILVER_RUNNER_JAR)),)
	@$(COMPOSE) build spark-master
	@$(COMPOSE) --profile smoke run --rm --no-deps spark-silver-integration
else
	@case "$(SILVER_RUNNER_JAR)" in /*) ;; *) echo 'SILVER_RUNNER_JAR must be an absolute path'; exit 1 ;; esac
	@test -f "$(SILVER_RUNNER_JAR)"
	@$(COMPOSE) --profile smoke run --rm --no-deps \
		-v "$(SILVER_RUNNER_JAR):/opt/spark/jobs/japan-earthquake-etl-runner.jar:ro" spark-silver-integration
endif

test-recovery: ## ORC-05 retry/profile/lease/recovery/maintenance tests offline
	@python3 -m unittest discover -s airflow/tests -p 'test_recovery*.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=ResourcePilotJobTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-recovery: check-config-local check-java-build-inputs ## ORC-05 failure -> real exact Bronze readback; không tải nguồn/ghi lake/restart
	@$(COMPOSE) build airflow-api-server
	@$(COMPOSE) run --rm --no-deps -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/recovery_qa.py

smoke-resource-pilot: check-config-local check-java-build-inputs ## ORC-05 real parsers + bounded standalone Spark shuffle; cần master/worker/MinIO đang healthy
	@$(COMPOSE) build spark-master
	@$(COMPOSE) --profile smoke run --rm --no-deps spark-resource-pilot

maintenance-preview: ## ORC-05 read-only QA staging diagnosis; không xóa/di chuyển/release lease
	@PYTHONPATH=airflow/dags python3 -m staging_maintenance --root "$(RUN_SUMMARY_ROOT)" \
		--dag-id "$(RUN_SUMMARY_DAG)" --run-id "$(RUN_SUMMARY_RUN_ID)"
.PHONY: test-observability observability-smoke smoke-observability observability-read observability-read-runtime

test-observability: ## ORC-04 counts/reasons/secret/failure/empty/rerun tests offline
	@python3 -m unittest discover -s airflow/tests -p 'test_run_observability.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_backfill_observability.py'
	@$(MAKE) test-orchestration

.PHONY: smoke-backfill-observability
smoke-backfill-observability: check-config-local check-java-build-inputs ## ORC-04/03 live read-only Bronze reuse/rerun + summary; không tải nguồn/ghi lake
	@$(COMPOSE) build airflow-api-server
	@$(COMPOSE) run --rm --no-deps -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/backfill_readback_qa.py --observability

observability-smoke: ## ORC-04 metadata-only mock success/failure/rerun; ghi staging local, không gọi nguồn
	@PYTHONPATH=airflow/dags python3 -m run_summary_cli --smoke --root "$(RUN_SUMMARY_ROOT)"

smoke-observability: require-env ## ORC-04 metadata smoke trong Airflow đang chạy; không restart/unpause/ingest
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

smoke-backfill-readback: check-config-local check-java-build-inputs ## ORC-03 exact reuse/rerun; chỉ đọc Bronze đã có, không tải nguồn
	@$(COMPOSE) build airflow-api-server
	@$(COMPOSE) run --rm --no-deps -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/backfill_readback_qa.py

smoke-backfill-pilot: check-config-local check-java-build-inputs ## ORC-03 LIVE: nạp 2 USGS ngày + 1 segment JMA, rerun và readback outside pins
	@$(COMPOSE) build airflow-api-server
	@$(COMPOSE) run --rm --no-deps -e PYTHONPATH=/opt/airflow/dags \
		airflow-api-server python /opt/airflow/dags/backfill_readback_qa.py --source-pilot

test-source-schedule: ## ORC-02 profile/UTC/JMA change/lease/DAG tests offline
	@python3 -m unittest discover -s airflow/tests -p 'test_source*.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaYearIngestRunnerTest,UsgsIngestRunnerTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-source-readiness: check-config-local check-java-build-inputs ## ORC-02 real scheduler + USGS/JMA/MinIO; giữ data/volume và pause state
	@$(COMPOSE) build airflow-api-server
	@$(COMPOSE) up -d --no-build --wait --wait-timeout $(WAIT_TIMEOUT) airflow-api-server airflow-scheduler airflow-dag-processor
	@$(COMPOSE) --profile live run --rm --no-deps source-readiness-qa

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

test-contracts: $(CONTRACT_CHECKS) check-java-build-inputs ## Kiểm tra docs, contract, fixture và catalog mẫu; không cần mạng

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
		-Dtest=JmaArchiveDownloaderTest,JmaYearIngestRunnerTest -Dsurefire.failIfNoSpecifiedTests=false test
	@python3 -m unittest discover -s airflow/tests -p 'test_jma_*.py'

test-jma-qa: ## JMA-05 offline error/readback/revision/resume QA, không gọi nguồn hoặc MinIO thật
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=JmaArchiveDownloaderTest,JmaArchiveValidatorTest,JmaBronzeWriterTest,JmaYearIngestRunnerTest,JmaBronzeQaVerifierTest \
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

package-java: ## Clean, test, verify và đóng gói Spark JAR
	@./mvnw --batch-mode --no-transfer-progress clean verify

check: test-contracts check-config check-foundation check-usgs-live ## Full static readiness, gồm Compose và Maven verify; không start service

check-mvp-baseline: ## Kiểm tra baseline PLN-01, scope/KPI/DoD và task index
check-repository-layout: ## Kiểm tra scaffold, module và mount source
check-source-coverage: ## Kiểm tra range/ROI/timezone/overlap USGS và JMA
check-bronze-contract: ## Kiểm tra object path, manifest, checksum và retry contract
check-data-model-contract: ## Kiểm tra logical model Silver/Gold/ML và publish gate
check-shared-fixtures: ## Kiểm tra fixture synthetic USGS/JMA, ZIP và checksum
check-real-sample-catalog: ## Kiểm tra metadata DAT-01 offline; không đọc MinIO
check-week-3-plan: ## Kiểm tra kế hoạch chia việc và data-readiness gate
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

build: check-config-local check-java-build-inputs ## Build ba image local: MinIO, Airflow Java17 và Spark
	@$(COMPOSE) build minio airflow-api-server spark-master

up: build ## Build và khởi động toàn bộ 9 foundation service; chờ healthy
	@$(COMPOSE) up -d --no-build --wait --wait-timeout "$(WAIT_TIMEOUT)" $(FOUNDATION_SERVICES)

start: up ## Alias của up

up-minio: check-config-local ## Build/start MinIO, chờ healthy và bootstrap bucket bằng init
	@$(COMPOSE) build minio
	@$(COMPOSE) up -d --no-build --wait --wait-timeout "$(WAIT_TIMEOUT)" minio
	@$(COMPOSE) run --rm --no-deps minio-init

up-airflow: check-config-local check-java-build-inputs ## Build/start Airflow và dependency MinIO/PostgreSQL/init
	@$(COMPOSE) build minio airflow-api-server
	@$(COMPOSE) up -d --no-build --wait --wait-timeout "$(WAIT_TIMEOUT)" \
		airflow-api-server airflow-scheduler airflow-dag-processor

up-spark: check-config-local check-java-build-inputs ## Build image chung trước khi start Spark master/worker
	@$(COMPOSE) build spark-master
	@$(COMPOSE) up -d --no-build --wait --wait-timeout "$(WAIT_TIMEOUT)" spark-master spark-worker

up-query: check-config-local ## Start Iceberg REST/Trino và dependency MinIO/init
	@$(COMPOSE) build minio
	@$(COMPOSE) up -d --no-build --wait --wait-timeout "$(WAIT_TIMEOUT)" iceberg-rest trino

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

smoke-foundation: build ## Build đủ image, start stack và test MinIO/Airflow/Spark/Trino thật
	@$(RUNTIME_ENV) FOUNDATION_WAIT_TIMEOUT_SECONDS="$(WAIT_TIMEOUT)" ./scripts/smoke-foundation.sh

smoke-minio: ## Bootstrap/start MinIO và test ghi/đọc object thật
smoke-airflow: ## Start Airflow và trigger DAG afl_01_smoke thật
smoke-spark: ## Build/start Spark và submit HelloWorldJob thật
smoke-query: ## Start query stack và tạo/ghi/đọc/xóa đúng table smoke
smoke-usgs-live: ## Gọi USGS thật cho 2023-01-01..04 UTC, ghi Bronze và kiểm tra rerun
smoke-jma-live: ## JMA-05 live DAG 1997/2000/2023, readback DAT-01 và rerun; cần Docker/.env/Internet

$(COMPONENT_SMOKES): check-config-local
	@$(RUNTIME_ENV) ./scripts/$@.sh

verify-samples: verify-real-samples ## Alias đọc lại hai sample DAT-01 từ MinIO

verify-real-samples: ## Readback checksum/count USGS/JMA đã có; không tải nguồn hoặc tự start
	@$(RUNTIME_ENV) DAT01_AIRFLOW_CONTAINER="$(DAT01_AIRFLOW_CONTAINER)" ./scripts/verify-real-samples.sh
