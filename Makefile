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
SECURITY_DEPLOYMENT_ROOT ?=
SECURITY_HANDOFF_MANIFEST ?=
SECURITY_AUDIT_FLAGS = $(if $(SECURITY_DEPLOYMENT_ROOT),--deployment-root "$(SECURITY_DEPLOYMENT_ROOT)") $(if $(SECURITY_HANDOFF_MANIFEST),--handoff-manifest "$(SECURITY_HANDOFF_MANIFEST)")
QA_03_MATRIX ?= airflow/dags/fixtures/qa_03_jma_historical_matrix.json
QA_03_JSON ?= staging/qa-03/qa-03.json
QA_03_MARKDOWN ?= staging/qa-03/qa-03.md
QA_03_RUNTIME_REQUEST ?=
QA_03_RUNTIME_COMMAND ?=
QA_02_MATRIX ?= airflow/dags/fixtures/qa_02_rerun_revision_matrix.json
QA_02_JSON ?= staging/qa-02/qa-02.json
QA_02_MARKDOWN ?= staging/qa-02/qa-02.md
QA_02_RUNTIME_REQUEST ?=
QA_02_RUNTIME_COMMAND ?=
MLI_IMPORT_REQUEST ?=
# A per-run CLI input must not change Compose's shared Airflow/init config hash.
# Pass it explicitly to the one-off service or DAG QA argv below instead.
unexport MLI_IMPORT_REQUEST
MLI_IMPORT_RUNNER_COMMAND ?= /opt/pipeline/bin/mli-import-runner
QA_04_REQUEST ?=
QA_04_RUNTIME_COMMAND ?=
QA_04_STAGING_ROOT ?= /opt/pipeline/staging/qa-04
QA_01_RUN_ID ?=
CURRENT_MONTH_REQUEST ?=
CURRENT_MONTH_ROOT ?= staging/current-month-component
unexport CURRENT_MONTH_REQUEST CURRENT_MONTH_ROOT
PIPELINE_QA_TASK ?= QA-02
unexport PIPELINE_QA_TASK

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
.PHONY: test-qa-03 smoke-qa-03
.PHONY: test-qa-02 smoke-qa-02
.PHONY: test-qa-01 smoke-qa-01
.PHONY: prepare-current-month test-current-month test-current-month-gold current-month-local current-month-gold

prepare-current-month: ## QA-01 component: explicit host JAR/classpath preparation, không Docker hoặc source fetch
	@python3 scripts/local-ml.py prepare

test-current-month: ## QA-01 Source capture/Silver fixture regressions, không gọi nguồn thật/MinIO
	@python3 -m unittest discover -s tests -p 'test_current_month_cli.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=CurrentMonthCaptureTest,CurrentMonthSilverTest -Dsurefire.failIfNoSpecifiedTests=false

test-current-month-gold: ## QA-01 operational Gold: local Iceberg + SQL protocol fixtures, không Docker/start/source HTTP
	@python3 -m unittest discover -s tests -p 'test_current_month_cli.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=CurrentMonthGoldTest,OperationalGoldPolicyTest,GoldPublicationTest,GoldIcebergWriterTest,DatasetManifestTest -Dsurefire.failIfNoSpecifiedTests=false

current-month-gold: ## Opt-in MinIO Silver -> operational Gold/Trino; prebuilt JAR + held lease, không start/pull/build
	@test -n "$(CURRENT_MONTH_REQUEST)" || { echo 'Set CURRENT_MONTH_REQUEST to an exact Gold request JSON'; exit 2; }
	@python3 scripts/current-month.py --gold --request "$(CURRENT_MONTH_REQUEST)"

current-month-local: ## Opt-in live USGS -> isolated local Bronze/Silver; không Docker, Gold hoặc scheduler mutation
	@test -n "$(CURRENT_MONTH_REQUEST)" || { echo 'Set CURRENT_MONTH_REQUEST to an exact request JSON'; exit 2; }
	@python3 scripts/current-month.py --request "$(CURRENT_MONTH_REQUEST)" --root "$(CURRENT_MONTH_ROOT)"
.PHONY: check-docs test-docs
.PHONY: test-mli-import smoke-mli-import smoke-mli-import-acceptance smoke-mli-import-dag up-ml
.PHONY: test-qa-04 smoke-qa-04
.PHONY: test-mli-build check-mli-build-runtime
check-mli-build-runtime: ## Read-only runner pin/module preflight; không build/pull/start/init hoặc lease
	@$(COMPOSE) exec -T airflow-scheduler python3 -c 'import os; from ml_dataset_real import prepared; prepared(dict(os.environ)); print("MLI-02 real runtime prepared; no build/pull/start/init performed.")'

test-mli-build: ## MLI-02 graph/control-plane/real Java adapter guards; không Docker
	@python3 -m unittest discover -s airflow/tests -p 'test_mli_02_build_dataset.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_ml_dataset_real.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=DatasetBuildJobTest,DatasetManifestTest,FeatureExportCoverageTest,FeatureExportBundleTest -Dsurefire.failIfNoSpecifiedTests=false

.PHONY: test-qa-acceptance smoke-qa-acceptance prepare-qa-history test-ml-export smoke-ml-export export-ml-dataset verify-ml-export

test-ml-export: ## MLD-05 validation/export/retry/snapshot/coverage tests; JDK17, không Docker
	@python3 -m unittest discover -s airflow/tests -p 'test_feature_export_runtime.py'
	@python3 -m unittest discover -s tests -p 'test_ml_export_cli.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=FeatureExportCoverageTest,FeatureExportBundleTest -Dsurefire.failIfNoSpecifiedTests=false

smoke-ml-export: check-config-local ## MLD-05 synthetic full-period QA trên storage thật; không build/start/init
	@$(RUNTIME_CHECK) --component ml --client mld05-feature-export
	@$(COMPOSE) --profile ml run --rm --no-deps --pull never --entrypoint python3 mld05-feature-export /opt/airflow/dags/feature_export_runtime.py --qa

export-ml-dataset: check-config-local ## MLD-05 export exact official dataset; dừng nếu coverage/audit chưa đạt
	@test -n "$(MLD05_DATASET_ID)" && test -n "$(MLD05_NAMESPACE)" || { echo 'Set MLD05_DATASET_ID and MLD05_NAMESPACE'; exit 2; }
	@$(RUNTIME_CHECK) --component ml --client mld05-feature-export
	@$(COMPOSE) --profile ml run --rm --no-deps --pull never -e MLD05_DATASET_ID="$(MLD05_DATASET_ID)" -e MLD05_NAMESPACE="$(MLD05_NAMESPACE)" mld05-feature-export

verify-ml-export: ## MLD-05 verify bundle local/Colab bằng pins và EXP-01 loader; không Docker
	@test -n "$(MLD05_BUNDLE_PATH)" && test -n "$(MLD05_MANIFEST_SHA256)" && test -n "$(MLD05_CHECKSUMS_SHA256)" || { echo 'Set MLD05_BUNDLE_PATH, MLD05_MANIFEST_SHA256, MLD05_CHECKSUMS_SHA256'; exit 2; }
	@$(PILOT_PYTHON) scripts/verify-ml-export.py "$(MLD05_BUNDLE_PATH)" --manifest-sha256 "$(MLD05_MANIFEST_SHA256)" --checksums-sha256 "$(MLD05_CHECKSUMS_SHA256)"

prepare-qa-history: ## QA-03: tải/reuse 4 archive JMA và 1 USGS capture có pin; không Docker/ETL
	@python3 scripts/prepare-qa-history.py

test-qa-acceptance: ## QA-02/03/04 fixed-runner safety/report/lease tests; offline, không Docker
	@python3 -m unittest discover -s airflow/tests -p 'test_pipeline_qa_acceptance.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_qa_04_storage_acceptance.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=PipelineQaSafetyTest,Qa04MlRecoveryAcceptanceTest -Dsurefire.failIfNoSpecifiedTests=false

smoke-qa-acceptance: check-config-local ## QA-02/03/04 storage acceptance; không build/start/init
	@$(RUNTIME_CHECK) --component ml --client pipeline-qa-acceptance
	@$(COMPOSE) --profile ml run --rm --no-deps --pull never -e PIPELINE_QA_TASK="$(PIPELINE_QA_TASK)" pipeline-qa-acceptance

.PHONY: test-ml-window smoke-ml-window resume-ml-window
.PHONY: prepare-ml-local stage-ml-windows test-ml-feature ml-features-local ml-pilot-local pilot-env test-pilot inspect-pilot jupyter-pilot
.PHONY: pack-pilot test-exp01 inspect-exp01 jupyter-exp01
.PHONY: pack-team-pilot test-team-pilot profile-jma-research profile-jma-replay test-research-profile plan-research-profiles capture-research-profiles profile-research-archives
RESEARCH_PROFILE_ROOT ?= staging/dat03-research-profile-20261024
RESEARCH_PROFILE_YEARS ?=
RESEARCH_PROFILE_ARGS = --root "$(RESEARCH_PROFILE_ROOT)" $(if $(RESEARCH_PROFILE_YEARS),--years "$(RESEARCH_PROFILE_YEARS)",)
TEAM_PILOT_ZIP ?= staging/shared-datasets/jma-development-pilot-team.zip
JMA_PROFILE_ARCHIVE ?=
JMA_PROFILE_MEMBER ?=
JMA_PROFILE_SHA256 ?=
JMA_PROFILE_OUTPUT ?=
JMA_PROFILE_YEAR ?=
JMA_PROFILE_MONTHS ?=

pack-team-pilot: ## DAT-03 kit offline có pilot/loader/README/pins; không Docker hoặc upload
	@test -n "$(PILOT_BUNDLE)" || { echo 'PILOT_BUNDLE is required'; exit 1; }
	@$(PILOT_PYTHON) scripts/pack-team-pilot.py "$(PILOT_BUNDLE)" "$(TEAM_PILOT_ZIP)"

test-team-pilot: ## DAT-03 synthetic package/pin/receipt checks; không Docker, cần pyarrow
	@$(PILOT_PYTHON) -m unittest discover -s tests -p 'test_team_pilot.py'

profile-jma-research: ## DAT-03 profile ZIP local bằng Java parser; cần JAR đã build, không source/network/lake writes
	@test -n "$(JMA_PROFILE_ARCHIVE)" -a -n "$(JMA_PROFILE_MEMBER)" -a -n "$(JMA_PROFILE_SHA256)" -a -n "$(JMA_PROFILE_OUTPUT)" || { echo 'JMA_PROFILE_ARCHIVE/MEMBER/SHA256/OUTPUT are required'; exit 1; }
	@java -Xmx768m -cp spark/target/japan-earthquake-etl-runner.jar ie212.earthquake.spark.jma.JmaResearchProfileJob "$(JMA_PROFILE_ARCHIVE)" "$(JMA_PROFILE_MEMBER)" "$(JMA_PROFILE_SHA256)" "$(JMA_PROFILE_OUTPUT)"

profile-jma-replay: ## DAT-03 đo reader theo tháng từ ZIP local; không BronzeReady/coverage/Gold writes
	@test -n "$(JMA_PROFILE_ARCHIVE)" -a -n "$(JMA_PROFILE_MEMBER)" -a -n "$(JMA_PROFILE_SHA256)" -a -n "$(JMA_PROFILE_YEAR)" -a -n "$(JMA_PROFILE_MONTHS)" -a -n "$(JMA_PROFILE_OUTPUT)" || { echo 'JMA_PROFILE_ARCHIVE/MEMBER/SHA256/YEAR/MONTHS/OUTPUT are required'; exit 1; }
	@java -Xmx768m -cp spark/target/japan-earthquake-etl-runner.jar ie212.earthquake.spark.jma.JmaResearchReplayProfileJob "$(JMA_PROFILE_ARCHIVE)" "$(JMA_PROFILE_MEMBER)" "$(JMA_PROFILE_SHA256)" "$(JMA_PROFILE_YEAR)" "$(JMA_PROFILE_MONTHS)" "$(JMA_PROFILE_OUTPUT)"

test-research-profile: ## DAT-03 kiểm capture/profile control plane bằng mock, không Docker/mạng
	@python3 -m unittest discover -s tests -p 'test_research_profiles.py'

plan-research-profiles: ## DAT-03 xem 25 archive/bytes; offline, không tạo file
	@python3 scripts/profile-research-archives.py plan $(RESEARCH_PROFILE_ARGS)

capture-research-profiles: ## DAT-03 EXPLICIT network capture 2000–2024 vào staging, không Bronze/deployment
	@python3 scripts/profile-research-archives.py capture $(RESEARCH_PROFILE_ARGS)

profile-research-archives: ## DAT-03 profile tuần tự ZIP đã pin bằng JAR có sẵn, không tự build/download
	@python3 scripts/profile-research-archives.py profile $(RESEARCH_PROFILE_ARGS)

.PHONY: test-research-e2e capture-research-e2e prepare-research-e2e run-research-e2e verify-research-e2e
.PHONY: test-research-recovery prepare-research-recovery check-research-recovery
.PHONY: test-research-continuation prepare-research-continuation run-research-continuation verify-research-continuation
.PHONY: test-research-collection
RESEARCH_DEPLOYMENT_ROOT ?=
RESEARCH_E2E_FOLDER ?=
RESEARCH_RECOVERY_FOLDER ?=
RESEARCH_RETAINED_ROOT ?=
RESEARCH_PREFLIGHT_ROOT ?=
RESEARCH_CONTINUATION_FOLDER ?=
RESEARCH_CHECKPOINT_ROOT ?=
.PHONY: test-research-rollout plan-research-rollout probe-research-capacity test-research-usgs plan-research-usgs capture-research-usgs verify-research-usgs
.PHONY: test-research-trino-profile prepare-research-trino-profile apply-research-trino-profile verify-research-trino-profile
.PHONY: test-research-light-session measure-research-light-session
RESEARCH_ROLLOUT_PLAN ?=
RESEARCH_CAPACITY_RUNTIME ?=
RESEARCH_CAPACITY_STRATEGY ?= full
RESEARCH_CAPACITY_QUERY_SECONDS ?= 90
RESEARCH_USGS_ROOT ?= staging/dat03-usgs-2023
RESEARCH_USGS_REUSE_DECEMBER ?=
RESEARCH_JAVA ?= java
RESEARCH_USGS_JAR ?= spark/target/japan-earthquake-etl-runner.jar
RESEARCH_TRINO_PROFILE_FOLDER ?=
test-research-rollout: ## DAT-03 offline rollout/capacity guards; không Docker/mạng
	@python3 -m unittest discover -s tests -p 'test_research_rollout.py'

plan-research-rollout: ## DAT-03 verify25 original captures/profiles, plan288 months; không lake writes
	@python3 scripts/research-rollout.py plan --captures "$(RESEARCH_PROFILE_ROOT)" $(if $(RESEARCH_ROLLOUT_PLAN),--output "$(RESEARCH_ROLLOUT_PLAN)")

probe-research-capacity: ## DAT-03 bounded read-only Trino stress probe, không là coverage/research acceptance
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_CAPACITY_RUNTIME)" || { echo 'RESEARCH_DEPLOYMENT_ROOT/CAPACITY_RUNTIME are required'; exit 1; }
	@python3 scripts/research-rollout.py probe --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --runtime-root "$(RESEARCH_CAPACITY_RUNTIME)" --strategy "$(RESEARCH_CAPACITY_STRATEGY)" --query-seconds "$(RESEARCH_CAPACITY_QUERY_SECONDS)"

test-research-usgs: ## DAT-03 offline capture guards và Java response validation; không network/Docker
	@python3 -m unittest discover -s tests -p 'test_research_usgs_captures.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=ResearchUsgsCaptureCheckTest -Dsurefire.failIfNoSpecifiedTests=false

plan-research-usgs: ## DAT-03 xem12 exact USGS queries/halo/seed; không network hoặc files
	@python3 scripts/capture-research-usgs.py plan --root "$(RESEARCH_USGS_ROOT)"

capture-research-usgs: ## DAT-03 EXPLICIT original API capture/Java check; không Bronze/ETL/deployment
	@python3 scripts/capture-research-usgs.py capture --root "$(RESEARCH_USGS_ROOT)" --java "$(RESEARCH_JAVA)" --jar "$(RESEARCH_USGS_JAR)" $(if $(RESEARCH_USGS_REUSE_DECEMBER),--reuse-december "$(RESEARCH_USGS_REUSE_DECEMBER)")

verify-research-usgs: ## DAT-03 read-only exact12 captures + Java check + summary pin; không download
	@python3 scripts/capture-research-usgs.py verify --root "$(RESEARCH_USGS_ROOT)" --java "$(RESEARCH_JAVA)" --jar "$(RESEARCH_USGS_JAR)"

test-research-trino-profile: ## DAT-03 offline profile/scope/credential guards; không Docker
	@python3 -m unittest discover -s tests -p 'test_research_trino_profile.py'

prepare-research-trino-profile: ## DAT-03 explicit private backup/preflight; không restart hoặc dữ liệu
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" || { echo 'RESEARCH_DEPLOYMENT_ROOT is required'; exit 1; }
	@python3 scripts/research-trino-profile.py prepare --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)"

apply-research-trino-profile: ## DAT-03 OPERATOR-APPROVED Trino-only recreate; không build/pull/data writes
	@test "$(TRINO_PROFILE_OPERATOR_APPROVED)" = yes -a -n "$(RESEARCH_TRINO_PROFILE_FOLDER)" || { echo 'Explicit TRINO_PROFILE_OPERATOR_APPROVED=yes and exact folder are required'; exit 1; }
	@python3 scripts/research-trino-profile.py apply --folder "$(RESEARCH_TRINO_PROFILE_FOLDER)" --operator-approved-trino-only

verify-research-trino-profile: ## DAT-03 read-only late health/pins; không apply hoặc recreate lần nữa
	@test -n "$(RESEARCH_TRINO_PROFILE_FOLDER)" || { echo 'Exact RESEARCH_TRINO_PROFILE_FOLDER is required'; exit 1; }
	@python3 scripts/research-trino-profile.py verify --folder "$(RESEARCH_TRINO_PROFILE_FOLDER)"

test-research-light-session: ## DAT-03 mock four-container stop/restore; không Docker
	@python3 -m unittest discover -s tests -p 'test_research_light_session.py'

measure-research-light-session: ## DAT-03 EXPLICIT approved stop4/probe/restore4; không build/pull/data writes
	@test "$(RESEARCH_LIGHT_OPERATOR_APPROVED)" = yes -a -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_CAPACITY_RUNTIME)" || { echo 'Explicit four-container approval and exact roots required'; exit 1; }
	@python3 scripts/research-light-session.py --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --runtime-root "$(RESEARCH_CAPACITY_RUNTIME)" --operator-approved-four-container-session

test-research-collection: ## DAT-03 collection/shared Mc/whole-window Parquet regression; không Docker hoặc nguồn thật
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=ResearchCollectionPlanTest,ResearchCollectionJobTest -Dsurefire.failIfNoSpecifiedTests=false

test-research-e2e: ## DAT-03 offline metadata/receipt guards, không Docker/mạng
	@python3 -m unittest discover -s tests -p 'test_research_e2e.py'

test-research-recovery: ## DAT-03 recovery control-plane regression; không Docker hoặc data writes
	@python3 -m unittest discover -s tests -p 'test_research_recovery.py'

prepare-research-recovery: ## DAT-03 pin failed run + JAR preflight riêng; không đổi runner cũ
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_RETAINED_ROOT)" || { echo 'RESEARCH_DEPLOYMENT_ROOT/RETAINED_ROOT are required'; exit 1; }
	@python3 scripts/research-recovery.py prepare --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --original-root "$(RESEARCH_RETAINED_ROOT)"

check-research-recovery: ## DAT-03 read-only live recovery preflight; không resume/publish/release
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_RECOVERY_FOLDER)" || { echo 'RESEARCH_DEPLOYMENT_ROOT/RECOVERY_FOLDER are required'; exit 1; }
	@python3 scripts/research-recovery.py check --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --folder "$(RESEARCH_RECOVERY_FOLDER)"

test-research-continuation: ## DAT-03 continuation/code provenance/acceptance regression, không Docker
	@python3 -m unittest discover -s tests -p 'test_research_continuation.py'
	@python3 -m unittest discover -s tests -p 'test_trino_memory.py'

prepare-research-continuation: ## DAT-03 stage code recovery riêng, pin original/preflight/bytecode; không lake writes
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_RETAINED_ROOT)" -a -n "$(RESEARCH_PREFLIGHT_ROOT)" || { echo 'RESEARCH_DEPLOYMENT_ROOT/RETAINED_ROOT/PREFLIGHT_ROOT are required'; exit 1; }
	@python3 scripts/research-continuation.py prepare --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --original-root "$(RESEARCH_RETAINED_ROOT)" --preflight-root "$(RESEARCH_PREFLIGHT_ROOT)" $(if $(RESEARCH_CHECKPOINT_ROOT),--checkpoint-root "$(RESEARCH_CHECKPOINT_ROOT)")

run-research-continuation: ## DAT-03 explicit own-run continuation 2016 verify/publish -> 2023 case; release chỉ sau đủ gates
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_CONTINUATION_FOLDER)" || { echo 'RESEARCH_DEPLOYMENT_ROOT/CONTINUATION_FOLDER are required'; exit 1; }
	@python3 scripts/research-continuation.py run --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --folder "$(RESEARCH_CONTINUATION_FOLDER)"

verify-research-continuation: ## DAT-03 offline exact recovery receipt, four-case SHA chain và release proof
	@test -n "$(RESEARCH_CONTINUATION_FOLDER)" || { echo 'RESEARCH_CONTINUATION_FOLDER is required'; exit 1; }
	@python3 scripts/research-continuation.py verify --folder "$(RESEARCH_CONTINUATION_FOLDER)"

capture-research-e2e: ## DAT-03 explicit USGS tháng 12/2023 + halo; chỉ capture local, không lake writes
	@python3 scripts/research-e2e.py capture-usgs

prepare-research-e2e: ## DAT-03 stage JAR có sẵn + exact captures vào deployment staging riêng; không build/start
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" || { echo 'RESEARCH_DEPLOYMENT_ROOT is required'; exit 1; }
	@python3 scripts/research-e2e.py prepare --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --profiles "$(RESEARCH_PROFILE_ROOT)"

run-research-e2e: ## DAT-03 explicit real Bronze -> Gold/Trino + rerun trong namespace mới; giữ lease khi lỗi
	@test -n "$(RESEARCH_DEPLOYMENT_ROOT)" -a -n "$(RESEARCH_E2E_FOLDER)" || { echo 'RESEARCH_DEPLOYMENT_ROOT/E2E_FOLDER are required'; exit 1; }
	@python3 scripts/research-e2e.py run --deployment-root "$(RESEARCH_DEPLOYMENT_ROOT)" --folder "$(RESEARCH_E2E_FOLDER)"

verify-research-e2e: ## DAT-03 offline read-only exact acceptance/capture/receipt SHA chain
	@test -n "$(RESEARCH_E2E_FOLDER)" || { echo 'RESEARCH_E2E_FOLDER is required'; exit 1; }
	@python3 scripts/research-e2e.py verify --folder "$(RESEARCH_E2E_FOLDER)"
.PHONY: prepare-full-month test-full-month qa-full-month-live full-month-local
FULL_MONTH_ROOT ?= staging/orc06-full-month
FULL_MONTH_REQUEST ?=
.PHONY: prepare-real-etl-runtime test-real-etl qa-real-etl
ORC07_CAPTURE_ROOT ?= staging/orc06-full-month/captures

check-docs: ## DOC-01 validate link/file/heading Markdown nội bộ; offline, không runtime
	@python3 scripts/check-docs.py

test-docs: ## DOC-01 link checker và onboarding command regression; standard library only
	@python3 -m unittest discover -s tests -p 'test_documentation.py'

test-qa-01: ## QA-01 identity/chain/count/SQL guards + ORC-07 blocker tests offline; không Docker
	@python3 -m unittest discover -s airflow/tests -p 'test_qa_01_*.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_current_month_runtime.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_real_etl_runtime.py'
	@python3 -m unittest discover -s tests -p 'test_qa_01_cli.py'
	@python3 -m unittest discover -s tests -p 'test_current_month_cli.py'

smoke-qa-01: check-config-local ## QA-01 read-only evidence của scheduled run exact; không build/start/trigger ETL
	@$(RUNTIME_CHECK) --component observability
	@$(RUNTIME_CHECK) --component query
	@python3 scripts/qa-01.py --run-id "$(QA_01_RUN_ID)" --env-file "$(ENV_FILE)" --compose-file "$(COMPOSE_FILE)"

prepare-real-etl-runtime: check-config-local check-java-build-inputs ## ORC-07 build explicit image riêng; không start/restart foundation
	@python3 scripts/real-etl.py prepare

test-real-etl: ## ORC-07 control-plane + Spark/Iceberg local; không nguồn thật/Docker
	@python3 -m unittest discover -s tests -p 'test_real_etl_cli.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_real_etl_*.py'
	@SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost ./mvnw --batch-mode --no-transfer-progress -pl spark -am test -Dtest=RealEtlAdapterTest,GoldPublicationTest -Dsurefire.failIfNoSpecifiedTests=false

qa-real-etl: check-config-local ## ORC-07 bounded MinIO/Airflow/Trino QA; image/services/captures phải chuẩn bị riêng
	@python3 scripts/real-etl.py qa --captures "$(ORC07_CAPTURE_ROOT)"

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

test-pilot: ## Pilot/model regression; cần pyarrow/model deps và loopback Jupyter kernel, không Docker/nguồn mạng
	@$(PILOT_PYTHON) -m unittest discover -s tests -p 'test_pilot_*.py'
	@$(PILOT_PYTHON) -m unittest discover -s tests -p 'test_team_pilot.py'
	@$(PILOT_PYTHON) -m unittest tests/test_experiment_harness.py
	@$(PILOT_PYTHON) -m unittest tests/test_baseline_algorithms.py
	@$(PILOT_PYTHON) -m unittest tests/test_adaptive_hdbscan.py

inspect-pilot: ## Fresh Jupyter kernel Run All; PILOT_BUNDLE=exact local directory, không Docker/API
	@test -n "$(PILOT_BUNDLE)" || { echo 'Set PILOT_BUNDLE=staging/ml-pilot/pilot_id=<exact-id>'; exit 2; }
	@$(PILOT_PYTHON) scripts/inspect-pilot-notebook.py "$(PILOT_BUNDLE)"

pack-pilot: ## Đóng đúng 5 file pilot đã verify để gửi team; PILOT_BUNDLE và PILOT_ZIP, không overwrite khác bytes
	@test -n "$(PILOT_BUNDLE)" || { echo 'Set PILOT_BUNDLE=staging/ml-pilot/pilot_id=<exact-id>'; exit 2; }
	@$(PILOT_PYTHON) scripts/pack-pilot.py "$(PILOT_BUNDLE)" "$(PILOT_ZIP)"

jupyter-pilot: ## Mở Jupyter loopback với bundle local đã chọn; Ctrl+C để dừng, không tự export
	@test -n "$(PILOT_BUNDLE)" || { echo 'Set PILOT_BUNDLE=staging/ml-pilot/pilot_id=<exact-id>'; exit 2; }
	@PILOT_BUNDLE="$(abspath $(PILOT_BUNDLE))" $(PILOT_PYTHON) -m jupyterlab --ip=127.0.0.1 --no-browser notebooks/dat_02_inspection.ipynb

test-exp01: ## EXP-01 harness & Colab adapter unit tests; không Docker/network
	@python3 -m unittest tests/test_experiment_harness.py

inspect-exp01: ## EXP-01 fresh Jupyter kernel Run All; không Docker/API
	@python3 scripts/inspect-exp01-notebook.py $${PILOT_BUNDLE:+--bundle "$$PILOT_BUNDLE"}

jupyter-exp01: ## Mở Jupyter loopback cho EXP-01 harness smoke notebook
	@jupyter lab --ip=127.0.0.1 --no-browser notebooks/exp_01_harness_smoke.ipynb

test-exp02: ## EXP-02 Window, DBSCAN, HDBSCAN global unit tests; không Docker/network
	@python3 -m unittest tests/test_baseline_algorithms.py

inspect-exp02: ## EXP-02 fresh Jupyter kernel Run All cho baseline algorithms; không Docker/API
	@python3 scripts/inspect-exp02-notebook.py $${PILOT_BUNDLE:+--pilot-bundle "$$PILOT_BUNDLE"}

jupyter-exp02: ## Mở Jupyter loopback cho EXP-02 baseline algorithms notebook
	@jupyter lab --ip=127.0.0.1 --no-browser notebooks/exp_02_baseline_algorithms.ipynb

test-exp03: ## EXP-03 HDBSCAN adaptive và entity resolution unit tests; không Docker/network
	@python3 -m unittest tests/test_adaptive_hdbscan.py

inspect-exp03: ## EXP-03 fresh Jupyter kernel Run All cho adaptive HDBSCAN; không Docker/API
	@python3 scripts/inspect-exp03-notebook.py $${PILOT_BUNDLE:+--pilot-bundle "$$PILOT_BUNDLE"}

jupyter-exp03: ## Mở Jupyter loopback cho EXP-03 adaptive HDBSCAN notebook
	@jupyter lab --ip=127.0.0.1 --no-browser notebooks/exp_03_adaptive_hdbscan.ipynb
export QA_RUN_ID

test-ml-window: ## MLD-03 bucket physical plan, distributed invariants, microseconds và fail-fast Parquet tests
	@python3 -m unittest discover -s airflow/tests -p 'test_mainshock_window_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=WindowModelResolverTest,MainshockWindowEngineTest,MainshockWindowIntegrationTest,MainshockWindowRegressionTest,MainshockRuntimeTest,WindowCoveragePolicyTest,JmaMainshockPilotSamplerTest \
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

test-qa-03: ## QA-03 historical scope/resource/reuse matrix và report checks; không gọi nguồn
	@python3 -m unittest discover -s airflow/tests -p 'test_qa_03_jma_historical_backfill.py'
	@python3 airflow/dags/qa_03_jma_historical_backfill.py --matrix "$(QA_03_MATRIX)" --json "$(QA_03_JSON)" --markdown "$(QA_03_MARKDOWN)"

smoke-qa-03: check-config-local ## QA-03 Bronze-to-Published backfill; cần ORC-07 và request runtime đã pin
	@test -n "$(QA_03_RUNTIME_REQUEST)" || { echo 'Set QA_03_RUNTIME_REQUEST=/opt/pipeline/staging/.../request.json'; exit 2; }
	@test -n "$(QA_03_RUNTIME_COMMAND)" || { echo 'Set QA_03_RUNTIME_COMMAND to the JMA backfill runner'; exit 2; }
	@$(RUNTIME_CHECK) --component gold --client qa-03-jma-historical-backfill
	@$(COMPOSE) --profile gold run --rm --no-deps --pull never \
		-e QA_03_RUNTIME_REQUEST="$(QA_03_RUNTIME_REQUEST)" \
		-e QA_03_RUNTIME_COMMAND="$(QA_03_RUNTIME_COMMAND)" \
		qa-03-jma-historical-backfill
test-qa-02: ## QA-02 matrix/fixture/report checks; không gọi nguồn hoặc runtime
	@python3 -m unittest discover -s airflow/tests -p 'test_qa_02_rerun_revision.py'
	@python3 airflow/dags/qa_02_rerun_revision.py --matrix "$(QA_02_MATRIX)" --json "$(QA_02_JSON)" --markdown "$(QA_02_MARKDOWN)"

smoke-qa-02: check-config-local ## QA-02 Bronze-to-Published rerun/revision; cần ORC-07 và request runtime đã pin
	@test -n "$(QA_02_RUNTIME_REQUEST)" || { echo 'Set QA_02_RUNTIME_REQUEST=/opt/pipeline/staging/.../request.json'; exit 2; }
	@test -n "$(QA_02_RUNTIME_COMMAND)" || { echo 'Set QA_02_RUNTIME_COMMAND to the QA adapter runner'; exit 2; }
	@$(RUNTIME_CHECK) --component ml --client qa-02-rerun-revision
	@$(COMPOSE) run --rm --no-deps --pull never \
		-e QA_02_RUNTIME_REQUEST="$(QA_02_RUNTIME_REQUEST)" \
		-e QA_02_RUNTIME_COMMAND="$(QA_02_RUNTIME_COMMAND)" \
		qa-02-rerun-revision

test-mli-import: ## MLI-03 offline bundle validation, journal idempotency và control-plane handoff
	@python3 -m unittest discover -s airflow/tests -p 'test_mli_03_import_results.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_mli_import_qa.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_mli_import_dag_qa.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=ResultBundleContractTest,MlResultImportJobTest,TrustedDatasetResolverTest,MlResultPublicationIcebergTest,TrinoMlVerifierTest -Dsurefire.failIfNoSpecifiedTests=false test

smoke-mli-import: check-config-local ## MLI-03 runtime import vào namespace QA; cần Docker/MinIO/Catalog/Trino và exact request
	@test -n "$(MLI_IMPORT_REQUEST)" || { echo 'Set MLI_IMPORT_REQUEST=/opt/pipeline/staging/.../request.json'; exit 2; }
	@test -n "$(MLI_IMPORT_RUNNER_COMMAND)" || { echo 'Set MLI_IMPORT_RUNNER_COMMAND to the container Spark runner'; exit 2; }
	@$(RUNTIME_CHECK) --component ml --client mli-03-import-results
	@$(COMPOSE) run --rm --no-deps --pull never \
		-e MLI_IMPORT_REQUEST="$(MLI_IMPORT_REQUEST)" \
		-e MLI_IMPORT_RUNNER_COMMAND="$(MLI_IMPORT_RUNNER_COMMAND)" \
		mli-03-import-results

smoke-mli-import-acceptance: check-config-local ## MLI-03 synthetic fixtures trên MinIO/Iceberg/Trino; namespace QA mới, không build/start/reset
	@$(RUNTIME_CHECK) --component ml --client mli-03-import-results
	@$(COMPOSE) run --rm --no-deps --pull never --entrypoint python3 \
		mli-03-import-results /opt/airflow/dags/mli_import_qa.py

smoke-mli-import-dag: check-config-local ## MLI-03 fresh DAG.test trên accepted QA request; không clear/mark-success hoặc build/start
	@test -n "$(MLI_IMPORT_REQUEST)" || { echo 'Set MLI_IMPORT_REQUEST to the accepted QA import-request.json'; exit 2; }
	@$(RUNTIME_CHECK) --component ml --client mli-03-import-results
	@$(RUNTIME_CHECK) --component airflow
	@$(COMPOSE) exec -T airflow-dag-processor python3 /opt/airflow/dags/mli_import_dag_qa.py "$(MLI_IMPORT_REQUEST)"
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

test-qa-04: test-qa-acceptance ## QA-04 fixed-runner + bounded matrix/component gates offline; Java17
	@python3 -m unittest discover -s airflow/tests -p 'test_qa_04_failure_recovery.py'
	@python3 -m unittest discover -s airflow/tests -p 'test_recovery*.py'
	@./mvnw --batch-mode --no-transfer-progress -pl spark -am \
		-Dtest=GoldIcebergWriterTest,GoldPublicationTest,TrinoSqlClientTest,ResultBundleContractTest \
		-Dsurefire.failIfNoSpecifiedTests=false test

smoke-qa-04: check-config-local ## QA-04 runtime recovery trong namespace QA; không tự build/start/init
	@test -n "$(QA_04_REQUEST)" || { echo 'Set QA_04_REQUEST=/opt/pipeline/staging/.../qa-04-request.json'; exit 2; }
	@test -n "$(QA_04_RUNTIME_COMMAND)" || { echo 'Set QA_04_RUNTIME_COMMAND to the deployed QA-04 runner'; exit 2; }
	@$(RUNTIME_CHECK) --component ml --client qa-04-failure-recovery
	@$(COMPOSE) --profile ml run --rm --no-deps --pull never \
		-e QA_04_REQUEST="$(QA_04_REQUEST)" \
		-e QA_04_RUNTIME_COMMAND="$(QA_04_RUNTIME_COMMAND)" \
		-e QA_04_STAGING_ROOT="$(QA_04_STAGING_ROOT)" \
		-e ORC_07_STATUS="$${ORC_07_STATUS:-Ready}" \
		-e MLI_03_STATUS="$${MLI_03_STATUS:-In Progress}" \
		qa-04-failure-recovery

.PHONY: ci ci-docs ci-java ci-airflow test-ci-workflow test-security-audit audit-security audit-security-runtime audit-security-runtime-readonly
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
	@$(MAKE) check-docs test-docs
	@$(MAKE) test-contracts
	@python3 -m unittest discover -s tests -p 'test_real_etl_cli.py'
	@python3 -m unittest discover -s tests -p 'test_full_month_*.py'
	@python3 -m unittest discover -s tests -p 'test_qa_01_cli.py'
	@python3 -m unittest discover -s tests -p 'test_current_month_cli.py'
	@python3 -m unittest discover -s tests -p 'test_ml_export_cli.py'
	@$(MAKE) test-research-e2e
	@$(MAKE) test-research-recovery
	@$(MAKE) test-research-continuation
	@$(MAKE) test-research-rollout
	@python3 -m unittest discover -s tests -p 'test_research_usgs_captures.py'
	@$(MAKE) test-research-trino-profile
	@$(MAKE) test-research-light-session
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
	@python3 scripts/audit-security.py --output "$(SECURITY_REPORT)" $(SECURITY_AUDIT_FLAGS)

audit-security-runtime-readonly: ## SEC-01 retained logs + pinned ML handoff, không S3 writes/build/start/init
	@python3 scripts/audit-security.py --output "$(SECURITY_REPORT)" --runtime $(SECURITY_AUDIT_FLAGS)

audit-security-runtime: ## SEC-01 security inspection + exact-key permission QA; không build/start/init
	@test "$(ENV_FILE)" = .env && test "$(COMPOSE_FILE)" = compose.yaml || \
		{ echo 'SEC-01 runtime audit only supports the project .env and compose.yaml.' >&2; exit 1; }
	@if test -z "$(SECURITY_DEPLOYMENT_ROOT)"; then ./scripts/check-config.sh --require-local; fi
	@python3 scripts/audit-security.py --output "$(SECURITY_REPORT)" --runtime --permission-probes $(SECURITY_AUDIT_FLAGS)

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

test-contracts: $(CONTRACT_CHECKS) check-week-5-plan test-week-5-plan check-java-build-inputs test-research-profile ## Kiểm tra docs, contract, fixture và catalog mẫu; không cần mạng

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

up-ml: check-config-local ## Start ML storage/query + staging init khi cần; không build/pull hoặc trigger DAG
	@$(RUNTIME_ENV) WAIT_TIMEOUT="$(WAIT_TIMEOUT)" python3 scripts/docker-runtime.py up --component ml

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
