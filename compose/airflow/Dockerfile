FROM maven:3.9.16-eclipse-temurin-17 AS build

WORKDIR /workspace

COPY pom.xml ./pom.xml
COPY spark/pom.xml ./spark/pom.xml
COPY spark/src ./spark/src
COPY tests/fixtures ./tests/fixtures

RUN mvn --batch-mode --no-transfer-progress clean verify

FROM apache/spark:3.5.9-scala2.12-java17-ubuntu AS java-runtime

FROM apache/airflow:3.3.2-python3.13

USER root

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"

RUN mkdir -p /opt/pipeline/bin /opt/pipeline/lib \
    && chown -R airflow:0 /opt/pipeline

COPY --from=java-runtime /opt/java/openjdk /opt/java/openjdk
COPY --from=build --chown=airflow:0 \
    /workspace/spark/target/japan-earthquake-etl-runner.jar \
    /opt/pipeline/lib/usgs-ingest-runner.jar
COPY --chown=airflow:0 --chmod=0755 \
    compose/airflow/usgs-runner.sh \
    /opt/pipeline/bin/usgs-ingest-runner
COPY --chown=airflow:0 --chmod=0755 \
    compose/airflow/jma-runner.sh \
    /opt/pipeline/bin/jma-ingest-runner
COPY --chown=airflow:0 config/jma/hypocenter_archives_v1.csv \
    /opt/pipeline/config/jma/hypocenter_archives_v1.csv

USER airflow
