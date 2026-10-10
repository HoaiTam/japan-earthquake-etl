FROM maven:3.9.16-eclipse-temurin-17 AS build

WORKDIR /workspace

COPY pom.xml ./pom.xml
COPY spark/pom.xml ./spark/pom.xml
COPY spark/src ./spark/src
COPY tests/fixtures ./tests/fixtures
COPY config/jma ./config/jma

RUN mvn --batch-mode --no-transfer-progress clean verify

FROM apache/spark:3.5.9-scala2.12-java17-ubuntu

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && mkdir -p /opt/spark/jobs \
    && chown -R spark:spark /opt/spark/jobs

COPY --from=build --chown=spark:spark \
    /workspace/spark/target/japan-earthquake-etl.jar \
    /opt/spark/jobs/japan-earthquake-etl.jar

COPY --from=build --chown=spark:spark \
    /workspace/spark/target/japan-earthquake-etl-runner.jar \
    /opt/spark/jobs/japan-earthquake-etl-runner.jar

USER spark

# Keep Iceberg/AWS runtime outside SDK relocation; load explicitly with --jars.
COPY --from=build --chown=spark:spark /workspace/spark/target/iceberg/ /opt/spark/jobs/iceberg/
