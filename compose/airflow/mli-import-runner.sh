#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  printf '%s\n' 'MLI03_REQUEST_REQUIRED' >&2
  exit 2
fi

exec /opt/spark/bin/spark-submit \
  --master "${MLI_SPARK_MASTER:-local[2]}" \
  --conf spark.sql.session.timeZone=UTC \
  --jars /opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar \
  --class ie212.earthquake.spark.ml.MlResultImportJob \
  /opt/spark/jobs/japan-earthquake-etl-runner.jar "$1"
