#!/usr/bin/env bash
set -euo pipefail

# Central spark-submit wrapper so the Maven package list lives in ONE place
exec /opt/spark/bin/spark-submit \
  --master "local[2]" \
  --driver-memory 1g \
  --conf "spark.jars.ivy=/tmp/ivy" \
  --packages "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  "$@"
