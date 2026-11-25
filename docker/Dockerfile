# Airflow 3 + Java + PySpark in one image: Spark runs in local mode inside the
# Airflow worker. On a cluster, swap the Spark tasks for SparkSubmitOperator /
# EMR / Dataproc operators that call `python -m meter_pipeline.jobs <job>`.
FROM apache/airflow:3.2.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY --chown=airflow:root pyproject.toml README.md /opt/ami/
COPY --chown=airflow:root src /opt/ami/src
COPY --chown=airflow:root conf /opt/ami/conf
COPY --chown=airflow:root data_generator /opt/ami/data_generator
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /opt/ami

ENV AMI_CONFIG=/opt/ami/conf/pipeline.yaml \
    AMI_DATA_ROOT=/opt/ami/data \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/ami/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False
COPY --chown=airflow:root dags /opt/ami/dags
