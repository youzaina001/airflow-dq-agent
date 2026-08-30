FROM apache/airflow:3.1.5-python3.12

ARG AIRFLOW_VERSION=3.1.5

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-airflow.txt

COPY --chown=airflow:root pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root evals /opt/airflow/evals

ENV PYTHONPATH=/opt/airflow/src
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
