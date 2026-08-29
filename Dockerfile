FROM apache/airflow:3.1.5-python3.12

ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.12
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-airflow.txt

COPY --chown=airflow:root pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root evals /opt/airflow/evals

ENV PYTHONPATH=/opt/airflow/src
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
