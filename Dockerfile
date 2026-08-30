FROM apache/airflow:3.1.5-python3.12

ARG AIRFLOW_VERSION=3.1.5
ARG PYTHON_VERSION=3.12
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-no-providers-${PYTHON_VERSION}.txt"

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
COPY constraints-airflow-overlay.txt /constraints-airflow-overlay.txt
COPY scripts/merge_airflow_constraints.py /merge_airflow_constraints.py
RUN python /merge_airflow_constraints.py \
        --base-url "${AIRFLOW_CONSTRAINTS_URL}" \
        --overlay /constraints-airflow-overlay.txt \
        --output /tmp/constraints-airflow.txt \
    && pip install --no-cache-dir --constraint /tmp/constraints-airflow.txt \
        "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-airflow.txt

COPY --chown=airflow:root pyproject.toml /opt/airflow/pyproject.toml
COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root evals /opt/airflow/evals

ENV PYTHONPATH=/opt/airflow/src
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true
