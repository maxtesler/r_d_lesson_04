# airflow/Dockerfile
FROM apache/airflow:2.6.0-python3.9

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    netcat \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy Airflow configuration and entrypoint script
COPY airflow.cfg /opt/airflow/airflow.cfg
COPY scripts/entrypoint.sh /opt/airflow/entrypoint.sh

# Set proper permissions for the entrypoint script
RUN chmod +x /opt/airflow/entrypoint.sh

USER airflow

# Set environment variable to disable provider discovery of Google packages
ENV AIRFLOW__PROVIDERS__DISABLE_PACKAGE_PROVIDERS=google,google_cloud,apache-airflow-providers-google

# Install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt && \
    pip uninstall -y apache-airflow-providers-google apache-airflow-providers-google-cloud


# Set up directory for dbt
RUN mkdir -p /opt/airflow/dags/dbt

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]