from __future__ import annotations

import os
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Якщо у твоєму Airflow немає SimpleHttpOperator — скажи, я дам альтернативу через PythonOperator + requests
from airflow.operators.email import EmailOperator
import requests
from airflow.operators.python import PythonOperator


KYIV_TZ = pendulum.timezone("Europe/Kiev")

DBT_JOB_ENDPOINT = os.getenv("DBT_JOB_ENDPOINT", "").strip()
TRAIN_JOB_ENDPOINT = os.getenv("TRAIN_JOB_ENDPOINT", "").strip()
EMAIL_TO = os.getenv("EMAIL_TO", "").strip()

# Це "http_conn_id" Airflow connection.
# Для простоти в навчальному сетапі ми використаємо endpoint як повний URL (див. use of endpoint below),
# але SimpleHttpOperator зручніше працює з Connection.
# Якщо хочеш через Connection — скажи, зробимо.
HTTP_CONN_ID = "http_default"


def _validate_env() -> None:
    missing = []
    if not DBT_JOB_ENDPOINT:
        missing.append("DBT_JOB_ENDPOINT")
    if not TRAIN_JOB_ENDPOINT:
        missing.append("TRAIN_JOB_ENDPOINT")
    if not EMAIL_TO:
        missing.append("EMAIL_TO")

    if missing:
        raise ValueError(
            "Missing required env vars: "
            + ", ".join(missing)
            + ". Add them to .env and restart containers."
        )


_validate_env()


def _build_process_date(ds: str, dag_run_conf: dict | None) -> str:
    """
    ds: Airflow execution date string in YYYY-MM-DD
    dag_run_conf: dagrun.conf (manual trigger)
    Rules:
      - if dagrun.conf has 'process_date' -> use it
      - else use ds
    """
    if dag_run_conf and dag_run_conf.get("process_date"):
        return str(dag_run_conf["process_date"])
    return ds

def post_job(url: str, process_date: str) -> None:
    resp = requests.post(url, json={"process_date": process_date}, timeout=30)
    if resp.status_code != 201:
        raise ValueError(
            f"Job call failed: url={url} status={resp.status_code} body={resp.text}"
        )

with DAG(
    dag_id="process_iris",
    description="Process Iris dataset via dbt -> train model -> email notification",
    start_date=pendulum.datetime(2025, 4, 22, 1, 0, 0, tz=KYIV_TZ),
    # end_date is exclusive in Airflow scheduling sense for generating runs.
    # We want 22,23,24 => stop at 2025-04-25 01:00
    end_date=pendulum.datetime(2025, 4, 25, 1, 0, 0, tz=KYIV_TZ),
    schedule="0 1 * * *",  # 01:00 every day
    catchup=True,          # щоб Airflow “догнав” 22-24 квітня
    max_active_runs=1,
    default_args={
        "owner": "max",
        "retries": 0,
    },
    tags=["robotdreams", "iris", "dbt", "ml"],
) as dag:

    start = EmptyOperator(task_id="start")

    # 1) DBT job trigger (must return 201)
    run_dbt = PythonOperator(
    task_id="run_dbt_transform",
    python_callable=post_job,
    op_kwargs={
        "url": DBT_JOB_ENDPOINT,
        "process_date": "{{ dag_run.conf.get('process_date', ds) }}",
    },
)

    # 2) Train model job trigger (must return 201)
    train_model = PythonOperator(
    task_id="train_model",
    python_callable=post_job,
    op_kwargs={
        "url": TRAIN_JOB_ENDPOINT,
        "process_date": "{{ dag_run.conf.get('process_date', ds) }}",
    },
)

    # 3) Email notification
    notify = EmailOperator(
        task_id="notify_success",
        to=EMAIL_TO,
        subject="✅ process_iris успішно виконано за {{ dag_run.conf.get('process_date', ds) }}",
        html_content="""
        <p>DAG <b>process_iris</b> успішно виконався.</p>
        <ul>
          <li>Дата обробки: <b>{{ dag_run.conf.get('process_date', ds) }}</b></li>
          <li>Run ID: {{ run_id }}</li>
        </ul>
        """,
    )

    end = EmptyOperator(task_id="end")

    start >> run_dbt >> train_model >> end
