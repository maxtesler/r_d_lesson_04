from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


create_table_sql = """
CREATE TABLE IF NOT EXISTS analytics.fake_events (
    id SERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    user_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    value NUMERIC
);
"""


def gen_insert_sql(rows_count=100):
    event_types = ["view", "click", "purchase", "signup"]
    now = datetime.utcnow()
    values = []
    for _ in range(rows_count):
        ts = now - timedelta(seconds=random.randint(0, 86400))
        # format timestamp as UTC with explicit offset
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S+00")
        user_id = random.randint(1, 1000)
        event_type = random.choice(event_types).replace("'", "''")
        value = round(random.random() * 100, 2) if event_type == "purchase" else None
        value_sql = "NULL" if value is None else str(value)
        values.append(f"('{ts_str}', {user_id}, '{event_type}', {value_sql})")

    if not values:
        return ""  # nothing to run

    sql = (
        "INSERT INTO analytics.fake_events (event_time, user_id, event_type, value) VALUES\n"
        + ",\n".join(values)
        + ";"
    )
    return sql


with DAG(
    dag_id="example_write_fake_to_postgres",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=4,
) as dag:

    # use SqlOperator to run DDL
    t1_create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        sql=create_table_sql,
        conn_id="postgres_analytics",
    )

    # generate the INSERT SQL at runtime and push via XCom
    t2_gen_sql = PythonOperator(
        task_id="gen_insert_sql",
        python_callable=gen_insert_sql,
        op_kwargs={"rows_count": 500},
    )

    # use SqlOperator to execute the generated INSERT SQL (reads SQL from XCom)
    t3_insert_data = SQLExecuteQueryOperator(
        task_id="insert_fake_rows",
        sql="{{ ti.xcom_pull(task_ids='gen_insert_sql') }}",
        conn_id="postgres_analytics",
    )

    t1_create_table >> t2_gen_sql >> t3_insert_data