from datetime import timedelta
import json
import logging
import random
import uuid
import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="airflow_is_the_best_orchestrator",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["example", "taskflow", "xcom"],
)
def example_dag():
    @task
    def create_fake_json(n_rows: int = 5):
        """
        Create a fake JSON-serializable payload and return it.
        The returned object will be automatically pushed to XCom by TaskFlow API.
        """
        payload = []
        for i in range(n_rows):
            payload.append(
                {
                    "id": str(uuid.uuid4()),
                    "index": i,
                    "name": f"name_{i}",
                    "value": round(random.random() * 100, 2),
                }
            )
        # payload is a list of dicts which is JSON-serializable
        logger.info("Created payload with %d rows", len(payload))
        return payload

    @task
    def read_as_dataframe(xcom_payload):
        """
        Receive XCom payload, try to convert it to a pandas DataFrame and log it.
        Handles either a Python object (list/dict) or a JSON string.
        """
        try:
            data = xcom_payload
            # If payload is a JSON string, parse it first
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    logger.error("Payload is a string but not valid JSON")
                    raise

            df = pd.DataFrame(data)
            logger.info("Converted payload to DataFrame with shape %s", df.shape)
            # Log DataFrame contents
            logger.info("DataFrame contents:\n%s", df.to_string(index=False))
            return {"n_rows": len(df), "columns": list(df.columns)}
        except Exception:
            logger.exception("Failed to convert XCom payload to DataFrame")
            raise

    # Task wiring using TaskFlow API (return values are passed via XCom)
    payload = create_fake_json()
    read_as_dataframe(payload)


dag = example_dag()