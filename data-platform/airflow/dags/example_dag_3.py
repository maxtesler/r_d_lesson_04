from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from typing import List

"""
Example Airflow DAG using TaskFlow API mixed with classic operators.

File: /c:/Users/Illia/projects/robot_dreams/UA_DATA-ENGINEERING_KHOROSHYKH/lecture_07/data-platform/airflow/dags/example_dag_2.py
"""

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="example_taskflow_mix_operators",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "taskflow", "mix"],
) as dag:

    @task(task_id="generate_numbers")
    def generate_numbers(n: int = 5) -> List[int]:
        """Generate a simple list of integers."""
        return list(range(1, n + 1))

    @task(task_id="multiply")
    def multiply(nums: List[int], factor: int = 2) -> List[int]:
        """Multiply each number by factor and return the new list."""
        return [x * factor for x in nums]

    # TaskFlow tasks produce Task objects when called
    nums = generate_numbers(6)
    multiplied = multiply(nums, factor=3)

    # Classic BashOperator: write multiplied list (pulled from XCom) to a file
    write_to_file = BashOperator(
        task_id="write_to_file",
        bash_command=(
            "echo {{ ti.xcom_pull(task_ids='multiply') }} > "
            "/tmp/airflow_example_output.txt"
        ),
    )

    # Classic PythonOperator: read the file and log its content
    def read_and_log_file(**context):
        path = "/tmp/airflow_example_output.txt"
        try:
            with open(path, "r") as f:
                content = f.read().strip()
        except FileNotFoundError:
            content = f"File {path} not found"
        print("Contents of output file:", content)
        return content

    read_file = PythonOperator(
        task_id="read_file",
        python_callable=read_and_log_file,
        provide_context=True,
    )

    # Simple end marker using EmptyOperator
    done = EmptyOperator(task_id="done")

    # set dependencies: TaskFlow -> BashOperator -> PythonOperator -> done
    multiplied >> write_to_file >> read_file >> done