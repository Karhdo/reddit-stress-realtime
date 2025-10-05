from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="retrain_stress_model",
    schedule_interval="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=5)},
) as dag:

    train = BashOperator(
        task_id="train",
        bash_command="python -m src.model.train",
    )
