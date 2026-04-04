from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


COMPOSE_NETWORK = "global_malaria_burden_predicting_system_default"

# Common AWS environment variables — defined once, reused in all three tasks
AWS_ENV = {
    "AWS_ACCESS_KEY_ID":     "{{ var.value.AWS_ACCESS_KEY_ID }}",
    "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
    "AWS_DEFAULT_REGION":    "{{ var.value.AWS_DEFAULT_REGION }}",
    "S3_BUCKET_NAME":        "{{ var.value.S3_BUCKET_NAME }}",
}

LOGS_MOUNT = Mount(
    source="/home/brenda/Global_Malaria_burden_predicting_system/logs",
    target="/app/logs",
    type="bind",
)

with DAG(
    dag_id="malaria_pipeline",
    default_args=default_args,
    description="Daily malaria data ingestion, transformation and model training",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["malaria", "ingestion", "transformation", "training"],
) as dag:

    ingest = DockerOperator(
        task_id="bronze_ingestion",
        image="malaria-ingestion",
        command="python ingestion/run_ingestion.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        environment=AWS_ENV,
        mounts=[LOGS_MOUNT],
    )

    transform = DockerOperator(
        task_id="silver_gold_transformation",
        image="malaria-transformation",
        command="python transformation/run_transformation.py",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        environment=AWS_ENV,
        mounts=[LOGS_MOUNT],
    )

    train = DockerOperator(
        task_id="ml_training",
        image="malaria-training",
        command="python model_training/run_training.py",
        docker_url="unix:///var/run/docker.sock",
        
        network_mode=COMPOSE_NETWORK,
        auto_remove=True,
        mount_tmp_dir=False,
        environment={
            **AWS_ENV,
            "MLFLOW_TRACKING_URI": "http://mlflow:5000",
        },
        mounts=[LOGS_MOUNT],
    )

    ingest >> transform >> train