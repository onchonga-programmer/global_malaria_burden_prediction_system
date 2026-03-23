from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ── Default settings applied to every task ───────────────────
default_args = {
    "owner": "airflow",
    "retries": 2,                          # retry failed tasks twice
    "retry_delay": timedelta(minutes=5),   # wait 5 mins between retries
    "email_on_failure": False,
}

# ── Define the DAG ────────────────────────────────────────────
with DAG(
    dag_id="malaria_pipeline",
    description="Ingest malaria data from OWID and transform to gold layer",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",   # every day at 6am
    catchup=False,                   # don't backfill missed runs
    tags=["malaria", "ingestion", "transformation"],
) as dag:

    # ── Task 1: Ingestion ─────────────────────────────────────
    ingest = DockerOperator(
        task_id="ingest_malaria_data",
        image="malaria-ingestion",
        auto_remove=True,           # remove container after it finishes
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        environment={
            "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
            "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
        },
        mounts=[
            Mount(source="/logs", target="/app/logs", type="bind")
        ],
    )

    # ── Task 2: Transformation ────────────────────────────────
    transform = DockerOperator(
        task_id="transform_malaria_data",
        image="malaria-transformation",
        auto_remove=True,
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        environment={
            "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
            "S3_BUCKET_NAME": "{{ var.value.S3_BUCKET_NAME }}",
        },
        mounts=[
            Mount(source="/logs", target="/app/logs", type="bind")
        ],
    )

    