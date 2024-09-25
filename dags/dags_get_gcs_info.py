import pendulum
import logging
from google.cloud import storage
from airflow.decorators import task
from airflow.models.dag import DAG

AWS_HYPERBILLING_GCS_BUCKET_NAME = "hyperbilling_dataset"

_LOGGER = logging.getLogger(__name__)

with DAG(
    dag_id="split_aws_hyperbilling_all_dataset_step1",
    schedule=None,
    start_date=pendulum.datetime(2024, 5, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["Megazone", "SpaceONE"],
) as dag:

    @task(task_id="select_to_sync_parquet_file")
    def select_to_sync_parquet_file():
        client = storage.Client()
        bucket = client.get_bucket(AWS_HYPERBILLING_GCS_BUCKET_NAME)

        target_to_sync_parquet_file_path = [
            object_info.name
            for object_info in bucket.list_blobs()
            if object_info.name.endswith(".parquet")
        ]

        _LOGGER.info(
            f"[TASK] select_to_sync_parquet_file: {target_to_sync_parquet_file_path}"
        )
        return target_to_sync_parquet_file_path
