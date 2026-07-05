"""
광고 통계 CSV 추출 배치.

dag_id는 Spring Batch job 이름(adsCsvExportJob)과 정확히 동일해야 한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "adsCsvExportJob"

default_args = {
    "owner": "adip",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=JOB_NAME,
    default_args=default_args,
    description="광고 통계 CSV 추출 배치 (Spring Batch: adsCsvExportJob)",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "ads"],
    params={"target_date": ""},
) as dag:

    run_ads_csv_export = KubernetesPodOperator(
        task_id="run_ads_csv_export",
        name="ads-csv-export-batch",
        **batch_pod_kwargs(
            group="job-ads",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )
