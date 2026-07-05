"""
일별 광고 통계 집계 배치.

dag_id는 Spring Batch job 이름(dailyAdStatsJob)과 정확히 동일해야 한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "dailyAdStatsJob"

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
    description="일별 광고 통계 집계 배치 (Spring Batch: dailyAdStatsJob)",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "ads"],
    params={"target_date": ""},
) as dag:

    run_daily_ad_stats = KubernetesPodOperator(
        task_id="run_daily_ad_stats",
        name="daily-ad-stats-batch",
        **batch_pod_kwargs(
            group="job-ads",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )
