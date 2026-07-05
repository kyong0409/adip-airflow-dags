"""
정산 리포트 배치.

dag_id는 Spring Batch job 이름(settlementReportJob)과 정확히 동일해야 한다.
settlementAggregateJob 완료 시 트리거되어 실행되는 settlement 체인의 마지막 단계.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "settlementReportJob"

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
    description="정산 리포트 배치 (Spring Batch: settlementReportJob)",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "settlement"],
    params={"target_date": ""},
) as dag:

    run_settlement_report = KubernetesPodOperator(
        task_id="run_settlement_report",
        name="settlement-report-batch",
        **batch_pod_kwargs(
            group="job-settlement",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )
