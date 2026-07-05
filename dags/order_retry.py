"""
주문 동기화 재실행 배치 — 실패 후 재실행(retry) 시연을 위한 DAG.

dag_id는 Spring Batch job 이름(orderRetryJob)과 정확히 동일해야 한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "orderRetryJob"

default_args = {
    "owner": "adip",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id=JOB_NAME,
    default_args=default_args,
    description="주문 동기화 재실행 배치 (Spring Batch: orderRetryJob) — 실패/재실행 시연용",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "order"],
    params={"target_date": ""},
) as dag:

    run_order_retry = KubernetesPodOperator(
        task_id="run_order_retry",
        name="order-retry-batch",
        **batch_pod_kwargs(
            group="job-order",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )
