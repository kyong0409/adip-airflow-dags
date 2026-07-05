"""
주문 동기화 배치.

dag_id는 Spring Batch job 이름(orderSyncJob)과 정확히 동일해야 한다 — dscore.fw-be의
AirflowDagAdapter가 jobNm을 그대로 dag_id로 사용해 POST /api/v2/dags/{dag_id}/dagRuns 를 호출한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "orderSyncJob"

default_args = {
    "owner": "adip",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    # 의도적 실패(skip 초과) 시연 DAG — retry 없이 즉시 FAILED로 떨어져야 UI 재실행 데모가 빠르다
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=JOB_NAME,
    default_args=default_args,
    description="주문 동기화 배치 (Spring Batch: orderSyncJob)",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "order"],
    params={"target_date": ""},
) as dag:

    run_order_sync = KubernetesPodOperator(
        task_id="run_order_sync",
        name="order-sync-batch",
        **batch_pod_kwargs(
            group="job-order",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )
