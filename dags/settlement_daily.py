"""
정산 일배치.

dag_id는 Spring Batch job 이름(dailySettlementJob)과 정확히 동일해야 한다.
완료 시 TriggerDagRunOperator로 settlementAggregateJob을 트리거한다
(settlement_daily -> settlement_aggregate -> settlement_report 체인의 첫 단계).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "dailySettlementJob"

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
    description="정산 일배치 (Spring Batch: dailySettlementJob) — 완료 시 settlementAggregateJob 트리거",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "settlement"],
    params={"target_date": ""},
) as dag:

    run_daily_settlement = KubernetesPodOperator(
        task_id="run_daily_settlement",
        name="daily-settlement-batch",
        **batch_pod_kwargs(
            group="job-settlement",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )

    trigger_settlement_aggregate = TriggerDagRunOperator(
        task_id="trigger_settlement_aggregate",
        trigger_dag_id="settlementAggregateJob",
        conf={"target_date": "{{ params.target_date }}"},
        wait_for_completion=False,
    )

    run_daily_settlement >> trigger_settlement_aggregate
