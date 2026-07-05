"""
정산 집계 배치.

dag_id는 Spring Batch job 이름(settlementAggregateJob)과 정확히 동일해야 한다.
dailySettlementJob 완료 시 TriggerDagRunOperator로 트리거되어 실행되고, 완료 시
settlementReportJob을 다시 트리거한다 (settlement 체인의 두 번째 단계).

참고 — TriggerDagRunOperator(wait_for_completion=False) 대신, "선행 DAG의 특정 태스크가
끝날 때까지 이 DAG 내부에서 명시적으로 기다린다" 는 패턴이 필요하면 ExternalTaskSensor를
쓸 수도 있다 (이 데모에서는 트리거 체인만으로 충분해 실제로는 사용하지 않는다):

    from airflow.sensors.external_task import ExternalTaskSensor

    wait_for_daily_settlement = ExternalTaskSensor(
        task_id="wait_for_daily_settlement",
        external_dag_id="dailySettlementJob",
        external_task_id="run_daily_settlement",
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        deferrable=True,  # 워커 슬롯을 점유하지 않고 비동기로 대기
    )
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from config.defaults import batch_pod_kwargs

JOB_NAME = "settlementAggregateJob"

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
    description="정산 집계 배치 (Spring Batch: settlementAggregateJob) — 완료 시 settlementReportJob 트리거",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "settlement"],
    params={"target_date": ""},
) as dag:

    run_settlement_aggregate = KubernetesPodOperator(
        task_id="run_settlement_aggregate",
        name="settlement-aggregate-batch",
        **batch_pod_kwargs(
            group="job-settlement",
            job_name=JOB_NAME,
            extra_args=["targetDate={{ params.target_date }}"],
        ),
    )

    trigger_settlement_report = TriggerDagRunOperator(
        task_id="trigger_settlement_report",
        trigger_dag_id="settlementReportJob",
        conf={"target_date": "{{ params.target_date }}"},
        wait_for_completion=False,
    )

    run_settlement_aggregate >> trigger_settlement_report
