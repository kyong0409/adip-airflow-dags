"""
Airflow 3 제어흐름 오퍼레이터 데모 — BranchPythonOperator / ShortCircuitOperator /
LatestOnlyOperator / EmptyOperator / TaskGroup.

실제 배치 잡과 무관한 시연 전용 DAG (dag_id=jobNm 규칙 예외).
"""

from datetime import datetime

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator


def _choose_branch(**context):
    # 시연용 고정 분기 — 필요 시 context["params"] 등을 기준으로 바꿀 수 있다.
    return "group_a.task_a1"


with DAG(
    dag_id="ops_showcase_flow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "ops-showcase"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(task_id="branch", python_callable=_choose_branch)

    with TaskGroup(group_id="group_a") as group_a:
        task_a1 = EmptyOperator(task_id="task_a1")
        task_a2 = EmptyOperator(task_id="task_a2")
        task_a1 >> task_a2

    with TaskGroup(group_id="group_b") as group_b:
        task_b1 = EmptyOperator(task_id="task_b1")

    short_circuit = ShortCircuitOperator(task_id="short_circuit", python_callable=lambda: True)

    latest_only = LatestOnlyOperator(task_id="latest_only")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> branch >> [group_a, group_b] >> end
    start >> short_circuit >> latest_only >> end
