"""
@task.kubernetes 데모 — KubernetesPodOperator를 TaskFlow 스타일로 사용하는
KPO 실행 방식 ②. (방식 ①은 다른 배치 DAG들이 쓰는 KubernetesPodOperator 직접 사용)

Python 함수를 그대로 직렬화해 Pod 안에서 실행하고 싶은 경량 유틸/전처리 작업에 적합하다.
"""

from datetime import datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="ops_kpo_taskflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "ops-showcase"],
) as dag:

    @task.kubernetes(
        image="python:3.11-slim",
        namespace="batch",
        name="ops-kpo-taskflow-pod",
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        startup_timeout_seconds=300,
        in_cluster=True,
    )
    def run_in_pod():
        print("Hello from inside a KubernetesPodOperator pod, via @task.kubernetes")

    run_in_pod()
