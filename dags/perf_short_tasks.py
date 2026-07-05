"""
5초 sleep @task 20개를 동적 태스크 매핑(expand)으로 병렬 실행 — Executor 간 처리량/지연
비교 측정용 (LocalExecutor vs CeleryExecutor vs KubernetesExecutor).
"""

import time
from datetime import datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="perf_short_tasks",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "perf"],
    max_active_tasks=20,
) as dag:

    @task
    def sleep_task(i: int):
        time.sleep(5)
        return i

    sleep_task.expand(i=list(range(20)))
