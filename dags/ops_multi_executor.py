"""
멀티 Executor 데모 — 가벼운 @task 3개(기본 executor에서 실행)와 executor="KubernetesExecutor"
를 지정한 @task 1개 + KubernetesPodOperator 1개를 섞어서 보여준다.

주의: executor kwarg는 Airflow 3에서 모든 오퍼레이터/@task에 항상 문법적으로 허용된다.
다만 이 값이 실제로 유효하려면 airflow.cfg [core] executor 설정이
"LocalExecutor,KubernetesExecutor" 처럼 멀티 executor 구성(values-multi.yaml 등)이어야 한다.
단일 executor 환경에서는 이 DAG의 파싱/로드는 문제없이 성공하지만, executor="KubernetesExecutor"
가 붙은 태스크를 실제로 큐잉하는 시점에 스케줄러가 해당 executor를 찾지 못해 에러가 난다.
데모 클러스터가 멀티 executor로 구성되어 있을 때만 정상 동작함을 알고 사용할 것.
"""

from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(
    dag_id="ops_multi_executor",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "ops-showcase"],
) as dag:

    @task
    def light_task_1():
        return "light-1 done"

    @task
    def light_task_2():
        return "light-2 done"

    @task
    def light_task_3():
        return "light-3 done"

    @task(executor="KubernetesExecutor")
    def heavy_task_on_kubernetes():
        return "heavy task ran on KubernetesExecutor"

    run_kpo_on_kubernetes = KubernetesPodOperator(
        task_id="run_kpo_on_kubernetes",
        name="ops-multi-executor-pod",
        namespace="batch",
        image="python:3.11-slim",
        cmds=["python", "-c"],
        arguments=["print('kpo task running via KubernetesExecutor-capable pool')"],
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        startup_timeout_seconds=300,
        executor="KubernetesExecutor",
    )

    t1 = light_task_1()
    t2 = light_task_2()
    t3 = light_task_3()
    heavy = heavy_task_on_kubernetes()

    [t1, t2, t3] >> heavy >> run_kpo_on_kubernetes
