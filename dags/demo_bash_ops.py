"""
멀티 Executor 비교 시연 — 단일 환경(default Celery)에서 태스크별 executor 지정만으로
같은 부하를 나란히 실행해 Pod 기동 지연 차이를 한 Grid 화면에서 비교한다.

preflight (executor 미지정 = 기본 Celery)
   ├─ sleep_5s_celery ×10 (expand, executor 미지정)               ── 상주 워커에서 즉시 병렬
   └─ sleep_5s_k8s    ×10 (expand, executor="KubernetesExecutor") ── 태스크마다 Pod 기동
   → (둘 다 성공 후) java_runtime_check

과거에는 07_switch_executor.sh로 클러스터의 executor 자체를 전환해 같은 DAG를 두 번
돌려 비교했다. Airflow 3.0에서 stable이 된 AIP-61(Using Multiple Executors Concurrently)
덕분에, 이제는 환경 전환 없이 task 단위 executor= 파라미터 지정만으로 같은 화면에서 비교할
수 있다. queue='kubernetes' 라우팅은 3.0에서 제거된 구(CeleryKubernetesExecutor) 방식이므로
사용하지 않는다 — executor= 파라미터가 정식 방식이다.

말미의 java_runtime_check 태스크는 **의도적으로 실패**한다 — Bash 태스크는 Airflow 워커
이미지 안에서 실행되므로 JDK 같은 배치 런타임이 없다. "배치 본체는 KPO(자기 이미지)로
격리해야 하는 이유"의 실증이며, 이 태스크의 실패는 데모 포인트이지 오류가 아니다.
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

SLEEP_TASK_COUNT = 10

with DAG(
    dag_id="demo_bash_ops",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "demo", "multi-executor"],
) as dag:

    # ── 전처리성 셸 작업: executor 미지정 → 기본(Celery)에서 실행.
    preflight = BashOperator(
        task_id="preflight",
        bash_command=(
            'echo "host=$(hostname) date=$(date -Is)"; '
            'test -d "${AIRFLOW_HOME:-/opt/airflow}/dags" '
            '&& echo "dags dir ok: $(ls ${AIRFLOW_HOME:-/opt/airflow}/dags | wc -l) entries"'
        ),
    )

    # ── executor 미지정 → DAG/환경 기본값(Celery) 사용. 상주 워커에서 즉시 병렬 실행.
    sleep_tasks_celery = BashOperator.partial(
        task_id="sleep_5s_celery",
    ).expand(
        bash_command=[
            f'echo "celery task {i} start on $(hostname)"; sleep 5'
            for i in range(SLEEP_TASK_COUNT)
        ]
    )

    # ── executor="KubernetesExecutor" → 태스크마다 워커 Pod가 새로 뜬다.
    #    같은 sleep 5초인데 Pod 기동 지연만큼 완료 시각이 늦어지는 것이 관찰 포인트.
    sleep_tasks_k8s = BashOperator.partial(
        task_id="sleep_5s_k8s",
        executor="KubernetesExecutor",
    ).expand(
        bash_command=[
            f'echo "k8s task {i} start on $(hostname)"; sleep 5'
            for i in range(SLEEP_TASK_COUNT)
        ]
    )

    # ── 의도된 실패: 워커 이미지에는 배치 런타임(JDK)이 없다.
    #    Bash로 배치 본체를 돌리려면 워커 이미지에 런타임을 다 넣어야 하고
    #    리소스 격리도 안 된다 → KPO를 쓰는 이유.
    java_runtime_check = BashOperator(
        task_id="java_runtime_check",
        bash_command="java -version",
    )

    preflight >> [sleep_tasks_celery, sleep_tasks_k8s] >> java_runtime_check
