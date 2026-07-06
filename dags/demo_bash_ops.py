"""
BashOperator 데모 — Executor 선택(KubernetesExecutor vs CeleryExecutor) 판단 근거 실측용.
4케이스 매트릭스(계획서 §5)의 ①(E1×Bash) / ③(E2×Bash) 케이스를 담당한다.

같은 DAG를 Executor만 바꿔(07_switch_executor.sh) 두 번 실행해 비교한다:
- E1 KubernetesExecutor: 태스크마다 워커 Pod가 새로 생성된다 — sleep 5초짜리에도
  Pod 기동이 수십 초. `kubectl get pods -n airflow -w` 로 관찰.
- E2 CeleryExecutor: 상주 워커 안에서 즉시 실행. flower(port-forward 5555)로 큐/워커 관찰.

측정 항목(계획서 §5.4 기록표): 첫 태스크 기동 지연, sleep5×20 총 소요.

말미의 java_runtime_check 태스크는 **의도적으로 실패**한다 — Bash 태스크는 Airflow 워커
이미지 안에서 실행되므로 JDK 같은 배치 런타임이 없다. "배치 본체는 KPO(자기 이미지)로
격리해야 하는 이유"의 실증이며, 이 태스크의 실패는 데모 포인트이지 오류가 아니다.
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

SLEEP_TASK_COUNT = 20

with DAG(
    dag_id="demo_bash_ops",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "demo", "executor-matrix"],
) as dag:

    # ── 전처리성 셸 작업: "이 정도 가벼운 일은 Bash면 충분"의 예시.
    #    단 E1에서는 이 한 줄짜리 echo에도 Pod가 하나 뜬다는 것이 관찰 포인트.
    preflight = BashOperator(
        task_id="preflight",
        bash_command=(
            'echo "host=$(hostname) date=$(date -Is)"; '
            'test -d "${AIRFLOW_HOME:-/opt/airflow}/dags" '
            '&& echo "dags dir ok: $(ls ${AIRFLOW_HOME:-/opt/airflow}/dags | wc -l) entries"'
        ),
    )

    # ── 측정용 부하: sleep 5초 × 20개 병렬 (동적 태스크 매핑).
    #    E1: 20개 Pod 기동 오버헤드 누적 / E2: 워커 슬롯만큼 즉시 병렬.
    sleep_tasks = BashOperator.partial(
        task_id="sleep_5s",
    ).expand(
        bash_command=[
            f'echo "task {i} start on $(hostname)"; sleep 5' for i in range(SLEEP_TASK_COUNT)
        ]
    )

    # ── 의도된 실패: 워커 이미지에는 배치 런타임(JDK)이 없다.
    #    Bash로 배치 본체를 돌리려면 워커 이미지에 런타임을 다 넣어야 하고
    #    리소스 격리도 안 된다 → KPO를 쓰는 이유. (계획서 §5.2 ⑶)
    java_runtime_check = BashOperator(
        task_id="java_runtime_check",
        bash_command="java -version",
    )

    preflight >> sleep_tasks >> java_runtime_check
