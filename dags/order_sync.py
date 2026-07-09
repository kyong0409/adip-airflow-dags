"""
주문 동기화 배치 파이프라인.

preflight(echo) → run_order_sync(KPO, orderSyncJob) → run_order_export(KPO, orderCsvExportJob)

같은 이미지(job-order) 안에 Job이 2개 있고, --spring.batch.job.name 인자만 바꿔 KPO task를
하나 더 만드는 것으로 두 번째 Job을 실행한다 — "이미지 1개 × jobName 인자 = Job N개" 규약의 실증이다.

dag_id는 Spring Batch job 이름(orderSyncJob)과 정확히 동일해야 한다 — dscore.fw-be의
AirflowDagAdapter가 jobNm을 그대로 dag_id로 사용해 POST /api/v2/dags/{dag_id}/dagRuns 를 호출한다.
run_order_sync가 실패(skip 초과)하면 UI에서 이 dag_id로 재실행하는 것이 시연의 핵심 사이클이며,
그 뒤를 이어 run_order_export가 같은 targetDate로 CSV를 뽑아 Blob에 업로드한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.bash import BashOperator

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
    description="주문 동기화 배치 파이프라인 (preflight -> orderSyncJob -> orderCsvExportJob)",
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["adip", "order"],
    # skip_limit 은 orderSyncJob 의 BatchSkipPolicy 한도(JobParameter, 앱 기본 3)로 전달된다.
    # 트리거 다이얼로그에서 올려(예: 10) 실행하면 불량 5건도 한도 내가 되어 1차에 성공한다 —
    # "재실행 시 파라미터 조정" 시연 포인트. 빈 값이면 앱 쪽 parseInt 가 깨지므로 기본 "3" 유지.
    params={"target_date": "", "skip_limit": "3"},
) as dag:

    # ── 기본 executor(Celery)에서 실행. KubernetesExecutor가 기본이었다면 이 echo
    #    하나에도 Pod가 뜬다 — 멀티 executor에서 기본을 Celery로 두는 이유.
    preflight = BashOperator(
        task_id="preflight",
        bash_command='echo "host=$(hostname) date=$(date -Is)"',
    )

    run_order_sync = KubernetesPodOperator(
        task_id="run_order_sync",
        name="order-sync-batch",
        **batch_pod_kwargs(
            group="job-order",
            job_name=JOB_NAME,
            job_params={
                "targetDate": "{{ params.target_date }}",
                "skipLimit": "{{ params.skip_limit }}",
            },
        ),
    )

    # ── 같은 이미지, --spring.batch.job.name 만 orderCsvExportJob으로. KPO 모니터링 부하를 K8s로
    #    옮기는 멀티 executor 활용 예 (task 단위 executor 지정, AIP-61, Airflow 3.0 stable).
    run_order_export = KubernetesPodOperator(
        task_id="run_order_export",
        name="order-csv-export-batch",
        executor="KubernetesExecutor",
        **batch_pod_kwargs(
            group="job-order",
            job_name="orderCsvExportJob",
            job_params={"targetDate": "{{ params.target_date }}"},
        ),
    )

    preflight >> run_order_sync >> run_order_export
