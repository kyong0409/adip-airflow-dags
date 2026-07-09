"""
sample_batch_template 을 PoC(minikube) 환경에서 실제 실행되도록 조정한 DAG.

구조는 템플릿과 동일한 3패턴(①@task Celery → ②@task KubernetesExecutor → ③KPO orderSyncJob)이되,
PoC 클러스터에 없는 AKS 전용 리소스(ACR pull secret, Workload Identity SA/라벨,
Key Vault CSI·PVC 마운트, agentpool 노드풀, batch-common-config ConfigMap)는 제거했다.
AKS 배포 시 되살릴 옵션은 sample_batch_template.py 를 본다.

orderSyncJob 은 시연 데이터에 불량 5건이 심어져 있어 skipLimit=3 이면 1차 실패한다 —
이 DAG 는 템플릿 검증용이므로 기본 skip_limit 을 10 으로 두어 바로 성공하게 한다.
(의도적 실패 → UI 재실행 사이클 시연은 기존 orderSyncJob DAG 담당)
"""

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sdk import task
from kubernetes.client import models as k8s

from config.images import IMAGES

IMAGE = IMAGES["job-order"]

MSSQL_SECRET = Secret("env", "MSSQL_SA_PASSWORD", "mssql-secret", "SA_PASSWORD")

default_args = {
    "owner": "adip",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    "retries": 0,  # PoC 관찰용 — 실패 시 즉시 FAILED 로 확인
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="sample_batch_poc",
    default_args=default_args,
    description="템플릿 3패턴을 PoC(minikube) 환경에서 실행하는 검증용 DAG",
    schedule=None,  # PoC 는 수동 트리거로 실행 (스케줄 검증은 템플릿의 cron 예시 참고)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["adip", "sample", "poc"],
    is_paused_upon_creation=False,  # 시연 DAG 공통 규약 — git-sync 반영 즉시 트리거 가능
    params={
        "target_date": "",   # 빈 값이면 논리적 실행일(YYYYMMDD) 사용
        "skip_limit": "10",  # 시연 불량 5건 < 10 → 1차 성공. 실패 사이클을 보려면 3으로 트리거
    },
    doc_md=__doc__,
) as dag:

    # ① 기본 executor(Celery 워커)에서 즉시 실행 — Pod 기동 지연 없음
    @task
    def resolve_target_date(params: dict, ds_nodash: str) -> str:
        return params.get("target_date") or ds_nodash

    # ② task 단위 executor 지정 — 이 task 하나만 Airflow 워커 Pod 로 격리 실행.
    #    minikube 단일 노드라 node_selector/tolerations 없이 리소스만 지정한다.
    @task(
        executor="KubernetesExecutor",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={"cpu": "250m", "memory": "512Mi"},
                                limits={"cpu": "500m", "memory": "1Gi"},
                            ),
                        )
                    ],
                )
            )
        },
    )
    def heavy_precheck(target_date: str) -> str:
        datetime.strptime(target_date, "%Y%m%d")
        return target_date

    # ③ Spring Batch 실행 — PoC 에 존재하는 리소스만 사용 (batch ns, 로컬 이미지, mssql-secret)
    run_order_sync = KubernetesPodOperator(
        task_id="run_order_sync",
        name="sample-poc-order-sync",
        namespace="batch",
        image=IMAGE,
        image_pull_policy="IfNotPresent",  # minikube 내 로컬 빌드 이미지 사용
        arguments=[
            "--spring.batch.job.name=orderSyncJob",
            "targetDate={{ ti.xcom_pull(task_ids='heavy_precheck') }}",
            "skipLimit={{ params.skip_limit }}",
        ],
        env_vars={
            "SPRING_PROFILES_ACTIVE": "k8s",
            "TZ": "Asia/Seoul",
            "JAVA_TOOL_OPTIONS": "-Xms256m -Xmx768m",
        },
        secrets=[MSSQL_SECRET],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        get_logs=True,
        on_finish_action="keep_pod",
        startup_timeout_seconds=300,
        in_cluster=True,
        # deferrable=True,  # triggerer 는 떠 있으나(values-common.yaml) PoC 기본 경로 검증을
        #                     단순하게 유지하기 위해 비활성 — 워커 슬롯 절약 관찰 시 해제
    )

    run_order_sync << heavy_precheck(resolve_target_date())
