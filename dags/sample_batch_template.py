"""
[템플릿] Airflow를 배치 스케줄러로 쓸 때의 표준 DAG 샘플.

시연 DAG(order_sync.py)가 batch_pod_kwargs 팩토리로 파라미터를 숨겨 두었다면,
이 파일은 반대로 **모든 파라미터를 풀어서 주석으로 설명**하는 학습/복붙용 템플릿이다.
새 배치 DAG를 만들 때 이 파일을 복사한 뒤 필요 없는 주석을 지우고 시작하면 된다.

포함된 패턴 3가지:
  ① @task                                  — executor 미지정 → 환경 기본 executor(Celery)에서 실행
  ② @task(executor="KubernetesExecutor")   — 데코레이터로 task별 executor 지정 + pod_override 리소스
  ③ KubernetesPodOperator                  — Spring Batch job 실행 (임의 이미지 Pod, Airflow 불필요)

KPO 블록에는 AKS 전환 시 필요한 옵션(ACR pull secret, Workload Identity 라벨/SA,
Key Vault Secrets Store CSI 마운트, Azure Files PVC 파일 마운트)을 주석으로 함께 담았다 —
로컬(minikube) 시연에서는 주석 상태 그대로, AKS 배포 시 주석을 풀어 값만 바꾼다.

전제:
  - 클러스터 executor 설정은 "CeleryExecutor,KubernetesExecutor" (values-multi.yaml).
    리스트 첫 번째가 환경 기본값이므로 executor를 지정하지 않은 task는 전부 Celery에서 돈다.
    → DAG 레벨 default_args 로 executor 를 또 지정할 필요 없다. (특정 DAG 전체를 K8s로
      보내고 싶을 때만 default_args={"executor": "KubernetesExecutor"} 사용)
  - executor 리스트에 없는 executor 를 task에 지정하면 DAG 파싱이 실패한다.
  - queue="kubernetes" 방식 라우팅은 구(舊) CeleryKubernetesExecutor 방식으로 3.0에서
    제거됐다. 3.x에서는 반드시 executor= 파라미터를 쓴다.

이 템플릿은 orderSyncJob(샘플 Job 이름임)을 빌려 쓰지만 시연 연동 대상이 아니므로 dag_id를 별도로 둔다.
주의: 실전 배치 DAG의 dag_id는 Spring Batch job 이름(jobNm)과 정확히 일치해야 한다
(dscore.fw-be AirflowDagAdapter가 jobNm을 dag_id로 사용해 재실행 API를 호출)
"""

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret
from airflow.sdk import task  # Airflow 3.x 공식 authoring API (airflow.decorators.task 와 동일)
from kubernetes.client import models as k8s

from config.images import IMAGES

# ── 이미지: config/images.py 중앙관리 dict 에서 가져온다 (CI가 태그만 갱신하는 규약).
IMAGE = IMAGES["job-order"]

# ── 민감정보는 k8s Secret 으로 주입한다. env_vars 평문 금지(kubectl describe pod 로 노출됨).
#    Secret(주입방식, 컨테이너 env 이름, k8s Secret 이름, Secret 안의 키)
MSSQL_SECRET = Secret("env", "MSSQL_SA_PASSWORD", "mssql-secret", "SA_PASSWORD")
# 변형: Secret("env", None, "batch-db-secret")            → Secret의 모든 키를 env로
#       Secret("volume", "/etc/secrets", "batch-db-secret") → 파일로 마운트

default_args = {
    "owner": "adip",
    "depends_on_past": False,  # 이전 스케줄 실행 결과와 무관하게 실행
    # tz-aware start_date → 아래 cron 스케줄이 KST 기준으로 해석된다 (naive datetime 은 UTC)
    "start_date": pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    "retries": 1,  # KPO 재시도 = Pod 새로 생성. 앱이 UniqueRunIdIncrementer 를 내장하므로
    #                동일 파라미터 재시도에도 JobInstanceAlreadyCompleteException 이 없다.
    #                (incrementer 없는 앱이면 arguments 에 run.id="{{ run_id }}" 를 넣어 회피)
    "retry_delay": timedelta(minutes=3),
    # "executor": "KubernetesExecutor",  # ← DAG 전체 executor 지정은 여기서. 생략 = 기본(Celery)
}

with DAG(
    dag_id="sample_batch_template",
    default_args=default_args,
    description="[템플릿] task별 executor 지정 + KPO Spring Batch 실행 표준 샘플",
    # 스케줄: 배치 스케줄러로 쓸 때의 핵심. cron 식 외에 "@daily", timedelta(hours=6),
    #         None(수동/API 트리거 전용), Asset 스케줄(이벤트 기반)도 가능.
    schedule="0 2 * * *",  # 매일 02:00 (Asia/Seoul 로 하려면 start_date 에 tz 지정 or 기본 UTC 유의)
    catchup=False,  # start_date~현재 사이 밀린 스케줄을 소급 실행하지 않음 (배치는 대부분 False)
    max_active_runs=1,  # 동일 DAG run 동시 실행 방지 — 같은 데이터를 두 번 처리하지 않도록
    dagrun_timeout=timedelta(hours=2),  # run 전체 제한시간. 초과 시 실패 처리
    tags=["adip", "sample", "template"],
    # is_paused_upon_creation 기본값(True) 유지 — 템플릿이 스케줄대로 실제 돌지 않도록.
    # 시연 DAG들은 재실행 데모를 위해 False 로 둔다.
    #
    # params: Trigger UI 다이얼로그 / REST API conf 로 실행 시점에 덮어쓸 수 있는 값.
    #         "평소엔 스케줄 기본값, 필요할 때만 조정해 수동 실행" 패턴의 입구.
    params={
        "target_date": "",   # 빈 값이면 아래 resolve_target_date 가 논리적 실행일로 대체
        "skip_limit": "3",   # orderSyncJob 의 BatchSkipPolicy 한도 (JobParameter 로 전달)
    },
    doc_md=__doc__,  # 이 docstring 이 UI의 DAG Docs 탭에 그대로 노출된다
) as dag:

    # ─────────────────────────────────────────────────────────────────────
    # ① 가벼운 준비 task — executor 미지정 → 환경 기본값(Celery 워커)에서 즉시 실행.
    #    Pod 기동 지연(수십 초)이 없어 이런 초 단위 작업에 적합하다.
    #    반환값은 XCom 으로 저장되어 다음 task 가 가져다 쓴다.
    # ─────────────────────────────────────────────────────────────────────
    @task
    def resolve_target_date(params: dict, ds_nodash: str) -> str:
        """트리거 파라미터가 있으면 그 값, 없으면 논리적 실행일(YYYYMMDD)을 대상일로 확정."""
        return params.get("target_date") or ds_nodash

    # ─────────────────────────────────────────────────────────────────────
    # ② 무겁거나 격리가 필요한 "Airflow 파이썬" task — 데코레이터에서 executor 지정.
    #    KubernetesExecutor 는 이 task 하나를 위해 Airflow 워커 Pod 를 새로 띄운다.
    #    (KPO 와 다른 점: 여기서 뜨는 Pod 는 Airflow 이미지여야 하고 task 코드도 파이썬)
    #    리소스/노드배치는 executor_config 의 pod_override 로 조정한다.
    # ─────────────────────────────────────────────────────────────────────
    @task(
        executor="KubernetesExecutor",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",  # 워커 Pod 의 메인 컨테이너 이름은 "base" 고정
                            resources=k8s.V1ResourceRequirements(
                                requests={"cpu": "500m", "memory": "1Gi"},
                                limits={"cpu": "1", "memory": "2Gi"},
                            ),
                        )
                    ],
                    node_selector={"agentpool": "batch"},  # 배치 전용 노드풀로 보낼 때
                    tolerations=[
                        k8s.V1Toleration(
                            key="batch",
                            operator="Exists",
                            effect="NoSchedule"
                        )
                    ],
                )
            )
        },
    )
    def heavy_precheck(target_date: str) -> str:
        """대상일 형식 검증 등 무거운 사전 점검을 격리 Pod 에서 수행했다고 가정."""
        datetime.strptime(target_date, "%Y%m%d")  # 형식이 틀리면 여기서 task 실패
        return target_date

    # ─────────────────────────────────────────────────────────────────────
    # ③ Spring Batch job 실행 — KubernetesPodOperator(KPO).
    #    배치 앱 이미지를 그대로 Pod 로 띄우므로 워커에 JDK/앱이 없어도 된다.
    #    같은 이미지에 job 이 여러 개면 --spring.batch.job.name 만 바꾼 KPO 를 추가한다.
    # ─────────────────────────────────────────────────────────────────────
    run_order_sync = KubernetesPodOperator(
        task_id="run_order_sync",
        name="sample-order-sync",  # Pod 이름 접두어 (미지정 시 task_id 사용, 랜덤 접미사 자동)
        # executor="KubernetesExecutor",  # ← KPO 자체는 어차피 별도 Pod 를 띄우므로 지정은 선택.
        #                                    미지정 = Celery 워커가 Pod 를 만들고 모니터링.
        #                                    모니터링 부하까지 K8s 로 옮길 때만 지정한다.

        # ── 실행 위치/이미지 ────────────────────────────────────────────
        namespace="batch",
        image=IMAGE,  # AKS 에서는 ACR 경로 (예: "myacr.azurecr.io/adip/job-order:0.1.0")
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[
            k8s.V1LocalObjectReference("acr-pull-secret")
        ],
        #     ↑ ACR 인증 방식 1: docker-registry Secret. AKS-ACR attach(kubelet MI 권한)를
        #       걸었다면 pull secret 없이 당겨지므로 이 줄 자체가 불필요하다.

        # ── AKS Workload Identity (Pod 가 Azure 리소스에 UMI 로 인증할 때) ──
        # 배치 앱이 Key Vault/Blob 등에 직접 접근한다면 아래 3종 세트를 함께 켠다:
        #   ⑴ federated credential 이 연결된 UMI, ⑵ 그 UMI 를 annotation 으로 가진 SA,
        #   ⑶ Pod 라벨 azure.workload.identity/use=true (이 라벨이 있어야 WI 웹훅이
        #      토큰 볼륨/env(AZURE_CLIENT_ID 등)를 주입한다)
        service_account_name="batch-runner",
        labels={
            "azure.workload.identity/use": "true",
            "app": "order-sync",  # 그 외 라벨은 자유 (모니터링/네트워크폴리시 셀렉터용)
        },

        # ── 무엇을 실행할지: cmds / arguments ──────────────────────────
        # cmds 생략 → 이미지 ENTRYPOINT(["java","-jar","/app/app.jar"]) 사용.
        # ENTRYPOINT 가 없는 이미지라면 cmds=["java", "-jar", "/app/app.jar"] 로 명시.
        arguments=[
            # [Job 선택] option 인자(--) → Spring Environment 프로퍼티. Boot 3.x 는 단수(name)!
            "--spring.batch.job.name=orderSyncJob",
            # [JobParameter] 대시 없는 name=value 만 JobParameter 가 된다.
            #                --key=value 형식은 Environment 로만 가고 job 에는 전달되지 않는다.
            "targetDate={{ ti.xcom_pull(task_ids='heavy_precheck') }}",  # ①→② 를 거친 확정값
            "skipLimit={{ params.skip_limit }}",                         # Trigger UI 에서 조정 가능
            # "run.id={{ run_id }}",  # incrementer 없는 앱의 동일 파라미터 재실행 회피용 (앞 주석 참고)
        ],

        # ── 환경 컨텍스트 / 민감정보 ──────────────────────────────────
        # env: 여러 job 이 공유하는 값(프로파일·TZ·JVM 옵션). job 선택/비즈니스 파라미터와 구분할 것.
        env_vars={
            "SPRING_PROFILES_ACTIVE": "k8s",
            "TZ": "Asia/Seoul",
            "JAVA_TOOL_OPTIONS": "-Xms256m -Xmx768m",  # limits.memory 이내로
        },
        secrets=[MSSQL_SECRET],
        env_from=[
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(name="batch-common-config")
            )
        ],  # ConfigMap/Secret 통째로 주입할 때

        # ── 리소스 / 노드 배치 ────────────────────────────────────────
        # 반드시 container_resources=V1ResourceRequirements. 구 resources=dict 는 provider 6.0 에서 제거됨.
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        node_selector={"agentpool": "batch"},
        tolerations=[
            k8s.V1Toleration(
                key="batch",
                operator="Exists",
                effect="NoSchedule"
            )
        ],

        # ── 파일 / 시크릿 마운트 (volumes + volume_mounts 는 짝으로) ──────
        # ① Key Vault 시크릿을 파일로: Secrets Store CSI driver (AKS addon
        #    azureKeyvaultSecretsProvider 활성 + SecretProviderClass 리소스 필요.
        #    Workload Identity 조합 시 위 labels/SA 도 함께 켤 것).
        #    Spring 에서 읽는 법: 파일을 직접 읽거나, env 로
        #    SPRING_CONFIG_IMPORT=optional:configtree:/mnt/secrets-store/ 를 주면
        #    파일명이 그대로 프로퍼티 키가 된다.
        # ② 배치 입출력 파일 공유: Azure Files(SMB/NFS) PVC 마운트.
        volumes=[
            k8s.V1Volume(  # ① Key Vault → 파일
                name="secrets-store",
                csi=k8s.V1CSIVolumeSource(
                    driver="secrets-store.csi.k8s.io",
                    read_only=True,
                    volume_attributes={"secretProviderClass": "batch-kv-provider"},
                ),
            ),
            k8s.V1Volume(  # ② 배치 입출력 파일 (PVC 는 배치 네임스페이스에 미리 생성)
                name="batch-data",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="batch-data-pvc",
                ),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="secrets-store",
                mount_path="/mnt/secrets-store",
                read_only=True
            ),
            k8s.V1VolumeMount(
                name="batch-data",
                mount_path="/data"
            ),
            # sub_path="order" 를 주면 PVC 안의 특정 하위 디렉토리만 마운트
        ],

        # ── 운영 파라미터 ─────────────────────────────────────────────
        get_logs=True,  # 컨테이너 stdout 을 Airflow task 로그로 스트리밍 (배치 로그를 UI 에서 봄)
        on_finish_action="delete_succeeded_pod",  # 성공 Pod 만 삭제, 실패 Pod 는 남겨 kubectl 디버깅
        #                                           운영 안정화 후엔 "delete_pod" 로 전환 고려.
        #                                           (구 is_delete_operator_pod=True 는 deprecated — 이걸로 대체)
        startup_timeout_seconds=300,  # 이미지 pull + 스케줄링 대기 한도 (기본 120초는 첫 pull 에 짧을 수 있음)
        in_cluster=True,            # (기본) Airflow 가 도는 클러스터 안에 Pod 생성.
        #                               다른 클러스터로 보내려면 in_cluster=False + kubernetes_conn_id=
        reattach_on_restart=True,   # (기본) 스케줄러 재시작 시 돌던 Pod 에 재부착 — 배치 중복 실행 방지
        deferrable=True,            # Pod 완료 대기를 triggerer 로 넘겨 워커 슬롯 절약 (장시간 배치에 유용)
        # do_xcom_push=True,          # 컨테이너가 /airflow/xcom/return.json 을 쓰는 경우에만 의미 있음
    )

    # ── 의존성: ① 대상일 확정 → ② 격리 Pod 사전점검 → ③ Spring Batch 실행
    #    (TaskFlow 함수는 호출로 값을 넘기면 의존성이 자동 연결되고,
    #     KPO 는 xcom_pull 템플릿만으로는 의존성이 생기지 않으므로 >> 로 명시한다)
    run_order_sync << heavy_precheck(resolve_target_date())
