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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="sample_batch_template_clean",
    default_args=default_args,
    description="task별 executor 지정 + KPO Spring Batch 실행 표준 샘플 (주석 제거판)",
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["adip", "sample", "template"],
    params={
        "target_date": "",
        "skip_limit": "3",
    },
) as dag:

    @task
    def resolve_target_date(params: dict, ds_nodash: str) -> str:
        return params.get("target_date") or ds_nodash

    @task(
        executor="KubernetesExecutor",
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={"cpu": "500m", "memory": "1Gi"},
                                limits={"cpu": "1", "memory": "2Gi"},
                            ),
                        )
                    ],
                    node_selector={"agentpool": "batch"},
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
        datetime.strptime(target_date, "%Y%m%d")
        return target_date

    run_order_sync = KubernetesPodOperator(
        task_id="run_order_sync",
        name="sample-order-sync",
        namespace="batch",
        image=IMAGE,
        image_pull_policy="IfNotPresent",
        image_pull_secrets=[
            k8s.V1LocalObjectReference("acr-pull-secret")
        ],
        service_account_name="batch-runner",
        labels={
            "azure.workload.identity/use": "true",
            "app": "order-sync",
        },
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
        env_from=[
            k8s.V1EnvFromSource(
                config_map_ref=k8s.V1ConfigMapEnvSource(name="batch-common-config")
            )
        ],
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
        volumes=[
            k8s.V1Volume(
                name="secrets-store",
                csi=k8s.V1CSIVolumeSource(
                    driver="secrets-store.csi.k8s.io",
                    read_only=True,
                    volume_attributes={"secretProviderClass": "batch-kv-provider"},
                ),
            ),
            k8s.V1Volume(
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
        ],
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        startup_timeout_seconds=300,
        in_cluster=True,
        reattach_on_restart=True,
        deferrable=True,
    )

    run_order_sync << heavy_precheck(resolve_target_date())
