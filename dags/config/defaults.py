"""
KubernetesPodOperator 공통 kwargs 팩토리.

배치 실행 DAG(order_sync, daily_ad_stats, ads_csv_export)가
공통으로 쓰는 namespace/리소스/시크릿/환경변수를 한 곳에서 관리한다.
"""

from airflow.providers.cncf.kubernetes.secret import Secret

from config.images import IMAGES

# MSSQL_SA_PASSWORD 는 k8s Secret(mssql-secret)의 SA_PASSWORD 키에서 env로 주입한다.
MSSQL_SECRET = Secret("env", "MSSQL_SA_PASSWORD", "mssql-secret", "SA_PASSWORD")


def batch_pod_kwargs(group: str, job_name: str, extra_args: list[str] | None = None) -> dict:
    """
    KubernetesPodOperator(**kwargs)에 그대로 펼쳐 넣을 공통 인자 dict를 만든다.

    :param group: config.images.IMAGES 의 키 (예: "job-order", "job-ads")
    :param job_name: Spring Batch job 이름. SPRING_BATCH_JOB_NAME env로 앱에 전달된다.
    :param extra_args: KPO arguments 리스트에 추가할 항목
                        (예: ["targetDate={{ params.target_date }}"])
                        주의: Spring Boot 3의 JobLauncherApplicationRunner는 non-option 인자
                        (name=value)만 JobParameter로 변환한다. "--name=value" 형식은 무시되므로
                        job parameter는 반드시 "name=value" 로 넘길 것.
    """
    return {
        "namespace": "batch",
        "image": IMAGES[group],
        "image_pull_policy": "IfNotPresent",
        "get_logs": True,
        "on_finish_action": "delete_succeeded_pod",
        "startup_timeout_seconds": 300,
        "env_vars": {
            "SPRING_PROFILES_ACTIVE": "k8s",
            "SPRING_BATCH_JOB_NAME": job_name,
        },
        "secrets": [MSSQL_SECRET],
        "container_resources": {
            "requests": {"cpu": "250m", "memory": "512Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"},
        },
        "arguments": extra_args or [],
    }
