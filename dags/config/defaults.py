"""
KubernetesPodOperator(KPO) Pod 실행 최소 파라미터 팩토리.

개발자가 Spring Batch 배치를 KPO로 실행할 때 "무엇을 어디로 넘겨야 하는지" 판단하는
6개의 주입 축을 함수 시그니처 자체로 드러낸다:

  ① group      — 이미지 선택. config/images.py 의 키 (= 배치 그룹 = 모듈)
  ② job_name   — Job 지정. arguments 의 option 인자 "--spring.batch.job.name=<job_name>" 으로 전달
  ③ job_params — 비즈니스 파라미터(JobParameter). arguments 의 non-option "name=value" 인자로 전달
  ④ env        — 환경 컨텍스트(프로파일 등). 기본 env_vars 에 병합
  ⑤ secrets    — 민감정보. k8s Secret 선언 (기본: MSSQL_SECRET)
  ⑥ resources  — Pod별 CPU/메모리. 배치마다 달라질 수 있어 파라미터화 (기본값: 기존과 동일)

env vs arguments 판단 기준:
  - Job 선택       → arguments 의 option 인자 "--spring.batch.job.name=<job>"
                     (env "SPRING_BATCH_JOB_NAME=<job>" 방식도 relaxed binding으로 동일하게
                     동작하지만, 이 저장소는 "무슨 Job인지 DAG 코드에서 바로 보이도록"
                     arguments 방식을 표준으로 삼는다)
  - 비즈니스 파라미터 → non-option "name=value" 인자
                     (Spring Boot의 JobLauncherApplicationRunner는 대시 없는 인자만
                     JobParameter로 변환한다. "--name=value" 형식은 무시된다)
  - 환경/공통 설정   → env_vars (여러 Job이 공유하는 프로파일·TZ 등)
  - 민감정보        → k8s Secret 선언 (env_vars 평문 금지)
  - 리소스         → 배치별 파라미터 (resources)

확장 예시(주석):
  - 유니크 실행 파라미터가 필요한 배치(= RunIdIncrementer 를 쓰지 않는 배치)는 job_params 에
    run.id="{{ run_id }}" 같은 Airflow 템플릿 값을 추가하면 JobInstanceAlreadyCompleteException
    을 회피할 수 있다. 이 저장소의 배치 앱은 UniqueRunIdIncrementer 를 내장하므로 불필요.
  - 파일/디렉토리를 Pod에 마운트해야 하면 반환된 kwargs 에 volumes/volume_mounts 를 추가로
    병합하면 된다 (이 저장소는 Blob 업로드(orderCsvExportJob)로 대체하므로 별도 예제는 두지 않는다).
"""

from airflow.providers.cncf.kubernetes.secret import Secret

from config.images import IMAGES

# MSSQL_SA_PASSWORD 는 k8s Secret(mssql-secret)의 SA_PASSWORD 키에서 env로 주입한다.
MSSQL_SECRET = Secret("env", "MSSQL_SA_PASSWORD", "mssql-secret", "SA_PASSWORD")

_DEFAULT_RESOURCES = {
    "requests": {"cpu": "250m", "memory": "512Mi"},
    "limits": {"cpu": "1", "memory": "1Gi"},
}


def batch_pod_kwargs(
    group: str,
    job_name: str,
    job_params: dict | None = None,
    env: dict | None = None,
    secrets: list | None = None,
    resources: dict | None = None,
) -> dict:
    """
    KubernetesPodOperator(**kwargs)에 그대로 펼쳐 넣을 Pod 실행 kwargs를 만든다.
    6개 주입 축(①~⑥)과 env vs arguments 판단 기준은 모듈 docstring 참고.
    """
    arguments = [f"--spring.batch.job.name={job_name}"]
    arguments += [f"{k}={v}" for k, v in (job_params or {}).items()]

    env_vars = {"SPRING_PROFILES_ACTIVE": "k8s"}
    env_vars.update(env or {})

    return {
        "namespace": "batch",
        "image": IMAGES[group],
        "image_pull_policy": "IfNotPresent",
        "get_logs": True,
        "on_finish_action": "delete_succeeded_pod",
        "startup_timeout_seconds": 300,
        "env_vars": env_vars,
        "secrets": secrets if secrets is not None else [MSSQL_SECRET],
        "container_resources": resources or _DEFAULT_RESOURCES,
        "arguments": arguments,
    }
