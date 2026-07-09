# adip-airflow-dags

Airflow 3.2.x 배치 시연용 DAG 모음. `dags/` 폴더가 git-sync 대상이며, Airflow 스케줄러/웹서버
파드가 이 저장소를 주기적으로 pull하여 DAG를 반영한다.

시연 범위(260709 회의 반영): DAG **2개**로 축소 — `demo_bash_ops`(멀티 Executor 비교),
`orderSyncJob`(KPO 파이프라인). 클러스터는 **멀티 Executor 단일 환경**
(`CeleryExecutor,KubernetesExecutor` — 첫 번째가 기본값 = Celery)으로 띄워 두고, **태스크
단위로 `executor=` 파라미터를 지정**해 같은 화면에서 비교한다(AIP-61 "Using Multiple
Executors Concurrently", Airflow 3.0에서 stable). 과거의 "환경 자체를 전환해 같은 DAG를 두 번
실행"하는 4케이스 매트릭스 방식은 폐기됐다.

## dag_id 규칙

**dag_id = Spring Batch job 이름(jobNm)** — dscore.fw-be의 `AirflowDagAdapter`가 재실행/트리거
시 `jobNm`을 그대로 `dag_id`로 사용해 `POST /api/v2/dags/{dag_id}/dagRuns`를 호출하기 때문이다.
배치 실행 DAG(`order_sync.py`)는 파일명과 `dag_id`가 다를 수 있어도, **`dag_id`는 반드시
대응하는 Spring Batch job 이름과 camelCase까지 정확히 일치**해야 한다. `orderSyncJob`은 이
연동에 묶여 있으므로 **절대 변경하지 않는다**.

데모 전용 DAG(`demo_*` 접두어)는 이 규칙의 예외로, Spring Batch job과 무관하므로 snake_case
dag_id를 그대로 사용한다.

## DAG 카탈로그

| dag_id | 파일 | 용도 | 구성 |
|---|---|---|---|
| `demo_bash_ops` | `demo_bash_ops.py` | 멀티 Executor 비교 시연 — 같은 sleep 부하를 Celery 태스크 vs K8s 태스크로 나란히 실행해 Pod 기동 지연을 비교. 말미 `java_runtime_check`는 **의도된 실패**(워커 이미지엔 JDK가 없다 → 배치 본체를 KPO로 격리하는 이유의 실증) | `preflight → [sleep_5s_celery×10, sleep_5s_k8s×10(executor=KubernetesExecutor)] → java_runtime_check` |
| `orderSyncJob` | `order_sync.py` | 주문 동기화 배치 파이프라인 — skip 초과로 **의도적 실패 → UI 재실행 사이클**의 주인공. 성공 후 같은 이미지에서 `--spring.batch.job.name`만 바꿔 CSV export까지 이어짐 | `preflight(echo) → run_order_sync(orderSyncJob) → run_order_export(orderCsvExportJob, executor=KubernetesExecutor)` |

KPO 배치 task는 필요 시크릿으로 k8s Secret(`mssql-secret`)을 사용한다.

## config/ 모듈

- `config/images.py` — 배치 앱 이미지 태그(`IMAGES` dict)를 중앙관리. **CI가 빌드/푸시 후 이
  파일 값만 갱신**하는 규약이다. 키(`job-order`)는 고정.
- `config/defaults.py` — `batch_pod_kwargs(group, job_name, job_params, env, secrets, resources)`
  헬퍼. "Pod 실행 최소 파라미터 샘플"이자 이 저장소의 핵심 산출물.

## KubernetesPodOperator 공통 규약 (batch_pod_kwargs 6축)

`config/defaults.py`의 `batch_pod_kwargs`가 Pod 실행에 필요한 6개 주입 축을 함수 시그니처로
드러낸다:

| 축 | 파라미터 | 전달 위치 | 비고 |
|---|---|---|---|
| ① 이미지 선택 | `group` | `image` | `config/images.py`의 키 (= 배치 그룹 = 모듈) |
| ② Job 지정 | `job_name` | `arguments`의 option 인자 `--spring.batch.job.name=<job_name>` | env `SPRING_BATCH_JOB_NAME=<job>`도 relaxed binding으로 동일하게 동작하지만, 이 저장소는 "무슨 Job인지 DAG 코드에서 바로 보이도록" arguments 방식을 표준으로 삼는다 |
| ③ 비즈니스 파라미터 | `job_params` | `arguments`의 non-option `name=value` | Spring Boot의 JobLauncherApplicationRunner는 대시 없는 인자만 JobParameter로 변환한다. `--name=value` 형식은 무시됨 |
| ④ 환경 컨텍스트 | `env` | `env_vars` (기본 `SPRING_PROFILES_ACTIVE=k8s`에 병합) | 프로파일·TZ 등 여러 Job이 공유하는 값 |
| ⑤ 민감정보 | `secrets` | k8s Secret 선언 (기본 `MSSQL_SECRET`) | `MSSQL_SA_PASSWORD`를 `mssql-secret`의 `SA_PASSWORD` 키에서 env로 주입. 평문 env 금지 |
| ⑥ 리소스 | `resources` | `container_resources` (기본 requests `cpu=250m/mem=512Mi`, limits `cpu=1/mem=1Gi`) | 배치마다 다르므로 파라미터화 |

공통 고정값: `namespace="batch"`, `image_pull_policy="IfNotPresent"`, `get_logs=True`,
`on_finish_action="delete_succeeded_pod"`, `startup_timeout_seconds=300`.

모든 배치 실행 DAG: `schedule=None`(수동/트리거 시연), `catchup=False`,
`is_paused_upon_creation=False`(재실행 시연을 위해 기본 활성).

## 실행 방법 (멀티 Executor 단일 환경)

기본 시연은 `bash scripts/03_airflow.sh multi` 로 `CeleryExecutor,KubernetesExecutor`가 함께
등록된 단일 환경을 띄운 뒤, DAG는 그대로 두고 관찰만 한다:

```bash
bash scripts/03_airflow.sh multi
```

- `demo_bash_ops`: Grid에서 `sleep_5s_celery` 그룹이 먼저 끝나고 `sleep_5s_k8s` 그룹은 Pod 기동
  지연만큼 늦게 끝나는 것을 한 화면에서 비교 (`kubectl get pods -n airflow -w`로 Pod 생성 관찰)
- `orderSyncJob`: `run_order_sync`(Celery, 기본)와 `run_order_export`(`executor=
  "KubernetesExecutor"` 지정)를 UI task 상세의 executor 표기로 비교

단일 executor를 실측 비교해야 할 때만 `bash scripts/07_switch_executor.sh kubernetes|celery`로
환경 자체를 전환한다 — 기본 시연에서는 사용하지 않는다.

## git-sync 반영 흐름

1. adip-batch-apps CI가 `job-order` 이미지를 빌드하고 레지스트리에 push
2. CI가 이 저장소의 `dags/config/images.py`의 해당 태그 값을 갱신하여 커밋/푸시
3. Airflow 스케줄러/웹서버의 git-sync 사이드카가 주기적으로 이 저장소를 pull
4. 다음 DAG 파싱 주기에 새 이미지 태그로 DAG가 갱신됨 (파드 재시작 불필요)

DAG 파일 자체를 추가/수정하는 경우도 동일한 git-sync 경로로 반영된다.
