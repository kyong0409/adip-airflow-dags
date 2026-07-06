# adip-airflow-dags

Airflow 3.2.x 배치 시연용 DAG 모음. `dags/` 폴더가 git-sync 대상이며, Airflow 스케줄러/웹서버
파드가 이 저장소를 주기적으로 pull하여 DAG를 반영한다.

시연 범위(계획서 v2.1): **Executor 2종(Kubernetes/Celery) × Operator 2종(Bash/KPO) = 4케이스
매트릭스**. 흐름제어/센서/Asset 등 그 외 Operator 데모는 범위에서 제외되었다.

## dag_id 규칙

**dag_id = Spring Batch job 이름(jobNm)** — dscore.fw-be의 `AirflowDagAdapter`가 재실행/트리거
시 `jobNm`을 그대로 `dag_id`로 사용해 `POST /api/v2/dags/{dag_id}/dagRuns`를 호출하기 때문이다.
따라서 배치 실행 DAG(`order_sync`, `daily_ad_stats`, `ads_csv_export`)는 파일명과 `dag_id`가
다를 수 있어도, **`dag_id`는 반드시 대응하는 Spring Batch job 이름과 camelCase까지 정확히
일치**해야 한다.

데모 전용 DAG(`demo_*`, `ops_*` 접두어)는 이 규칙의 예외로, Spring Batch job과 무관하므로
snake_case dag_id를 그대로 사용한다.

## DAG 카탈로그

| dag_id | 파일 | 용도 | 주 Operator | 매트릭스 케이스 |
|---|---|---|---|---|
| `demo_bash_ops` | `demo_bash_ops.py` | Executor 비교 실측(sleep5×20) + 전처리 셸 + 런타임 한계 실증(java_runtime_check는 **의도된 실패**) | BashOperator | ①(E1×Bash) ③(E2×Bash) |
| `orderSyncJob` | `order_sync.py` | 주문 동기화 배치 — skip 초과 실패 → **UI 재실행 사이클 주인공** | KubernetesPodOperator | ②(E1×KPO) ④(E2×KPO) |
| `dailyAdStatsJob` | `daily_ad_stats.py` | 일별 광고 통계 집계 — 파티셔닝 무거운 잡 (선택·심화) | KubernetesPodOperator | ②④ |
| `adsCsvExportJob` | `ads_csv_export.py` | 광고 통계 CSV 추출 (선택) | KubernetesPodOperator | ②④ |
| `ops_kpo_taskflow` | `ops_kpo_taskflow.py` | `@task.kubernetes` (KPO 실행방식 ②, 회의 산출물) — 매트릭스 밖 참고 | `@task.kubernetes` | 참고 |

KPO 배치 DAG는 필요 connection으로 k8s Secret(`mssql-secret`)을 사용한다.

## config/ 모듈

- `config/images.py` — 배치 앱 이미지 태그(`IMAGES` dict)를 중앙관리. **CI가 빌드/푸시 후 이
  파일 값만 갱신**하는 규약이다. 키 이름(`job-order`, `job-ads`)은 고정.
- `config/defaults.py` — `batch_pod_kwargs(group, job_name, extra_args)` 헬퍼. 모든 배치 실행
  DAG가 공통으로 쓰는 namespace/리소스/시크릿/환경변수를 이 함수 하나로 만든다.

## KubernetesPodOperator 공통 규약 (배치 실행 DAG)

- `namespace="batch"`, `get_logs=True`, `on_finish_action="delete_succeeded_pod"`,
  `image_pull_policy="IfNotPresent"`, `startup_timeout_seconds=300`
- `env_vars`: `SPRING_PROFILES_ACTIVE=k8s`, `SPRING_BATCH_JOB_NAME=<jobNm>`
- `MSSQL_SA_PASSWORD`는 k8s Secret(`mssql-secret`)의 `SA_PASSWORD` 키에서 env로 주입
  (`airflow.providers.cncf.kubernetes.secret.Secret`)
- `container_resources`: requests `cpu=250m/mem=512Mi`, limits `cpu=1/mem=1Gi`
- JobParameter는 Airflow `params`로 받아 `arguments=["targetDate={{ params.target_date }}"]`
  형태로 전달 (기본값 `""`이면 앱이 어제 날짜로 처리). Spring Boot의
  JobLauncherApplicationRunner는 non-option 인자(`name=value`)만 JobParameter로 변환하므로
  `--name=value` 형식은 쓰지 않는다.
- 모든 배치 실행 DAG: `schedule=None`(수동/트리거 시연), `catchup=False`,
  `is_paused_upon_creation=False`(재실행 시연을 위해 기본 활성)

## 4케이스 매트릭스 실행 방법

Executor는 클러스터 설정(helm values)이므로 DAG는 그대로 두고 Executor만 전환한다:

```bash
bash scripts/07_switch_executor.sh kubernetes   # E1 — demo_bash_ops + orderSyncJob 실행 (케이스 ①②)
bash scripts/07_switch_executor.sh celery       # E2 — 같은 DAG 재실행 (케이스 ③④)
```

관찰: E1은 `kubectl get pods -n airflow -w`(태스크마다 Pod), E2는 flower(큐/상주 워커).

## git-sync 반영 흐름

1. adip-batch-apps CI가 `job-order` / `job-ads` 이미지를 빌드하고 레지스트리에 push
2. CI가 이 저장소의 `dags/config/images.py`의 해당 태그 값을 갱신하여 커밋/푸시
3. Airflow 스케줄러/웹서버의 git-sync 사이드카가 주기적으로 이 저장소를 pull
4. 다음 DAG 파싱 주기에 새 이미지 태그로 DAG가 갱신됨 (파드 재시작 불필요)

DAG 파일 자체를 추가/수정하는 경우도 동일한 git-sync 경로로 반영된다.
