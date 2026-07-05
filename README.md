# adip-airflow-dags

Airflow 3.2.x 배치 시연용 DAG 모음. `dags/` 폴더가 git-sync 대상이며, Airflow 스케줄러/웹서버
파드가 이 저장소를 주기적으로 pull하여 DAG를 반영한다.

## dag_id 규칙

**dag_id = Spring Batch job 이름(jobNm)** — dscore.fw-be의 `AirflowDagAdapter`가 재실행/트리거
시 `jobNm`을 그대로 `dag_id`로 사용해 `POST /api/v2/dags/{dag_id}/dagRuns`를 호출하기 때문이다.
따라서 배치 실행 DAG(`order_*`, `daily_ad_stats`, `ads_csv_export`, `settlement_*`)는 파일명과
`dag_id`가 다를 수 있어도, **`dag_id`는 반드시 대응하는 Spring Batch job 이름과 camelCase까지
정확히 일치**해야 한다.

시연/데모 전용 DAG(`ops_*`, `perf_*` 접두어)는 이 규칙의 예외로, Spring Batch job과 무관하므로
snake_case dag_id를 그대로 사용한다.

## DAG 카탈로그

| dag_id | 파일 | 용도 | 주 Operator | 필요 connection / provider |
|---|---|---|---|---|
| `orderSyncJob` | `order_sync.py` | 주문 동기화 배치 | KubernetesPodOperator | k8s Secret(`mssql-secret`) |
| `orderRetryJob` | `order_retry.py` | 주문 동기화 재실행 시연 | KubernetesPodOperator | k8s Secret(`mssql-secret`) |
| `dailyAdStatsJob` | `daily_ad_stats.py` | 일별 광고 통계 집계 | KubernetesPodOperator | k8s Secret(`mssql-secret`) |
| `adsCsvExportJob` | `ads_csv_export.py` | 광고 통계 CSV 추출 | KubernetesPodOperator | k8s Secret(`mssql-secret`) |
| `dailySettlementJob` | `settlement_daily.py` | 정산 일배치 (체인 1/3) | KubernetesPodOperator, TriggerDagRunOperator | k8s Secret(`mssql-secret`) |
| `settlementAggregateJob` | `settlement_aggregate.py` | 정산 집계 (체인 2/3) | KubernetesPodOperator, TriggerDagRunOperator | k8s Secret(`mssql-secret`) |
| `settlementReportJob` | `settlement_report.py` | 정산 리포트 (체인 3/3) | KubernetesPodOperator | k8s Secret(`mssql-secret`) |
| `ops_showcase_flow` | `ops_showcase_flow.py` | Branch/ShortCircuit/LatestOnly/TaskGroup 데모 | BranchPythonOperator, ShortCircuitOperator, LatestOnlyOperator, EmptyOperator | - |
| `ops_showcase_integration` | `ops_showcase_integration.py` | DB/HTTP/Blob 통합 데모 | SQLExecuteQueryOperator, HttpSensor, HttpOperator, Wasb 계열 | `mssql_default`, `dscore_be`, `wasb_azurite`; providers: common-sql, microsoft-mssql, microsoft-azure, http |
| `ops_showcase_asset_producer` / `ops_showcase_asset_consumer` | `ops_showcase_assets.py` | Asset 이벤트 기반 스케줄 + deferrable FileSensor 비교 | `@task`(outlets), FileSensor | - |
| `ops_kpo_taskflow` | `ops_kpo_taskflow.py` | `@task.kubernetes` (KPO 실행방식 ②) | `@task.kubernetes` | - |
| `ops_multi_executor` | `ops_multi_executor.py` | 멀티 Executor 혼합 실행 데모 | `@task`, `@task(executor=...)`, KubernetesPodOperator | 멀티 executor 클러스터(values-multi)에서만 유효 |
| `perf_short_tasks` | `perf_short_tasks.py` | Executor 처리량 비교용 부하 | `@task.expand` | - |

## config/ 모듈

- `config/images.py` — 배치 앱 이미지 태그(`IMAGES` dict)를 중앙관리. **CI가 빌드/푸시 후 이
  파일 값만 갱신**하는 규약이다. 키 이름(`job-order`, `job-ads`, `job-settlement`)은 고정.
- `config/defaults.py` — `batch_pod_kwargs(group, job_name, extra_args)` 헬퍼. 모든 배치 실행
  DAG가 공통으로 쓰는 namespace/리소스/시크릿/환경변수를 이 함수 하나로 만든다.

## KubernetesPodOperator 공통 규약 (배치 실행 DAG)

- `namespace="batch"`, `get_logs=True`, `on_finish_action="delete_succeeded_pod"`,
  `image_pull_policy="IfNotPresent"`, `startup_timeout_seconds=300`
- `env_vars`: `SPRING_PROFILES_ACTIVE=k8s`, `SPRING_BATCH_JOB_NAME=<jobNm>`
- `MSSQL_SA_PASSWORD`는 k8s Secret(`mssql-secret`)의 `SA_PASSWORD` 키에서 env로 주입
  (`airflow.providers.cncf.kubernetes.secret.Secret`)
- `container_resources`: requests `cpu=250m/mem=512Mi`, limits `cpu=1/mem=1Gi`
- JobParameter는 Airflow `params`로 받아 `arguments=["--targetDate={{ params.target_date }}"]`
  형태로 전달 (기본값 `""`이면 앱이 어제 날짜로 처리)
- 모든 배치 실행 DAG: `schedule=None`(수동/트리거 시연), `catchup=False`,
  `is_paused_upon_creation=False`(재실행 시연을 위해 기본 활성)

## settlement 체인

```
dailySettlementJob --(TriggerDagRunOperator, wait_for_completion=False)--> settlementAggregateJob
settlementAggregateJob --(TriggerDagRunOperator, wait_for_completion=False)--> settlementReportJob
```

## git-sync 반영 흐름

1. adip-batch-apps CI가 `job-order` / `job-ads` / `job-settlement` 이미지를 빌드하고 레지스트리에 push
2. CI가 이 저장소의 `dags/config/images.py`의 해당 태그 값을 갱신하여 커밋/푸시
3. Airflow 스케줄러/웹서버의 git-sync 사이드카가 주기적으로 이 저장소를 pull
4. 다음 DAG 파싱 주기에 새 이미지 태그로 DAG가 갱신됨 (파드 재시작 불필요)

DAG 파일 자체을 추가/수정하는 경우도 동일한 git-sync 경로로 반영된다.
