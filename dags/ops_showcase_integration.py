"""
Airflow 3 통합 오퍼레이터 데모 — SQLExecuteQueryOperator(MSSQL) + HttpSensor/HttpOperator
(dscore.fw-be) + Wasb(Azurite).

필요 provider (requirements.txt):
  apache-airflow-providers-common-sql
  apache-airflow-providers-microsoft-mssql
  apache-airflow-providers-microsoft-azure
  apache-airflow-providers-http

이 DAG는 Airflow DagBag에서 파일 단위로 파싱되므로, 위 provider가 설치되지 않아
이 파일 로드가 실패하더라도 다른 DAG의 스케줄링에는 영향이 없다.

필요 connection:
  mssql_default  - MSSQL (ord_order 테이블 조회)
  dscore_be      - dscore.fw-be REST API
  wasb_azurite   - Azurite(Azure Blob Storage 에뮬레이터)
"""

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator

with DAG(
    dag_id="ops_showcase_integration",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "ops-showcase"],
) as dag:

    check_order_count = SQLExecuteQueryOperator(
        task_id="check_order_count",
        conn_id="mssql_default",
        sql="SELECT COUNT(*) FROM ord_order",
    )

    check_be_alive = HttpSensor(
        task_id="check_be_alive",
        http_conn_id="dscore_be",
        endpoint="/api/om/batch-history/histories?page=0&size=1",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        timeout=60,
        poke_interval=10,
    )

    call_be_api = HttpOperator(
        task_id="call_be_api",
        http_conn_id="dscore_be",
        endpoint="/api/om/batch-history/histories?page=0&size=1",
        method="GET",
    )

    create_marker_file = BashOperator(
        task_id="create_marker_file",
        bash_command="echo ops-showcase > /tmp/ops_showcase_marker.txt",
    )

    upload_marker_to_wasb = LocalFilesystemToWasbOperator(
        task_id="upload_marker_to_wasb",
        file_path="/tmp/ops_showcase_marker.txt",
        container_name="ops-showcase",
        blob_name="marker.txt",
        wasb_conn_id="wasb_azurite",
    )

    wait_for_blob = WasbBlobSensor(
        task_id="wait_for_blob",
        container_name="ops-showcase",
        blob_name="marker.txt",
        wasb_conn_id="wasb_azurite",
        deferrable=True,
    )

    cleanup_blob = WasbDeleteBlobOperator(
        task_id="cleanup_blob",
        container_name="ops-showcase",
        blob_name="marker.txt",
        wasb_conn_id="wasb_azurite",
        is_prefix=False,
    )

    check_order_count >> check_be_alive >> call_be_api
    create_marker_file >> upload_marker_to_wasb >> wait_for_blob >> cleanup_blob
