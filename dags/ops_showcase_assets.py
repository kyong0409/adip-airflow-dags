"""
Airflow 3 Asset 기반 이벤트 스케줄 데모 — 생산 DAG(@task가 Asset outlet 갱신)와
소비 DAG(schedule=[asset])의 페어, 그리고 poke 방식 FileSensor vs deferrable FileSensor 비교.

마커 파일: /opt/airflow/dags/ops_showcase_marker.txt (git-sync 볼륨 안, 컨테이너 기준 경로)
"""

from datetime import datetime

from airflow.sdk import DAG, Asset, task
from airflow.providers.standard.sensors.filesystem import FileSensor

MARKER_FILE_PATH = "/opt/airflow/dags/ops_showcase_marker.txt"
MARKER_ASSET = Asset("adip://ops-showcase/marker-file")

with DAG(
    dag_id="ops_showcase_asset_producer",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "ops-showcase", "asset"],
) as producer_dag:

    @task(outlets=[MARKER_ASSET])
    def produce_marker_file():
        with open(MARKER_FILE_PATH, "w") as f:
            f.write("produced-by-ops_showcase_asset_producer")

    produce_marker_file()


with DAG(
    dag_id="ops_showcase_asset_consumer",
    schedule=[MARKER_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["adip", "ops-showcase", "asset"],
) as consumer_dag:

    # 일반 poke 모드 — 워커 슬롯을 점유한 채 poke_interval마다 반복 확인.
    wait_for_marker_poll = FileSensor(
        task_id="wait_for_marker_poll",
        filepath=MARKER_FILE_PATH,
        poke_interval=10,
        timeout=120,
    )

    # deferrable 모드 — 트리거러에게 위임하고 워커 슬롯을 반환한 채 비동기로 대기.
    wait_for_marker_deferred = FileSensor(
        task_id="wait_for_marker_deferred",
        filepath=MARKER_FILE_PATH,
        poke_interval=10,
        timeout=120,
        deferrable=True,
    )
