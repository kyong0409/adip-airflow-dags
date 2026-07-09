"""
배치 앱 컨테이너 이미지 태그 중앙관리 파일.

CI 파이프라인이 adip-batch-apps 의 job-order 모듈을 빌드하고 이미지를 레지스트리에
push한 뒤, 이 파일의 해당 값만 갱신하여 git-sync로 반영한다.
DAG 파일들은 이 딕셔너리의 키를 그대로 참조하므로, 키 이름은 임의로 바꾸지 말 것.
"""

IMAGES = {
    "job-order": "adip/job-order:0.1.0",
}
