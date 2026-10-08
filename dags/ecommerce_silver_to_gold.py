# 1. 모듈가져오기
from __future__ import annotations
import hashlib
import io
import json
import math
import os
from datetime import timedelta
from typing import Any
import boto3
import pandas as pd
import pendulum
import logging
from airflow.sdk import Param, dag, task, task_group, get_current_context # 필수요소
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 2. 전역변수(환경변수) -> .env는 이미 적용된 상태임 (airflow 내부 컨테이너)
AWS_REGION          = os.getenv("AWS_REGION","ap-northeast-2")
BUCKET              = os.getenv("ECOMMERCE_BUCKET","de-ai-08-infra-s3-bk-827913617635")
BEDROCK_EMBED_MODEL = os.getenv("BEDROCK_EMBED_MODEL","amazon.titan-embed-text-v2:0")
BEDROCK_REGION      = os.getenv("BEDROCK_REGION","us-east-1")
log                 = logging.getLogger(__name__)

# 공용/공통등 함수
# s3 client 함수
def s3_client():
    # airflow ui에 등록한 커넥션 정보를 활용
    hook = S3Hook(aws_conn_id="aws_default")
    return hook.get_conn()
    #return boto3.client("s3", region_name=AWS_REGION)


# DAG (@dag), 특정 함수에 @dag 데커레이터 추가하면 DAG 구성됨
@dag(
    # airflow ui(대시보스)에 표시되는 DAG ID
    dag_id="ecommerce_s_to_g",
    # DAG 설명
    description="s3 silver->분석/지식 Gold->RDS+vector",
    start_date = pendulum.datetime(2026,10,1, tz="Asia/Seoul"),
    # 트리거 방식 진행 (버튼 클릭)
    schedule   = None,
    catchup    = False,
    default_args={
        "owner":"ai-25",
        # 실패시 재시도 2회
        "retries":2,
        # 간격 30초 대기
        "retry_deply":timedelta(seconds=30)
    },
    # 실습 환경 통제하기 위해 임의로 파라미터 전달
    params={
        "process_date":Param(
            "2026-10-08",
            type="string",
            description="실버 데이터의 파티션 정보, ti의 실행 날짜 정보"
        )
    },
    # 태그
    tags = ["madallion", "gold", "rag", "vector"]
)
def ecommerce_silver_to_gold():
    # TASK (@task 구성, taskgroup(n개 task 그룹화))
    # T1. silver partition 확인 (작업해도 되는지 점검)
    wait_for_silver = S3KeySensor(
        task_id     = "wait_for_silver",
        bucket_name = BUCKET,
        # params.process_date => 실습상 주입한 파마미터 => 실제는 dt값을 획득 구성
        # _SUCCESS 파일이 존재하면 데이터가 모두 적제 된것으로 인지
        bucket_key  = "silver/dt={{ params.process_date }}/_SUCCESS",
        # AWS 접속 인증(UI상에 커넥션 등록값 활용), 만약 없다면 None, env에 키등록해야함
        aws_conn_id = "aws_default",
        # 15초마다 확인
        poke_interval = 15,
        # 최대 10분간 대기후 실패 처리 
        timeout     = 60*10,
        # 대기중에 계속 점유하지 않도록 재스케줄링 모드로 적용
        mode        = "reschedule"
    )

    # 2번째 task -> 함수형 구성
    @task # 함수위에 @task 테커레이터가 부여되면 task로 구성됨
    def resolve_process_date() -> str:
        '''
            airflow context에서 정보 획득
        '''
        context = get_current_context()
        # 코드 레벨로 파라미터 값을 추출(컨텍스트를 통해서)
        return context['params']['process_date']
    
    # TI(task instance)r가 생성됨
    process_date = resolve_process_date()
    
    # 의존성(3.x 방향석 지시, task의 결과를 새로 넣으면서진행, 병렬 진행, fan-in/fan-out 구성)
    # task >> task
    # 필요시 계속 추가
    wait_for_silver >> process_date

    # 처리 날짜 기준 csv 실제적 체크 (실존여부)
    @task
    def inspect_silver(process_date: str) -> dict[str, Any]:
        # 처리 날짜 기준으로 silver 파티션 검사
        # 데이터가 있는 위치까지 경로 구성
        prefix = f"silver/dt={process_date}/"
        # 목록 조회 요청
        response = s3_client().list_objects_v2(
            Bucket=BUCKET,
            Prefix=prefix
        )
        # 응답 데이터중 Key 목록만 획득
        keys = sorted(
            obj["Key"]
            for obj in response.get("Contents", [])
        )
        print(f"keys = {keys}")
        # ['silver/dt=2026-10-08/_SUCCESS', 'silver/dt=2026-10-08/cs_tickets.csv', 'silver/dt=2026-10-08/orders.csv', 'silver/dt=2026-10-08/policies.csv', 'silver/dt=2026-10-08/products.csv', 'silver/dt=2026-10-08/refunds.csv', 'silver/dt=2026-10-08/reviews.csv']

        # 실버 파일 목록이 계획한대로 구성되었는지 조사
        # 당일 스케줄 작동시 해당 파일들이 반드시 존재해야 한다!!
        required = {
            "orders.csv",
            "refunds.csv",
            "reviews.csv",
            "cs_tickets.csv",
            "products.csv",
            "policies.csv",
            "_SUCCESS",
        }

        # keys 에서 실제 key만 추출
        targets = { key.rsplit("/", 1)[-1] for key in keys }

        # 누락 파일 체크, 대상은 중복 제거 되어 있음
        missing = sorted(required - targets)
        # 누락이 존재하면 -> task 실패 처리
        if missing:
            raise ValueError(f"missing Silver files: { missing }")

        # 메타 정보 => XCom에 게시
        return {
            "process_date": process_date,
            "prefix" : prefix,
            "file_count" : len(keys)
        }        

    # task 연결
    # 여기까지 도달 => silver에 데이터가 정상적으로 구성되어 있다 
    silver_meta = inspect_silver( process_date )

    # 2개의 병렬 작업 task 구성 (분석(task, task, ...), 지식(task, task,....) )
    # @task_group(group_id="analytics_gold")
    # def analytics_gold(process_date: str):
    #     return {}

    # 비정형/지식 데이터 처리 함수
    @task_group(group_id="knowlegde_rag")
    def knowlegde_rag(process_date: str):
        # 실버 데이터를 공통 문서 모델로 구성 task 집합 -> ETL 수행
        # 1. 리뷰, cs, 환불, 정책 -> 통합 문서 구성 -> Extract
        @task
        def build_konwlegde_gold():pass
        # transform
        # 2. 통합 문서에 문제가 없는지 검사
        @task
        def knowlegde_quality_check():pass
        # 3. 검색 단위 청킹 처리
        @task
        def chunk_documents():pass
        # 4. 임베딩 -> 백터화
        @task
        def create_embedding():pass
        # 5. postgreSQL/pgvector 적제 -> Load
        @task
        def load_rag_to_pgvector():pass
        # 6. 실제 적제된 행 수 조회, 품질 확인 task(검증)
        @task
        def vector_quality_check():pass

        return {}

    # 각각 task 그룹 실행->호출
    # analytics_result = analytics_gold( process_date )
    knowlegd_result  = knowlegde_rag( process_date )

    # 2개의 tak 그룹을 병렬 fan-out 구성
    silver_meta >> [
        # analytics_result,
        knowlegd_result
    ]

    # 마무리 task 구성 -> DAG의 최종 상태 구성
    @task
    def finish( silver_meta: dict[str, Any],
                # analytics_result: dict[str, Any],
                knowlegd_result:dict[str, Any]
               ):
        # DAG 최종 결과
        result = {
            "status":"success",
            # 처리한 파일 수 기록
            "silver_files":silver_meta["file_count"],
            # 분석 결과
            # 지식 결과
            "rag":knowlegd_result
        }
        # XCom으로 게시 (반환)
        return {}
        
    # 의존성 마지막 구성 -> 각각 작업이 완료된 후 진행됨 -> fan-in
    finish(
        silver_meta,
        # analytics_result,
        knowlegd_result
    )


    pass

ecommerce_silver_to_gold()