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
from psycopg2.extras import execute_values # 여러 데이터흘 한번에 insert 적제 효율 상승

# 2. 전역변수(환경변수) -> .env는 이미 적용된 상태임 (airflow 내부 컨테이너)
AWS_REGION          = os.getenv("AWS_REGION","ap-northeast-2")
BUCKET              = os.getenv("ECOMMERCE_BUCKET","de-ai-25-infra-s3-bk-827913617635")
EMBEDDING_PROVIDER  = os.getenv("EMBEDDING_PROVIDER","bedrock") # local, bedrock
BEDROCK_EMBED_MODEL = os.getenv("BEDROCK_EMBED_MODEL","amazon.titan-embed-text-v2:0")
BEDROCK_REGION      = os.getenv("BEDROCK_REGION","us-east-1")
log                 = logging.getLogger(__name__)

# 공용/공통등 함수
#########
# AWS 관련
# s3 client 함수
def s3_client():
    # airflow ui에 등록한 커넥션 정보를 활용
    hook = S3Hook(aws_conn_id="aws_default")
    return hook.get_conn()
    #return boto3.client("s3", region_name=AWS_REGION)

# s3에 존재하는 csv를 읽어서 df로 반환
def read_csv_s3(key: str) -> pd.DataFrame:
    # csv 객체 획득
    obj = s3_client().get_object(Bucket=BUCKET, Key=key)
    # df 변환 처리
    return pd.read_csv( io.BytesIO(obj['body'].read()) )

# S3에 존재하는 jsonl을 읽어서 반환 (컨셉상 골드데이터인데 JSONL임)
def read_jsonl_s3(key: str) -> list[dict[str, Any]]:
    # jsonl 객체 획득
    obj = s3_client().get_object(Bucket=BUCKET, Key=key)
    text = obj["Body"].read().decode("utf-8")
    return [
        json.loads(line) # 역직렬화 (str -> dict)
        for line in text.splitlines()
        if line.strip() # 공백문자 제거
    ]

# jsonl -> s3 업로드 (데이터 양이 많으면 parquet, orc, avro 등으로 처리하는 것이 좋음, 압축필요)
# 처리 결과에 대한 메타 정보 반환
def write_jsonl_s3( rows: list[dict[str, Any]], key: str) -> dict[str, Any]:
    # list[dict[str, Any]] => str
    body = "\n".join(
        json.dumps(row, ensure_ascii=False) # 데이터 1개는 json 문자열로 처리 => \n 결합 => jsonl 형태로 구성
        for row in rows # 다양한 csv => 공통 스키마 구조로 dict화 한 데이터 1개씩 추출
    ).encode('utf-8')
    # s3 업로드 -> 골드 데이터를 s3에 업로드
    s3_client().put_object(
        Bucket = BUCKET,
        Key    = key,
        Body   = body,
        ContentType = "application/x-ndjson; charset=utf-8"
    )
    # 메타 데이터 구성후 반환
    return {"bucket": BUCKET, "key":key, "rows":len(rows)}


#########
# RAG 관련
# 청킹 (문서를 실제 검색 단위로 자르는 함수)
def chunk_text(text: str,               # 문장 원본 데이터
               max_chars: int = 220,    # 문서 단위로 자를 최대 문자수 임의 지정
               overlap: int = 50        # 문맥 정보 손실을 위해서 중첩 문자수 임의 지정
) -> list[str]:
    # 공백을 구분자로 문자등을 합치기 -> 말뭉치 구성
    # 공백/개행등을 정리하여 청킹 기준으로 구성
    text = " ".join( str(text).split() )
    # 공백 문장 나온다면
    if not text:
        return []
    # 청킹 결과를 담을 그릇 준비
    chunks: list[str] = []
    # 시작위치
    start = 0

    # 반복 처리
    while start < len(text):
        # 청킹 단위로 자르기
        # 현재 위치를 기준으로 220 + 더한값ㄱ이 작은지, 문장의 길이가 작은지 체크 -> 끝값 확인
        end   = min(start + max_chars, len(text))
        # 청크를 담는 그릇에 추가
        chunks.append( text[start:end].strip() )
        # 체킹
        if end >= len(text):
            break
        # 다음 청크를 시작할때, overlap 추가시켜서 문맥을 겹치게 구성 (맥락이 이어지게, 잃지 않게)
        start = max(0, end - overlap)

    # 청크 목록 반환
    return chunks

# 토큰화
def bedrock_embedding(text: str) -> list[float]:
    clinet = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

    response = clinet.invoke_model(
        modelId=BEDROCK_EMBED_MODEL,
        body = json.dumps({
            "InputText": text,
            "demensions": 1024, # 외부 env 구성 가능
            "normalize": True   # 유사도 게산에 유리하게 적용되도록(쉽게), 정규화 처리 세팅
        })
    )
    # dict 변환
    payload = json.loads( response["body"].read() )
    return payload["embedding"]
    pass

# 임베딩
def embed(text: str) -> list[float]:
    # 임베딩 모델별로 각각 처리 가능한 함수
    if EMBEDDING_PROVIDER == "bedrock":
        return bedrock_embedding(text)
    #elif EMBEDDING_PROVIDER == "local":
    #   pass
    pass    



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
    @task_group(group_id="knowledge_rag")
    def knowledge_rag(process_date: str):
        # 실버 데이터를 공통 문서 모델로 구성 task 집합 - ETL 수행
        # 1. 리뷰, cs, 환불, 정책 => 통합 문서 구성 -> ETL  => 골드 데이터 구성 완료
        @task
        def build_knowledge_gold(process_date: str) -> dict[str, Any]:
            # s3 버킷내 실버 경로
            base = f"silver/dt={process_date}"
            print( f"base => {base}")
        
            # 데이터 로드 
            # 상품 마스터
            products = read_csv_s3(f"{base}/products.csv")    
            # 고객 리뷰
            reviews = read_csv_s3(f"{base}/reviews.csv")
            # CS 문의
            cs_tickets = read_csv_s3(f"{base}/cs_tickets.csv")
            # 환불 기록
            refunds = read_csv_s3(f"{base}/refunds.csv")
            # 회사 정책 문서
            policies = read_csv_s3(f"{base}/policies.csv")
        
            # product_id로 제품명 dict 구성
            product_map = (
                products
                .set_index("product_id")["product_name"]
                .to_dict()
            )
        
            # 통합 스키마를 담는 그릇 준비
            docs: list[dict[str, Any]] = []
        
            # 리뷰 -> 지식 문서로 변환
            for row in reviews.to_dict("records"):
                docs.append({
                    # 문서 ID -> 데이터를 구분하는 ID가 문서 ID로 대체됨
                    "document_id"   : f"review:{row['review_id']}",  # V000001, V000002, ...
                    # 처리일
                    "process_date"  : process_date,
                    # 문서 출처
                    "source_type"   : "review",
                    # 소스 ID -> 중복 사용
                    "source_id"     : row['review_id'],
                    # 어떤 제품에 대한 리뷰
                    "product_id"    : row['product_id'],
                    # 이벤트 시간
                    "event_time"    : row['review_time'],
                    # 타이틀
                    "title"         : (
                        f"{product_map.get(row['product_id'])} "
                        "고객 리뷰"
                    ),
                    # 리뷰에 대한 핵심 내용 -> 자연형 문서 형태로 합침, 문장의 구분자로 .를 사용
                    "content"       : (
                        f"상품: {product_map.get(row['product_id'])}. "
                        f"평점: {row['rating']}점. "
                        f"고객 리뷰: {row['review_text']}"
                    ),
                    # 메타 데이터 -> 검색 결과와 함께 활용되는 구조화 정보
                    "metadata"      : {
                        "rating"   : int(row['rating']), 
                        "order_id" : row['order_id']
                    }
                })
            #print( docs )
        
            # CS -> 지식 문서로 변환
            for row in cs_tickets.to_dict("records"):
                docs.append({
                    "document_id"   : f"cs:{row['ticket_id']}",  # V000001, V000002, ...
                    "process_date"  : process_date,
                    "source_type"   : "cs_ticket",
                    "source_id"     : row['ticket_id'],
                    "product_id"    : row['product_id'],
                    "event_time"    : row['created_at'],
                    "title"         : (
                        f"{product_map.get(row['product_id'])} "
                        "CS 문의"
                    ),
                    "content"       : (
                        f"상품: {product_map.get(row['product_id'])}. "
                        f"문의 우형: {row['topic']}. "
                        f"고객 문의: {row['message']}"
                    ),
                    # 토픽, 고객 정보 메타 데이터 구성
                    "metadata"      : {
                        "topic"    : row['topic'], 
                        "order_id" : row['order_id'],
                        "customer_id" : row['customer_id']
                    }
                    
                })
        
                
            # 환불 -> 지식 문서로 변환
            for row in refunds.to_dict("records"):
                docs.append({
                    "document_id"   : f"refund:{row['refund_id']}", 
                    "process_date"  : process_date,
                    "source_type"   : "refund",
                    "source_id"     : row['refund_id'],
                    "product_id"    : row['product_id'],
                    "event_time"    : row['refund_time'],
                    "title"         : (
                        f"{product_map.get(row['product_id'])} "
                        "환불 기록"
                    ),
                    "content"       : (
                        f"상품: {product_map.get(row['product_id'])}. "
                        f"문의 유형: {row['refund_reason']}. "
                        f"고객 문의: {row['refund_amount']}원."
                    ),
                    # 환불 이유
                    "metadata"      : {
                        "refund_reason"    : row['refund_reason'], 
                        "order_id" : row['order_id']
                    }
                })
                
            # 정책 -> 지식 문서로 변환
            for row in policies.to_dict("records"):
                docs.append({
                    "document_id"   : f"policy:{row['policy_id']}", 
                    "process_date"  : process_date,
                    "source_type"   : "policy",
                    "source_id"     : row['policy_id'],
                    "product_id"    : None,
                    "event_time"    : None,
                    "title"         : row['title'],
                    "content"       : (
                        f"정책명: {row['title']}. "
                        f"정책 내용: {row['content']}"
                    ),
                    "metadata"      : {
                        "policy_type"    : row['policy_type']
                    }
                })
        
            # s3에 업로드 되는 상황까지 고려 =>  key 필요 (경로)
            # csv => dt => dict => jsonl
            key = (
                f"gold/knowledge/dt={process_date}/"
                "documents.jsonl"
            )
        
            # jsonl 내용 구성 s3 업로드 처리
            return write_jsonl_s3(docs, key)

        # transform
        # 2. 통합 문서(n개의 서로 다른 구조의 csv를 통합 스키마로 구성)의 문제 없는지 검사
        @task
        def knowledge_quality_check( knowledge_meta: dict[str, Any])->dict[str, Any]:
            # 해당 문서의 개수가 0건이면 이후 진행 x => 실패 처리
            if knowledge_meta["rows"] <= 0:
                raise ValueError(f"knowledge document is empty: {knowledge_meta}")
            return knowledge_meta
        
        # 3. 검색 단위 청킹 처리
        @task
        def chunk_documents(knowledge_meta: dict[str, Any], 
                            process_date: str)->dict[str, Any]:
            # 문서 구성시 사용했더 재료를 사용 or s3에서 다시 읽어와서 처리 
            # 시간을 요하지 않으므로 s3에서 다시 읽어와서 처리  -> 실패하면 에러 (s3 점검의 의미도 가짐)
            documents = read_jsonl_s3( knowledge_meta["key"] )

            # 청킹 데이터를 담을 그릇 준비
            chunks: list[dict[str, Any]] = []

            # 문서별로 독립적인 청킹 진행
            for doc in documents:
                # 청킹 처리 -> 말뭉치를 청크 단위로 분해(검색 단위)
                parts = chunk_text(doc["content"], max_chars=220, overlap=50) # 수치를 바꾸면 -> 품질 바뀜
                # 청크별로 레코드 구성(구조화)
                for i, part in enumerate(parts):
                    chunks.append({
                        # 청크 ID (문서ID:청크인덱스)
                        "chunk_id": f"{doc['id']}:{i}",
                        # 문서 ID
                        "document_id": doc['document_id'],
                        # process_date
                        "process_date": process_date,
                        # 청크 인덱스
                        "chunk_index": i,
                        # 문서 타입
                        "source_type": doc['source_type'],
                        # 제품 아이디
                        "product_id": doc['product_id'],
                        # 이벤트 타임
                        "event_time": doc['event_time'],
                        # 콘텐츠(원본)
                        "content": part,
                        # 메타데이터(문서원문 메타데이터 활용)
                        "metadata": doc['metadata']
                    })             

            # 키 생성 
            key = (
                f"gold/knowledge/dt={process_date}/"
                "chunks.jsonl"
            )
            # s3에 업로드
            return write_jsonl_s3(chunks, key)
        
        # 4. 임베딩 -> 백터화
        @task
        def create_embedding(chuck_meta: dict[str, Any], 
                             process_date: str)->dict[str, Any]:
            
            # 1. chuck_meta를 이용하여 청크 데이터 획득 
            chunks = read_jsonl_s3( chuck_meta["key"] )

            # 2. 백터화된 데이터를 저장할 그릇
            embedded = []

            # 3. 청크별로 임베딩 처리 -> 벡터화             
            for chunk in chunks:
                embedded.append({
                    # 청크 구조화된 데이터 dict를 그대로 삽입
                    **chunk,
                    # 임베딩 데이터 추가
                    "embedding": embed(
                        chunk["content"]
                    )
                })

            # 키 생성 
            key = (
                f"gold/knowledge/dt={process_date}/"
                "embeddings.jsonl"
            )
            # s3에 업로드
            result = write_jsonl_s3(embedded, key)

            # 추가 정보 삽입 (메타 정보, 임베딩모델 공급자, 차원정보(임베딩되는 데이터 단위))
            result["provider"]   = EMBEDDING_PROVIDER
            result["dimensions"] = 1024

            return result
        
        # 5. postgreSQL/pgVector 적제 -> Load
        @task
        def load_rag_to_pgvector(knowledge_meta: dict[str, Any], 
                                 embedding_meta: dict[str, Any], 
                                 process_date: str)->dict[str, Any]:
            # 데이터 획득
            # 지식 원문 데이터 획득
            documents = read_jsonl_s3( knowledge_meta['key'] )
            # 임베딩 완료된 청크 데이터 획득 
            chunks    = read_jsonl_s3( embedding_meta['key'] )

            # 데이터를 postgreSQL/pgVector  적제
            # PostgresHook 획득 => 데이터 삽입 처리
            hook = PostgresHook(
                # ecommerce_db : airflow > 커넥션 정보 등록
                postgres_conn_id = "ecommerce_db"
            )
            # 커넥션 획득
            conn = hook.get_conn()
            # 예외 처리 -> 실패를 대배하여 롤백 처리를 위해 구성
            try:
                with conn.cursor() as cur:
                    # process_date를 기준으로(데이터를 처리하는 날 기준)

                    # 중복 데이터 삭제 처리
                    # 같은 날짜에 재실행되어서 중복된 데이터가 처리 되지 않도록 파티션 기준으로 제거
                    cur.execute(
                        "delete from document_chunks "
                        "where process_date = %s",
                        (process_date,)
                    )
                    cur.execute(
                        "delete from knowledge_documents "
                        "where process_date = %s",
                        (process_date,)
                    )

                    # 데이터를 한번에 insert 하기 위해 insert에 맞는 구조로 데이터를 변형
                    document_rows = [
                        # 순서 유지 중요,
                        (
                            d["document_id"],
                            d["process_date"],
                            d["source_type"],
                            d["source_id"],
                            d["product_id"],
                            d["event_time"],
                            d["title"],
                            d["content"],
                            # 객체 직렬화 처리
                            json.dumps( d["metadata"], ensure_ascii=False)
                        )
                        for d in documents
                    ]

                    # 한번에 batch insert 
                    execute_values(
                        cur,
                        """
                            insert into knowledge_documents (
                                document_id,
                                process_date,
                                source_type,
                                source_id,
                                product_id,
                                event_time,
                                title,
                                content,
                                metadata
                            )
                            values %s
                        """,
                        document_rows
                    )

                    # 청크데이터 삽입
                    chunk_rows = [
                        (
                            c["chunk_id"],
                            c["document_id"],
                            c["process_date"],
                            c["chunk_index"],
                            c["source_type"],
                            c["product_id"],
                            c["event_time"],
                            c["content"],
                            json.dumps( c["metadata"], ensure_ascii=False),
                            c["embedding"]
                        )
                        for c in chunks
                    ]
                    execute_values(
                        cur,
                        """
                            insert into document_chunks (
                                chunk_id,
                                document_id,
                                process_date,
                                chunk_index,
                                source_type,
                                product_id,
                                event_time,
                                content,
                                metadata,
                                embedding
                            )
                            values %s
                        """,
                        [
                            row[:-1]
                            + (f"[{ ",".join( str(float(v)) for v in row[-1] ) }]")
                            for row in chunk_rows
                        ]
                        
                    )

                    pass
                # 최종 반영
                conn.commit()
            except Exception:
                conn.rollback() # 부분 적제 하지 않도록 트렌젝션 전체 롤백
                raise
            finally:
                conn.close()

            # 반환 정보
            # 적제된 문서수, 청크수, 사용된 임베딩 모델의 공급자
            return {
                "status": "loaded",
                "document": len(documents),
                "chunks": len(chunks),
                "embedding_provider": (embedding_meta["provider"])
            }

        
        # 6. 실제 적제된 행수 조회 , 품질 확인 task(검증)
        @task
        def vector_quality_check(
            loaded_result: dict[str, Any],
            process_date: str
        )->dict[str, Any]:pass

        # task간 연결 -> 앞 task의 결과물을 뒤 task의 인자로 전달 => 의존성 구성
        knowledge = build_knowledge_gold( process_date ) # 1. 특정 날짜의 데이터를 지식 데이터 구성(골드처리)
        checked   = knowledge_quality_check( knowledge ) # 2. 해당 데이터 체크
        chunks    = chunk_documents( checked, process_date )  # 3. 청킹 처리
        embedding  = create_embedding( chunks, process_date ) # 4. 임베딩 처리
        loaded    = load_rag_to_pgvector( checked, embedding, process_date ) # 5. pgVector 적재
        return vector_quality_check( loaded, process_date )   # 6. 품질 체크 


    # 각각 task 그룹 실행->호출
    # analytics_result = analytics_gold( process_date )
    knowlegd_result  = knowledge_rag( process_date )

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
            # 처리한 파일수 기록
            "silver_files": silver_meta["file_count"],
            # 분석 결과
            # 지식 결과
            "rag":knowlegd_result
        }
        # XCom으로 게시 (반환)
        return result
        
    # 의존성 마지막 구성 -> 각각 작업이 완료된 후 진행됨 -> fan-in
    finish(
        silver_meta,
        # analytics_result,
        knowlegd_result
    )


    pass

ecommerce_silver_to_gold()