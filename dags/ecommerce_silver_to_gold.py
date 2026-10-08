# 모듈가져오기
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

# 전역변수 (환경변수)
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
BUCKET = os.getenv("ECOMMERCE_BUCKET", "de-ai-08-loggen-s3-bk-827913617635")
BEDROCK_EMBED_MODEL = os.getenv("BEDROCK_EMBED_MODEL", "amazon.titan-embed-text-v2:0")
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "us-east-1")
log = logging.getLogger(__name__)

# 공용/공통등 함수

# DAG (@dag)

    # Task (@task 구성, taskgroup(n개 task 그룹화))

    # 의존성(3.x 부터 방향성 지시, task 결과를 새로 넣으면서 진행, 병렬 진행, fan-in/fan-out 구성 가능)