from __future__ import annotations

import argparse
import os
from pathlib import Path
from dotenv import load_dotenv

import boto3

load_dotenv()

def main():
    # 2026-10-08일 정보를 인자로 전달
    # 강제로 전달 하는 이유는 브론즈 -> 실버 airflow dag가 없어서 임의로 편성
    # python scripts/upload_silver.py --date 2026-10-08
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # 버킷 정보 획득
    bucket = os.environ["ECOMMERCE_BUCKET"]
    # 리전 정보 획득
    region = os.getenv("AWS_REGION", "ap-northeast-2")
    # 현재 임시 데이터가 있는 경로를 s3의 파티션과 동일하게 구성하여 표기
    # *.csv, _SUCCESS 데이터가 들어 있는 경로 획득
    src = Path("sample_data/silver") / f"dt={args.date}"

    # 없으면 오류
    if not src.exists():
        raise SystemExit(f"not found: {src}")

    # 존재하면 s3 클라이언트 획득
    s3 = boto3.client("s3", region_name=region)

    # 소스 폴더의 데이터를 하나씩 꺼내서
    for path in sorted(src.iterdir()):
        # 파일이 맞으면
        if path.is_file():
            # 객체 키 생성
            key = f"silver/dt={args.date}/{path.name}"
            # 로그
            print(f"{path} -> s3://{bucket}/{key}")
            # 업로드
            s3.upload_file(str(path), bucket, key)

    # 완료
    print("[OK] Silver upload complete")


if __name__ == "__main__":
    main()