FROM apache/apache/airflow:3.3.2

USER airflow

# 추가 설치 패키지 때문에 Dockerfile 구성
COPY requirements.txt /requirements.txt

# 실행
RUN pip install --no-cache-dir "apache-airflow=3.3.2" -r /requirements.txt