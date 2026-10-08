# 목표
- Silver -> Gold 데이터 파이프라인 구성중 (2가지 방향성 데이터 적제)
    - 분석용 Gold 데이터 (기존 적용 동일)
    - 지식용 Gold 데이터 (RAG용, 청킹, 임베딩)
```text
                         S3 Silver
                             |
                      Airflow Sensor
                             |
                      inspect_silver
                             |
             +---------------+---------------+
             |                               |
             v                               v
      Analytics Gold                  Knowledge Gold
             |                               |
      product_metrics                    documents
        daily_kpi                           |
             |                           Chunking
             |                               |
             |                           Embedding
             |                               |
             v                               v
      PostgreSQL Tables               pgvector Tables
             |                               |
             +---------------+---------------+
                             |
                          RAG/Agent
```