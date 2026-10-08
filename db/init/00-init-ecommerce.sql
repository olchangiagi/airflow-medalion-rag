-- =========================================================
-- 1. Airflow Metadata DB
-- =========================================================

CREATE USER airflow
WITH PASSWORD 'airflow';

CREATE DATABASE airflow
OWNER airflow;


-- =========================================================
-- 2. Ecommerce 실습 DB
-- =========================================================

CREATE USER ecommerce
WITH PASSWORD 'ecommerce';

CREATE DATABASE ecommerce
OWNER ecommerce;


-- =========================================================
-- Ecommerce DB 접속
-- =========================================================

\connect ecommerce


-- =========================================================
-- pgvector 설치
-- postgres 관리자 계정으로 실행됨
-- =========================================================

CREATE EXTENSION IF NOT EXISTS vector;


-- =========================================================
-- 이후 테이블은 ecommerce 사용자가 소유
-- =========================================================

SET ROLE ecommerce;


-- =========================================================
-- Analytics Gold
-- =========================================================

CREATE TABLE product_metrics (
    process_date            date            NOT NULL,
    product_id              varchar(30)     NOT NULL,
    product_name            varchar(200)    NOT NULL,
    category                varchar(100),

    sales_amount            numeric(18,2)   NOT NULL DEFAULT 0,
    order_count             integer         NOT NULL DEFAULT 0,
    quantity                integer         NOT NULL DEFAULT 0,

    refund_count            integer         NOT NULL DEFAULT 0,
    refund_amount           numeric(18,2)   NOT NULL DEFAULT 0,
    refund_rate             numeric(8,2)    NOT NULL DEFAULT 0,

    review_count            integer         NOT NULL DEFAULT 0,
    avg_rating              numeric(4,2)    NOT NULL DEFAULT 0,
    negative_review_count   integer         NOT NULL DEFAULT 0,

    PRIMARY KEY (
        process_date,
        product_id
    )
);


CREATE TABLE daily_kpi (
    process_date        date            PRIMARY KEY,

    sales_amount        numeric(18,2)   NOT NULL DEFAULT 0,
    order_count         integer         NOT NULL DEFAULT 0,

    refund_count        integer         NOT NULL DEFAULT 0,
    refund_rate         numeric(8,2)    NOT NULL DEFAULT 0,

    review_count        integer         NOT NULL DEFAULT 0,
    avg_rating          numeric(4,2)    NOT NULL DEFAULT 0,

    cs_ticket_count     integer         NOT NULL DEFAULT 0
);


-- =========================================================
-- Knowledge Gold
-- =========================================================

CREATE TABLE knowledge_documents (
    document_id     varchar(120)    PRIMARY KEY,

    process_date    date            NOT NULL,

    source_type     varchar(30)     NOT NULL,
    source_id       varchar(80)     NOT NULL,

    product_id      varchar(30),

    event_time      timestamptz,

    title           text,
    content         text            NOT NULL,

    metadata        jsonb
                    NOT NULL
                    DEFAULT '{}'::jsonb
);


CREATE INDEX idx_knowledge_documents_process_date
ON knowledge_documents(process_date);


CREATE INDEX idx_knowledge_documents_product
ON knowledge_documents(product_id);


CREATE INDEX idx_knowledge_documents_metadata
ON knowledge_documents
USING gin(metadata);


-- =========================================================
-- RAG Chunk + pgvector
-- =========================================================

CREATE TABLE document_chunks (
    chunk_id        varchar(160)    PRIMARY KEY,

    document_id     varchar(120)    NOT NULL
                    REFERENCES knowledge_documents(document_id)
                    ON DELETE CASCADE,

    process_date    date            NOT NULL,

    chunk_index     integer         NOT NULL,

    source_type     varchar(30)     NOT NULL,

    product_id      varchar(30),

    event_time      timestamptz,

    content         text            NOT NULL,

    metadata        jsonb
                    NOT NULL
                    DEFAULT '{}'::jsonb,

    embedding       vector(1024)    NOT NULL
);


CREATE INDEX idx_document_chunks_process_date
ON document_chunks(process_date);


CREATE INDEX idx_document_chunks_product
ON document_chunks(product_id);


CREATE INDEX idx_document_chunks_metadata
ON document_chunks
USING gin(metadata);


CREATE INDEX idx_document_chunks_embedding_hnsw
ON document_chunks
USING hnsw (
    embedding vector_cosine_ops
);


RESET ROLE;