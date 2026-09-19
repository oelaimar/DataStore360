CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.superstore_raw;

CREATE TABLE staging.superstore_raw (
    row_id        BIGINT,
    order_id      VARCHAR(32),
    order_date    VARCHAR(24),
    ship_date     VARCHAR(24),
    ship_mode     VARCHAR(32),
    customer_id   VARCHAR(16),
    customer_name VARCHAR(128),
    segment       VARCHAR(32),
    country       VARCHAR(64),
    city          VARCHAR(64),
    state         VARCHAR(64),
    postal_code   VARCHAR(24),
    region        VARCHAR(32),
    product_id    VARCHAR(32),
    category      VARCHAR(32),
    sub_category  VARCHAR(32),
    product_name  TEXT,
    sales         DOUBLE PRECISION,
    quantity      DOUBLE PRECISION,
    discount      DOUBLE PRECISION,
    profit        DOUBLE PRECISION
);