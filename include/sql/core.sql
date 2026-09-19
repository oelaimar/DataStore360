CREATE SCHEMA IF NOT EXISTS core;

-- Ordre de suppression : orders d'abord (dépendances FK).
DROP TABLE IF EXISTS core.orders;
DROP TABLE IF EXISTS core.customers;
DROP TABLE IF EXISTS core.products;


CREATE TABLE core.customers (
    customer_id_hash   VARCHAR(16)  PRIMARY KEY,
    customer_name_hash VARCHAR(64) NOT NULL,
    segment            VARCHAR(32),
    country            VARCHAR(64),
    city               VARCHAR(64),
    state              VARCHAR(64),
    postal_code        BIGINT,
    region             VARCHAR(32),
    CONSTRAINT customers_hash_not_blank CHECK (customer_name_hash <> '')
);

CREATE TABLE core.products (
    product_id   VARCHAR(32) PRIMARY KEY,
    category     VARCHAR(32) NOT NULL,
    sub_category VARCHAR(32),
    product_name TEXT        NOT NULL
);

CREATE TABLE core.orders (
    row_id        BIGINT PRIMARY KEY,
    order_id      VARCHAR(32) NOT NULL,
    customer_id   VARCHAR(16) NOT NULL,
    product_id    VARCHAR(32) NOT NULL,
    order_date    DATE        NOT NULL,
    ship_date     DATE        NOT NULL,
    ship_mode     VARCHAR(32),
    sales         DOUBLE PRECISION NOT NULL CHECK (sales >= 0),
    quantity      INTEGER           NOT NULL CHECK (quantity > 0),
    discount      DOUBLE PRECISION  NOT NULL CHECK (discount BETWEEN 0 AND 1),
    profit        DOUBLE PRECISION,
    delivery_time INTEGER           CHECK (delivery_time >= 0),
    profit_margin DOUBLE PRECISION,

    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
        REFERENCES core.customers (customer_id),
    CONSTRAINT fk_orders_product FOREIGN KEY (product_id)
        REFERENCES core.products (product_id),
    CONSTRAINT chk_ship_after_order CHECK (ship_date >= order_date)
);

CREATE INDEX idx_orders_customer ON core.orders (customer_id);
CREATE INDEX idx_orders_product  ON core.orders (product_id);
CREATE INDEX idx_orders_orderid  ON core.orders (order_id);
