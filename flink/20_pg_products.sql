-- Decode the PostgreSQL Debezium topic `pg.public.products` into an upsert topic for Tableflow.
-- price is STRING because the connector uses decimal.handling.mode=string.
CREATE TABLE `pg_products` (
  `id` INT NOT NULL,
  `sku` STRING,
  `name` STRING,
  `price` STRING,
  `stock` INT,
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

INSERT INTO `pg_products`
SELECT `id`, `sku`, `name`, `price`, `stock` FROM `pg.public.products`;
