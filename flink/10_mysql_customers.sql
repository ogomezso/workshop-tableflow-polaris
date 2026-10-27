-- Decode the MySQL Debezium topic `mysql.shop.customers` into an upsert topic for Tableflow.
-- Create the target with changelog.mode = 'upsert' from the start: Tableflow's mode is immutable after first materialization.
CREATE TABLE `mysql_customers` (
  `id` INT NOT NULL,
  `name` STRING,
  `email` STRING,
  `city` STRING,
  `tier` STRING,
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

INSERT INTO `mysql_customers`
SELECT `id`, `name`, `email`, `city`, `tier` FROM `mysql.shop.customers`;
