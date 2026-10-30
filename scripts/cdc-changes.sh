#!/usr/bin/env bash
# Generates visible changes in both source databases for the live demo.
set -euo pipefail
source env.sh; source secrets.env
KC="kubectl --context kind-$KIND_CLUSTER"

$KC -n data-sources exec statefulset/mysql -- mysql -uroot -p"$MYSQL_ROOT_PW" shop -e "
  UPDATE customers SET tier='gold' WHERE id=2;
  DELETE FROM customers WHERE id=5;
  INSERT INTO customers (name,email,city) VALUES ('Zoe Lind','zoe@example.com','Oslo');" 2>/dev/null

$KC -n data-sources exec statefulset/postgres -- psql -U postgres -d shop -c "
  UPDATE products SET stock=stock-5 WHERE sku='SKU-A';
  INSERT INTO products (sku,name,price,stock) VALUES ('SKU-E','Thingamajig',12.00,$((RANDOM % 90 + 10)));"

echo "Changes applied. They appear in the Iceberg tables after the next Tableflow commit (a few minutes)."
