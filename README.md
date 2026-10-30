# Tableflow + Apache Polaris: demo script

Streaming data from three kinds of producers lands as open Iceberg tables in **your own S3 bucket**, and any engine reads them through **your own Apache Polaris catalog**, with no storage credentials on the engine.

This README is the script for **running the demo** (about 15 minutes). To **build the environment**, follow [BUILD_GUIDE.md](BUILD_GUIDE.md) (Parts 0 to F). Scope, decisions and risks are in [STATEMENT_OF_WORK.md](STATEMENT_OF_WORK.md).

```
 producer app ------------------------------> orders_events  ------------------.
 MySQL  --Debezium--> mysql.shop.customers --Flink--> mysql_customers ----------+--> Tableflow --> Iceberg in S3
 Postgres --Debezium--> pg.public.products --Flink--> pg_products --------------'                      ^
                                                                                                       | vended, short-lived,
 Trino / PyIceberg --> Apache Polaris (read-only, federated) --> Tableflow Iceberg REST Catalog         | table-scoped S3 credentials
                          '--------------- assumes a read-only IAM role -------------------------------'
```

Everything except Confluent Cloud (on AWS) and the S3 bucket runs in a local kind cluster.

## Key messages

1. **Kafka topic to Iceberg table with no pipeline code.** Tableflow materializes the topic into Iceberg files in the customer's bucket (append for events, upsert for CDC).
2. **Open and customer-owned.** The data sits in the customer's S3 as standard Iceberg. Nothing is copied into a vendor store.
3. **One governed catalog, many engines.** Polaris federates the Tableflow catalog. Engines authenticate to Polaris only; **Polaris issues the S3 credentials**, scoped to one table and short-lived.
4. **No sync jobs.** A new table or a new snapshot appears in Polaris by itself.

## Before you go live (30 minutes)

Requirements: the environment is built ([BUILD_GUIDE.md](BUILD_GUIDE.md)) and the Debezium connectors, Flink statements and Tableflow topics are running.

Terminal layout: **T1** commands, **T2** `make polaris-forward` (leave it running). Browser tabs: Confluent Console (topics, Tableflow, Flink) and, optionally, the S3 bucket.

```bash
source env.sh && source secrets.env
alias k="kubectl --context kind-$KIND_CLUSTER"

k get pods -A | grep -vE "Running|Completed"                         # only the header line
make connectors-status                                               # mysql-cdc-shop and postgres-cdc-shop: RUNNING
confluent tableflow topic list --cluster $CLUSTER_ID --environment $ENV_ID    # three topics, Phase RUNNING
make polaris-forward                                                 # T2
make query TABLE=orders_events                                       # T1: prints rows
k -n query exec deploy/trino-coordinator -- trino --catalog polaris --execute 'SHOW TABLES FROM "'$CLUSTER_ID'"'
```

You should see `mysql_customers`, `orders_events` and `pg_products`. In the Console, check that both Flink `INSERT` statements are `RUNNING`.

- **Polaris keeps its state in memory.** If its pod restarted since the last check, run `make bootstrap-polaris` and `make trino` first (new reader credentials).
- **Rehearse once end to end**, including the changes in Act 6, because the Tableflow commit interval decides how long you wait there.
- Reset the sample data after a rehearsal: see [Reset between runs](#reset-between-runs).

## The demo

Replace `lkc-xxxxx` with your cluster ID (`echo $CLUSTER_ID`). Quote it in SQL.

### Act 1. Three kinds of producers (2 min)

**Say:** "Three sources, nothing special: an application writing events, and two operational databases."

**Show**
- Console > Topics: `orders_events` (app events), `mysql.shop.customers` and `pg.public.products` (database changes).
- `k -n clients logs deploy/consumer --tail=3` shows the live order totals; `make connectors-status` shows the two CDC connectors running in the cluster (self-managed Debezium; they only make outbound connections).
- Console > Flink: two statements turn the raw CDC topics into clean upsert topics `mysql_customers` and `pg_products`.

**Point:** the customer keeps writing to Kafka exactly as today.

### Act 2. Tableflow (2 min)

**Say:** "Turning a topic into a table is one switch."

**Show**
- Console > topic `orders_events` > Tableflow: storage is the customer's bucket, format Iceberg, status running.
- `aws s3 ls s3://$BUCKET --recursive | head`: Iceberg metadata and parquet files in the customer's own bucket.
- `make query TABLE=orders_events` prints the snapshot count (`... : N snapshots`) before the rows. Each Tableflow commit is a new Iceberg snapshot, so the number grows between runs.

**Point:** open format, customer-owned storage. Events use append mode, the CDC topics use upsert.

### Act 3. The open catalog (3 min)

**Say:** "Engines should not need to know about Kafka, Tableflow or bucket credentials. They talk to the customer's own catalog."

**Show**
1. The engine has no AWS credentials:
   ```bash
   k -n query exec deploy/trino-coordinator -- env | grep -i aws || echo "no AWS credentials in Trino"
   ```
2. It still reads the data:
   ```bash
   k -n query exec -it deploy/trino-coordinator -- trino --catalog polaris
   ```
   ```sql
   SHOW TABLES FROM "lkc-xxxxx";
   SELECT sku, count(*) AS orders, round(sum(amount), 2) AS revenue
   FROM "lkc-xxxxx".orders_events GROUP BY sku ORDER BY sku;
   ```

**Point:** Polaris federates the Tableflow catalog, checks the principal's privileges, assumes a read-only role and hands Trino short-lived credentials for that one table. Trino read S3 directly; the data never passes through Polaris or Tableflow.

3. Optional, the Polaris UI (needs `make polaris-console` once, see [BUILD_GUIDE.md](BUILD_GUIDE.md) F3): in T2 run `make polaris-console-forward` instead, open http://localhost:8080 and sign in as `root`. Show **Catalogs > tableflow** (type `EXTERNAL`, the S3 role, the connection to Tableflow), the namespace with its three tables, and **Users & Access Control** (principal `engine_reader`, role `reader` with its five read privileges). Open it once before presenting: its screens have not been checked in a browser.

### Act 4. CDC tables (2 min)

```sql
SELECT * FROM "lkc-xxxxx".mysql_customers ORDER BY id;
SELECT * FROM "lkc-xxxxx".pg_products ORDER BY id;
```

**Point:** two operational databases, now queryable tables in the same catalog, kept in sync by upsert. `price` is a string because the connectors send decimals as text.

### Act 5. Governance (1 min)

```sql
CREATE TABLE "lkc-xxxxx".t_demo (a int);
```

Expected: `Access Denied: Cannot create table`.

**Point:** the engine's principal is read-only (privileges are set per principal and catalog role in Polaris). One catalog, many engines, one place to govern access.

### Act 6. Live changes (3 min)

**Say:** "Now the databases change. Nobody touches the pipeline."

1. Baseline, then apply the changes (T1):
   ```bash
   make demo-changes
   ```
   MySQL: Luc Martin becomes `gold`, Mia Rossi (id 5) is deleted, Zoe Lind is added. Postgres: SKU-A stock drops by 5 and SKU-E is added.
2. Go on with Act 7 while Tableflow commits (a few minutes; the first commit with rows on a quiet topic can take longer).
3. Re-run the two CDC queries from Act 4 and the order query from Act 3. Orders keep growing because the producer is still running.

**Point:** changes flow from the database to the table with no job to schedule. **Rehearse the delete:** that a delete reaches the Iceberg table through the Flink upsert decode is not yet confirmed. If it does not, present updates and inserts, and say so.

If you cannot wait: run `make demo-changes` 15 minutes before the demo and treat this act as "here is the result".

### Act 7. Close (1 min)

**Say:** "A new topic with Tableflow enabled shows up in Polaris with no extra step, and any engine that speaks the Iceberg REST API can use it: Trino here, Spark, Flink or Snowflake elsewhere."

Optional, not rehearsed: create a topic, point the producer at it (`k -n clients set env deploy/producer TOPIC=<new topic>`), enable Tableflow (`confluent tableflow topic enable <topic> --cluster $CLUSTER_ID --environment $ENV_ID --storage-type BYOS --provider-integration $PI_ID --bucket-name $BUCKET --table-formats ICEBERG`) and run `SHOW TABLES` again after the first commit.

## Likely questions

| Question | Answer |
|----------|--------|
| Why Polaris if Tableflow already has a REST catalog? | Customers that standardize on their own catalog want one governed entry point for all engines. For BYOS tables, Tableflow's catalog does not vend storage credentials, so Polaris does, and engines never get bucket access. |
| Is the data copied? | No. Tableflow writes Iceberg to the customer's bucket; engines read it there. Polaris only holds catalog state. |
| How fresh is it? | One Tableflow commit behind (a few minutes). No sync job in between. |
| Can engines write? | No. Tableflow's catalog advertises read endpoints only, and the demo principal is read-only. |
| Why not Tableflow's push sync to Polaris? | It is not available for open-source Polaris yet (INIT-10332). Federation is what works today. |
| Does it work with private networking? | Not today with Polaris or catalog integrations. |
| Does it need Confluent-managed storage? | The reverse: Polaris federation needs your own bucket (BYOS). Confluent-managed storage cannot be read through Polaris. |
| Why self-managed Debezium here? | The demo databases live in a laptop cluster that Confluent Cloud cannot reach. With databases reachable from Confluent Cloud, the fully managed CDC connectors are the usual choice. |

## When something goes wrong during the demo

| Symptom | Fix |
|---------|-----|
| Trino or PyIceberg returns 401 or no tables | Polaris restarted (in-memory state): `make bootstrap-polaris`, then `make trino` |
| Query returns no rows for a CDC table | The first commit has not happened yet; show Act 2 and come back |
| Changes not visible yet | Tableflow has not committed; continue with Act 7 and re-query at the end |
| `make query` cannot connect | `make polaris-forward` is not running in T2 |

More cases and fixes: [BUILD_GUIDE.md](BUILD_GUIDE.md), Appendix A.

## Reset between runs

`make demo-changes` is cumulative. To restore the sample data (Tableflow picks the changes up on its next commit):

```bash
source env.sh && source secrets.env
k -n data-sources exec statefulset/mysql -- sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" shop -e "
  UPDATE customers SET tier=\"standard\" WHERE id=2;
  DELETE FROM customers WHERE email=\"zoe@example.com\";
  INSERT IGNORE INTO customers (id,name,email,city,tier) VALUES (5,\"Mia Rossi\",\"mia@example.com\",\"Milan\",\"standard\");"'
k -n data-sources exec statefulset/postgres -- psql -U postgres -d shop -c "
  UPDATE products SET stock=100 WHERE sku='SKU-A'; DELETE FROM products WHERE sku='SKU-E';"
```

## Repository

```
BUILD_GUIDE.md        build and operate the environment, step by step
STATEMENT_OF_WORK.md  scope, decisions, risks, estimates
Makefile              every step as a target (make help)
apps/ connect/ k8s/ connectors/ flink/ scripts/    the pieces the guide deploys (k8s/polaris/console.yaml is the optional UI)
env.sh.example  secrets.env.example               templates (make init creates the real files)
```

`env.sh`, `secrets.env` and `.polaris-reader.env` are git-ignored: never commit them. Items marked (verify) in the docs are not confirmed end to end.
