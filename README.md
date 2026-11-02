# Tableflow + Apache Polaris: demo script

Streaming data from three kinds of producers lands as open Iceberg tables in **your own S3 bucket**, and any engine reads them through **your own Apache Polaris catalog**, with no storage credentials on the engine.

This README is the script for **running the demo** (about 15 minutes). To **build the environment**, follow [BUILD_GUIDE.md](BUILD_GUIDE.md) (Parts 0 to F). Scope, decisions and risks are in [STATEMENT_OF_WORK.md](STATEMENT_OF_WORK.md).

```
 producer app ------------------------------> orders_events  ------------------.
 MySQL  --Debezium--> mysql.shop.customers --Flink--> mysql_customers ----------+--> Tableflow --> Iceberg in S3
 Postgres --Debezium--> pg_products (compacted, no Flink) -----------------------'                      ^
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

Requirements: the environment is built ([BUILD_GUIDE.md](BUILD_GUIDE.md)) and the Debezium connectors, the MySQL Flink statement and the Tableflow topics are running.

Terminal layout: **T1** commands, **T2** `make polaris-forward` (leave it running). Browser tabs: Confluent Console (topics, Tableflow, Flink) and, optionally, the S3 bucket.

```bash
source env.sh && source secrets.env
alias k="kubectl --context kind-$KIND_CLUSTER"

k get pods -A | grep -vE "Running|Completed"                         # only the header line
make connectors-status                                               # mysql-cdc-shop and postgres-cdc-shop: RUNNING (no postgres-stores-cdc-shop yet)
confluent tableflow topic list --cluster $CLUSTER_ID --environment $ENV_ID    # three topics, Phase RUNNING; `pg_stores` must not exist yet (you create and enable it live in Act 2)
make polaris-forward                                                 # T2
make query TABLE=orders_events                                       # T1: prints rows
k -n query exec deploy/trino-coordinator -- trino --catalog polaris --execute 'SHOW TABLES FROM "'$CLUSTER_ID'"'
```

You should see `mysql_customers`, `orders_events` and `pg_products`. In the Console, check that the Flink `INSERT` (MySQL) is `RUNNING`.

- **Polaris keeps its state in memory.** If its pod restarted since the last check, run `make bootstrap-polaris` and `make trino` first (new reader credentials).
- **Rehearse once end to end**, including the changes in Act 6, because the Tableflow commit interval decides how long you wait there.
- Reset the sample data after a rehearsal: see [Reset between runs](#reset-between-runs).

## The demo

Replace `lkc-xxxxx` with your cluster ID (`echo $CLUSTER_ID`). Quote it in SQL.

### Act 1. Three kinds of producers (2 min)

**Say:** "Three sources, nothing special: an application writing events, and two operational databases."

**Show**
- Console > Topics: `orders_events` (app events), `mysql.shop.customers`, and `pg_products` (database changes). `pg_stores` does not exist yet: you create it in Act 2.
- `k -n clients logs deploy/consumer --tail=3` shows the live order totals; `make connectors-status` shows the two CDC connectors running in the cluster (self-managed Debezium; they only make outbound connections).
- Console > Flink: one statement turns the raw MySQL topic into the clean upsert topic `mysql_customers`. Postgres needs none: its connector writes the compacted topic `pg_products` directly (Console > `pg_products` > Configuration: `cleanup.policy=compact`).

**Point:** the customer keeps writing to Kafka exactly as today.

### Act 2. Tableflow (2 min)

**Say:** "Turning a topic into a table is one switch."

**Show**
- Console > topic `orders_events` > Tableflow: storage is the customer's bucket, format Iceberg, status running.
- `aws s3 ls s3://$BUCKET --recursive | head`: Iceberg metadata and parquet files in the customer's own bucket.
- `make query TABLE=orders_events` prints the snapshot count (`... : N snapshots`) before the rows. Each Tableflow commit is a new Iceberg snapshot, so the number grows between runs.

**Live: a new source, then Tableflow.** Say "a team launches a new service with its own table; let's bring it in", then:

```bash
make new-source        # new Postgres table + Debezium connector + topic pg_stores, about 30 s
```

Show the new topic in the Console (Topics > `pg_stores`), then switch it on with the command the script prints:

```bash
confluent tableflow topic enable pg_stores --cluster $CLUSTER_ID --environment $ENV_ID \
  --storage-type BYOS --provider-integration $PI_ID --bucket-name $BUCKET --table-formats ICEBERG
```

The first commit with rows took 5 to 10 minutes in rehearsal (up to 15 is possible), so **do this early** and come back to it in Act 8. Nothing of this exists before the demo (see the pre-flight list).

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

**Point:** two operational databases, now queryable tables in the same catalog, kept in sync by upsert. Two paths on purpose: MySQL goes through Flink (room for transformation), Postgres goes straight from Debezium to Tableflow. `price` is a string because the connectors send decimals as text.

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
2. Go on with Act 7 while Tableflow commits (5 to 10 minutes in rehearsal; the first commit with rows on a quiet topic can take longer).
3. Re-run the two CDC queries from Act 4 and the order query from Act 3. Orders keep growing because the producer is still running.

**Point:** changes flow from the database to the table with no job to schedule. **Rehearsed (2026-11-02):** deletes reach the Iceberg table on both paths, through the Flink upsert for MySQL and through the tombstone for Postgres. Allow the same 5 to 10 minutes as for updates and inserts.

If you cannot wait: run `make demo-changes` 15 minutes before the demo and treat this act as "here is the result".

### Act 7. A schema change (3 min)

**Say:** "A team adds a column to its table. Nobody updates the pipeline."

1. Run the change (T1):
   ```bash
   make demo-ddl
   ```
   It runs `ALTER TABLE products ADD COLUMN category TEXT` on Postgres and updates two rows (Postgres announces a new column with the next row change).
2. After the next Tableflow commit, re-run the Postgres query from Act 4 in Trino: `SELECT * FROM "lkc-xxxxx".pg_products ORDER BY id;`. The new `category` column is there, `tools` for SKU-A and SKU-B and `NULL` for the rest. Polaris needed no refresh.

**Point:** Debezium registers the new schema version, Tableflow adds the column to the Iceberg table, and Polaris serves the new schema right away. **Be honest about the edges:** only optional columns can be added (drop, rename, reorder and `NOT NULL` columns are rejected), and the MySQL path through Flink would not pick the column up by itself, because a Flink statement keeps the schema it was created with. That is the trade-off between the two paths. Repeating the act needs a new column name (`make demo-ddl COLUMN=origin`); to start over, rebuild the `pg_products` table (see Reset). **Rehearsed (2026-11-02):** the column reached Trino within 5 to 10 minutes of `make demo-ddl`.

### Act 8. Close (1 min)

**Say:** "The topic we switched on at the start is now a table in Polaris. Nobody registered it or ran a sync job, and any engine that speaks the Iceberg REST API can use it: Trino here, Spark, Flink or Snowflake elsewhere."

```sql
SHOW TABLES FROM "lkc-xxxxx";                              -- pg_stores is listed (it appeared right after the switch, empty)
SELECT * FROM "lkc-xxxxx".pg_stores ORDER BY id;           -- 3 stores once the first commit is done
```

If the rows are not there yet, show that the table exists and say the first commit is still pending. Rehearsed end to end on 2026-11-02 (rows visible after the first commit, 5 to 10 minutes).

## Likely questions

| Question | Answer |
|----------|--------|
| Why Polaris if Tableflow already has a REST catalog? | Customers that standardize on their own catalog want one governed entry point for all engines. For BYOS tables, Tableflow's catalog does not vend storage credentials, so Polaris does, and engines never get bucket access. |
| Is the data copied? | No. Tableflow writes Iceberg to the customer's bucket; engines read it there. Polaris only holds catalog state. |
| How fresh is it? | One Tableflow commit behind: 5 to 10 minutes in our rehearsals (Tableflow targets about 5, up to 15 on quiet topics). No sync job in between. |
| Can engines write? | No. Tableflow's catalog advertises read endpoints only, and the demo principal is read-only. |
| Why not Tableflow's push sync to Polaris? | It is not available for open-source Polaris yet (INIT-10332). Federation is what works today. |
| Does it work with private networking? | Not today with Polaris or catalog integrations. |
| Does it need Confluent-managed storage? | The reverse: Polaris federation needs your own bucket (BYOS). Confluent-managed storage cannot be read through Polaris. |
| Can the commit latency be lowered? | Not today: Tableflow publishes about every 5 minutes (sooner when a topic accumulates enough data) and has no customer-facing setting for commit frequency. A configurable freshness feature is planned (not available). Show the Kafka side right away and the table after the commit. |
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

`make demo-changes` is cumulative. A column added by `make demo-ddl` cannot be removed from the Iceberg table (Tableflow does not drop columns): either use a new `COLUMN=` name for the next run, or rebuild the Postgres table: disable Tableflow on `pg_products`, delete the topic and its schemas, `ALTER TABLE products DROP COLUMN category`, rerun `make connectors` and enable Tableflow again.

**The live source** (`make new-source`) is a one-shot too. To repeat Act 2 and Act 8: `make new-source-down`, run the Confluent commands it prints (disable Tableflow, delete the topic and its schemas), wait until Tableflow has finished cleaning up (re-enabling too early can be refused) and run `CONNECTOR=postgres-stores-cdc-shop2 make new-source` (a new connector name, because Debezium keeps offsets per name).

To restore the sample data (Tableflow picks the changes up on its next commit):

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
