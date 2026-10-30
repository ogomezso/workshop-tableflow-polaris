# Build Guide: Tableflow + Self-Hosted Apache Polaris on kind (AWS S3)

Hands-on, in-order runbook. Companion to `STATEMENT_OF_WORK.md` (scope, risks, estimates).

**What you end up with**

```
MySQL (kind)    --Debezium (kind)--> Kafka --Flink decode--> mysql_customers --\
Postgres (kind) --Debezium (kind)--> Kafka --Flink decode--> pg_products ------+--> Tableflow (Iceberg)
Producer app (kind) -----------------> Kafka orders_events (append) -----------/        |
                                                                                    S3 bucket
Trino / PyIceberg --> Apache Polaris (kind, federated, read-only) --> Tableflow IRC ---^ (Polaris reads S3)
```

Everything you own runs in a **local kind cluster** (Docker): MySQL, PostgreSQL, the producer/consumer apps, Kafka Connect with Debezium, Apache Polaris and Trino. The only cloud resources are **Confluent Cloud on AWS** and **one S3 bucket** (the Tableflow sink) with two IAM roles:

- `tableflow-role`: Confluent assumes it (through a provider integration) to write Iceberg files into the bucket.
- `polaris-reader`: Polaris assumes it to hand engines short-lived, table-scoped, read-only S3 credentials. Polaris runs outside AWS, so it authenticates with the access key of a small IAM user that may only `sts:AssumeRole` this role.

The databases are never exposed: Debezium runs next to them in kind and only makes **outbound** connections to Confluent Cloud.

> **The S3 bucket and the Confluent Kafka cluster must be in the same AWS region** (Tableflow requirement). Pick one region in `env.sh` (`AWS_REGION`) and use it for both. Check in the Console that Tableflow is available in that region.

**Why this design** (details in `STATEMENT_OF_WORK.md`): Azure BYOS needs tenant-level Entra rights most SE accounts lack; Confluent-managed storage cannot be read through Polaris (Polaris federation is BYOS only); Confluent's fully managed CDC connectors cannot reach a database inside a laptop cluster, and public tunnels were not an option.

**Conventions**
- Commands run from the repo root in bash/zsh. `<ANGLE_BRACKETS>` are values you copy from a previous step.
- **(verify)** = not confirmed end to end. If a step tagged like that fails, check it first.
- Every `make` target pins `kubectl` to the kind cluster, so it is safe if your current kube context points elsewhere. For your own commands use the alias from 0.3.

**Order of work.** Part C is the go/no-go spike: it proves Polaris can federate Tableflow tables using only the simple Kafka-client pipeline. Do not build Parts D-F until it passes.

---

## Part 0. Prerequisites

### 0.1 Access

| Need | For |
|------|-----|
| AWS account where you can create S3 buckets, IAM roles and IAM users (`aws sts get-caller-identity` works) | bucket and roles |
| Confluent Cloud org (permission to create environments, clusters, API keys, provider integrations) | streaming + Tableflow |
| Outbound internet: Confluent Cloud (TCP 9092 and 443), AWS APIs, Docker Hub, Maven Central | clients, Debezium, image builds |

### 0.2 Tools

```bash
brew install kind awscli          # plus docker, kubectl, helm, confluent CLI, jq, gettext (envsubst), python3
docker --version; kind version; kubectl version --client; helm version; aws --version; confluent version; jq --version; envsubst --version | head -1; python3 --version
```

`envsubst` comes from Homebrew's `gettext`, which is keg-only: after `brew install gettext` run `brew link --force gettext` so `envsubst` is on your PATH.

Docker needs **at least 8 GB RAM** (Docker Desktop > Settings > Resources): Kafka Connect, Polaris, Trino and two databases share it.

### 0.3 Shared variables

```bash
make init                     # creates env.sh and secrets.env (never overwrites existing files)
```

`make init` writes a fixed random `SUFFIX` (bucket and IAM names derive from it), generates the database passwords and the Polaris root secret, and fills `AWS_ACCOUNT_ID` when you are logged in to AWS. Then edit `env.sh`: set `AWS_REGION` (and `OWNER_EMAIL` if your org requires an owner tag; it is added as the `owner_email` tag on every AWS resource).

```bash
aws sso login                 # or however you authenticate; use --profile / export AWS_PROFILE as needed
source env.sh
alias k="kubectl --context kind-$KIND_CLUSTER"
echo $AWS_REGION $BUCKET
```

Source `env.sh` in every new shell. Never put `$(...)` in it for values that must stay fixed.

---

## Part A. AWS foundation and kind cluster

```bash
make aws                      # bucket + IAM user/roles (idempotent, safe to re-run)
make kind namespaces          # kind cluster + namespaces
```

What `make aws` does (`scripts/01-aws.sh`):
1. Creates the S3 bucket `$BUCKET` in `$AWS_REGION`, public access blocked.
2. Creates `tableflow-role` with the Tableflow write policy from the Confluent docs; its trust policy is a placeholder until B4.
3. Creates the IAM user `polaris-kind` (may only assume `polaris-reader`) and the role `polaris-reader` (read-only on the bucket).
4. Creates the user's access key and writes it to `secrets.env` as `POLARIS_AWS_ACCESS_KEY_ID` / `POLARIS_AWS_SECRET_ACCESS_KEY`. The secret is shown by AWS only once, so a re-run will not recreate it (the script tells you how to rotate it if lost).

Resource names carry your `SUFFIX` (for example `tableflow-role-7149b0`, `polaris-reader-7149b0`, user `polaris-kind-7149b0`). The new IAM user needs a few seconds before it is valid as a principal in the `polaris-reader` trust policy, so you may see one or two `attempt N/8 failed, usually IAM propagation` lines; the script retries on its own and prints `role ... created` when done.

If AWS rejects a create call because of an org policy (for example a required tag), send the exact error to whoever owns the account policy, or set `OWNER_EMAIL`.

**Checkpoint A**

```bash
aws s3api head-bucket --bucket $BUCKET && echo bucket ok
aws iam get-role --role-name $TABLEFLOW_ROLE --query Role.Arn --output text
aws iam get-role --role-name $POLARIS_ROLE --query Role.Arn --output text
grep -c "POLARIS_AWS_ACCESS_KEY_ID=AKIA" secrets.env      # 1
k get nodes ; k get ns                                      # 1 node Ready; data-sources clients connect polaris query
```

---

## Part B. Confluent Cloud foundation

### B1. Environment, cluster, Schema Registry

```bash
confluent login --save
confluent environment create tableflow-polaris-demo --governance-package essentials
```

Copy the `env-xxxxx` id into `ENV_ID` in `env.sh`, `source env.sh`, then:

```bash
confluent environment use $ENV_ID
confluent kafka cluster create demo-aws --cloud aws --region $AWS_REGION --type standard
```

Copy the `lkc-xxxxx` id into `CLUSTER_ID`, `source env.sh`, then:

```bash
confluent kafka cluster use $CLUSTER_ID
confluent kafka cluster describe $CLUSTER_ID        # Endpoint -> KAFKA_BOOTSTRAP in secrets.env (without the SASL_SSL:// prefix)
confluent schema-registry cluster describe          # Cluster ID (lsrc-...) -> for the API key; "Endpoint URL" (psrc-...) -> SR_URL in secrets.env.
                                                    # Do NOT use the private endpoint (...aws.private.confluent.cloud): it does not resolve from your laptop
confluent organization describe                     # ID -> ORG_ID in env.sh
```

### B2. API keys

```bash
confluent api-key create --resource $CLUSTER_ID --description "demo kafka"              # -> KAFKA_KEY / KAFKA_SECRET
confluent api-key create --resource <lsrc-xxxxx> --description "demo schema registry"   # -> SR_KEY / SR_SECRET
confluent api-key create --resource tableflow --description "demo tableflow"            # -> TF_KEY / TF_SECRET (used by Polaris)
```

Each secret is shown once. Put all six values (plus `KAFKA_BOOTSTRAP` and `SR_URL`) in `secrets.env`.

### B3. Flink compute pool

```bash
confluent flink compute-pool create demo-pool --cloud aws --region $AWS_REGION --max-cfu 10
```

Copy the `lfcp-xxxxx` id into `POOL_ID` in `env.sh`.

### B4. Provider integration so Tableflow can write to your bucket (BYOS)

```bash
source env.sh && source secrets.env
make confluent-storage
```

`scripts/02-confluent-storage-access.sh` creates an AWS provider integration for `tableflow-role`, reads Confluent's IAM role ARN and external ID from it, and replaces the placeholder trust policy of `tableflow-role` with two statements for Confluent's role: `sts:AssumeRole` only with that external ID, and `sts:TagSession`. Both are required: with only AssumeRole, Tableflow fails with "Unable to assume the IAM role specified in the provider integration". It stores `PI_ID`, `CONFLUENT_IAM_ROLE_ARN` and `CONFLUENT_EXTERNAL_ID` in `env.sh`. Two things you will see: the CLI prints a deprecation notice for `provider-integration` (harmless: v1 is the only version that supports AWS), and AWS may answer `Invalid principal` for a minute because Confluent's role is not yet valid in your account; the script retries for about 3 minutes (you can also just rerun it, it reuses the integration). If it cannot parse the CLI output it prints it and asks you to copy the two values from Console > Provider integrations into `env.sh`, then run it again.

**Checkpoint B**

```bash
source env.sh && source secrets.env
confluent flink compute-pool describe $POOL_ID --environment $ENV_ID        # RUNNING / PROVISIONED
confluent provider-integration describe $PI_ID --environment $ENV_ID
aws iam get-role --role-name $TABLEFLOW_ROLE --query Role.AssumeRolePolicyDocument
```

The trust policy must have the `sts:AssumeRole` statement (Confluent's role plus your external ID) and the `sts:TagSession` statement. Enabling Tableflow in C2 validates the whole chain; if it fails with an `AssumeRole` error, compare the trust policy with the one shown in Console > Provider integrations.

---

## Part C. First pipeline and Polaris federation (GO / NO-GO)

Goal: producer on kind, then Tableflow, then Polaris on kind, then read rows with an engine that has no storage credentials.

### C1. Topic and producer app

```bash
source env.sh && source secrets.env
make clients            # builds the producer image, loads it into kind, creates topic orders_events, deploys producer + consumer
k -n clients get pods
k -n clients logs deploy/producer --tail=3
```

The producer prints `delivered=20 ...` every 20 confirmed deliveries (and `delivery failed` on errors); the consumer prints a running total, for example `orders=100 revenue=...`. Check in the Console that `orders_events` has messages and an Avro schema. The same image runs a consumer in a consumer group (`deploy/consumer`) to show regular consumption next to Tableflow.

### C2. Enable Tableflow (Iceberg, BYOS on S3)

```bash
make tableflow-orders
confluent tableflow topic describe orders_events --cluster $CLUSTER_ID --environment $ENV_ID | grep -E "Phase|Suspended|Error"
aws s3 ls s3://$BUCKET --recursive | head       # metadata and parquet appear after the first commit (a few minutes)
```

The phase starts as `PENDING` and should become `RUNNING`. If it becomes `FAILED` with "Unable to assume the IAM role specified in the provider integration", see Appendix A (trust policy).

### C3. Verify the Tableflow Iceberg REST Catalog (IRC) directly

`IRC` is built in `env.sh` from `AWS_REGION`, `ORG_ID` and `ENV_ID`:

```bash
source env.sh && source secrets.env
TOKEN=$(curl -s -X POST "$IRC/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=$TF_KEY -d client_secret=$TF_SECRET -d scope=catalog | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" "$IRC/v1/namespaces" | jq
```

You should see a namespace equal to your cluster ID (`lkc-xxxxx`). Tableflow tokens last 900 seconds.

### C4. Deploy Apache Polaris on kind

```bash
make polaris
```

Creates the `polaris-aws` secret (the IAM user's key and the region) and applies `k8s/polaris/polaris.yaml` (Polaris 1.8.0, in-memory persistence, catalog federation enabled through `/deployments/config/application.properties`).

### C5. Bootstrap the federated catalog and a read-only principal

```bash
make bootstrap-polaris
```

`scripts/bootstrap-polaris.sh` opens its own port-forward if needed, gets the root token, creates the federated `EXTERNAL` catalog `tableflow` and a principal `engine_reader` with a read-only catalog role (`TABLE_READ_DATA` is required for Polaris to vend credentials), and writes the reader credentials to `.polaris-reader.env` (git-ignored). The catalog storage config is S3 with `roleArn = $POLARIS_ROLE_ARN`, `region = $AWS_REGION` and `allowedLocations = s3://$BUCKET/`.

Rules the script follows (Polaris federation how-to): leave `remoteCatalogName` unset (Tableflow IRC accepts only an empty prefix) and keep `default-base-location` inside `allowedLocations`. Rerun it after every Polaris restart. The script never updates an existing catalog (a `409` is treated as success): to change a setting such as the bucket or the Tableflow key, restart Polaris (`k -n polaris rollout restart deploy/polaris`, state is in memory) and rerun `make bootstrap-polaris`.

### C6. Read through Polaris with an engine that has no storage credentials

```bash
make polaris-forward        # terminal 2, keep it running
make query TABLE=orders_events
```

The result also shows Tableflow's metadata columns (`$$topic`, `$$partition`, `$$offset`, `$$raw-value`...) next to your own; they are expected. `make query` creates `.venv`, installs PyIceberg and runs `scripts/query_polaris.py` with the reader credentials and `X-Iceberg-Access-Delegation: vended-credentials`.

**Checkpoint C / GO-NO-GO**: rows from `orders_events` print, using no AWS key of yours. If you hit a problem:

| Symptom | Cause | Fix |
|---------|-------|-----|
| `406 Feature not enabled: ENABLE_CATALOG_FEDERATION` | Config file not read | Mount path must be exactly `/deployments/config/application.properties` |
| Catalog creation fails (connection check) | Wrong `IRC`, `TF_KEY`/`TF_SECRET`, or key owner cannot read the topics | Rerun C3 with the same values |
| `403 ... not authorized for op LOAD_TABLE_WITH_READ_DELEGATION` | Principal lacks `TABLE_READ_DATA` | Rerun `make bootstrap-polaris` after a Polaris restart (state is in memory) |
| `403 ... locations outside the catalog's allowed locations` | Bucket not in `allowedLocations` | Fix `BUCKET` and recreate the catalog |
| loadTable OK, then S3 `AccessDenied` | Polaris cannot assume `polaris-reader`, or the role cannot read the bucket | `k -n polaris exec deploy/polaris -- env \| grep AWS_` (key and region present?); check the role trust names the `polaris-kind` user and the user policy allows `sts:AssumeRole` on the role (verify) |
| `MalformedPolicyDocument: Invalid principal` while creating the role | IAM propagation | `make aws` retries by itself; rerun it |
| Empty namespace list | Tableflow has not committed yet | Wait a few minutes; check Tableflow status |

**Fallback if federation cannot be made to work**: query the Tableflow IRC directly (Trino/PyIceberg against `$IRC` with the Tableflow key) and present Polaris as the roadmap (push sync is still blocked: INIT-10332). Decide before continuing.

---

## Part D. MySQL CDC (Debezium on kind, Flink decode)

### D1. Databases

```bash
make mysql postgres
k -n data-sources get pods,svc          # both Running; services are ClusterIP only
```

MySQL runs with `log_bin`, ROW format and GTID; Postgres with `wal_level=logical`, a `cdc` user with `REPLICATION` and the publication `dbz_publication`. The init SQL only runs on an empty volume. Verify:

```bash
k -n data-sources exec statefulset/mysql -- sh -c 'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -e "SHOW VARIABLES LIKE \"log_bin\"; SELECT * FROM shop.customers;"'
k -n data-sources exec statefulset/postgres -- psql -U postgres -d shop -c "SHOW wal_level;" -c "SELECT * FROM public.products;"
```

Expected: `log_bin ON` with 5 customers; `wal_level logical` with 4 products.

### D2. Kafka Connect with Debezium

```bash
make connect
```

Builds `connect/Dockerfile` (Confluent `cp-kafka-connect` plus the Debezium MySQL and PostgreSQL plugins from Maven Central), loads it into kind and deploys `k8s/connect/connect.yaml`: a single worker whose internal topics, converters (Avro with Schema Registry) and clients all use SASL_SSL to Confluent Cloud. The first build downloads about 1.5 GB, and loading the 3.7 GB image into kind takes a few minutes.

```bash
k -n connect logs deploy/connect --tail=20
k -n connect exec deploy/connect -- curl -s localhost:8083/connector-plugins | jq -r '.[].class' | grep -i debezium
```

You should see the MySQL and PostgreSQL connector classes.

### D3. Create the connectors

```bash
make connectors
make connectors-status
```

`connectors/mysql-cdc.json` and `connectors/postgres-cdc.json` are rendered with `envsubst` in memory (secrets never touch disk) and sent to the Connect REST API. Both use the `ExtractNewRecordState` SMT so each topic carries the flat row (the format the Flink statements in `flink/` expect) and a delete becomes a tombstone. Topic names are `<topic.prefix>.<db>.<table>`: `mysql.shop.customers` and `pg.public.products`. `make connectors` prints only the HTTP status, because the Connect REST API returns the full connector config (it contains the Kafka API secret in the schema-history settings and the database password). Do not paste `curl` output of `/connectors/<name>` anywhere; the configs also live in Connect's config topic, so treat the demo cluster as disposable. Wait 10 to 20 seconds before `make connectors-status`: right after the PUT the worker has not applied the config yet and the answer is `{}`. Both connectors should report `RUNNING`; the first snapshot takes a minute. MySQL also needs the schema-history topic `debezium-schema-history-mysql` (created by `make connectors`, otherwise by Debezium).

Check in the Console that both topics exist with Avro schemas.

### D4. Flink decode into an upsert topic (clean name, no dots)

Run `flink/10_mysql_customers.sql` in the Console Flink SQL workspace, **one statement at a time** (first the `CREATE TABLE`, then the `INSERT`; pasting both at once fails with "Only a single statement is supported at a time") (catalog = your environment, database = your cluster) or `make flink-help` for the CLI shell:

```sql
CREATE TABLE `mysql_customers` (
  `id` INT NOT NULL, `name` STRING, `email` STRING, `city` STRING, `tier` STRING,
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

INSERT INTO `mysql_customers`
SELECT `id`, `name`, `email`, `city`, `tier` FROM `mysql.shop.customers`;
```

Rules from the Confluent CDC guidance:
- The CDC topic appears in Flink automatically (no source DDL). Do not set connector/format/bootstrap options on `CREATE TABLE`.
- Create the target with `changelog.mode = 'upsert'` **from the start**. It is immutable after the first materialization.
- Do not enable Tableflow directly on `mysql.shop.customers`: deletes produce tombstones that suspend an append-mode table, and dotted names fail through Polaris.
- Whether deletes reach the Iceberg table through this decode is not confirmed (verify with the delete in `make demo-changes`).

### D5. Enable Tableflow on the decoded topic

```bash
make tableflow-cdc TOPICS=mysql_customers
```

**Checkpoint D**: after the first commit with data, `make query TABLE=mysql_customers`. Right after enabling Tableflow the table already exists in Polaris but is empty (two snapshots without rows); the first commit with rows can take 5 to 15 minutes on a low-volume topic. Then:

```bash
make demo-changes
```

After the next Tableflow commit (a few minutes) id=2 shows `gold`, Zoe is present, and id=5 should be gone (verify).

---

## Part E. PostgreSQL CDC

The Postgres connector was created in D3. Run `flink/20_pg_products.sql`:

```sql
CREATE TABLE `pg_products` (
  `id` INT NOT NULL, `sku` STRING, `name` STRING, `price` STRING, `stock` INT,
  PRIMARY KEY (`id`) NOT ENFORCED
) WITH ('changelog.mode' = 'upsert');

INSERT INTO `pg_products`
SELECT `id`, `sku`, `name`, `price`, `stock` FROM `pg.public.products`;
```

`price` is a string because of `decimal.handling.mode=string`; cast it in Flink if you want a numeric type.

```bash
make tableflow-cdc TOPICS=pg_products
```

**Checkpoint E**: `make demo-changes` also changes Postgres; `make query TABLE=pg_products` shows the change after the next commit.

---

## Part F. Trino on kind

### F1. Install Trino with a Polaris catalog

```bash
make trino
```

Renders `k8s/trino/values.yaml` with the reader credentials from `.polaris-reader.env` and installs the Trino chart as a single node (`coordinator.config.nodeScheduler.includeCoordinator=true`, 2 GB heap; `workers: 0` alone does not let the coordinator run queries). The catalog uses `iceberg.rest-catalog.vended-credentials-enabled=true` and native S3 (`fs.native-s3.enabled`, `s3.region`), so Trino holds no AWS key. `iceberg.rest-catalog.view-endpoints-enabled=false` is required: Tableflow's catalog has no view endpoints and without it `SHOW TABLES` fails with "Failed to list views". Validated with Trino 484 and chart 1.45.0.

### F2. Query

```bash
k -n query exec -it deploy/trino-coordinator -- trino --catalog polaris
```

```sql
SHOW SCHEMAS;
SHOW TABLES FROM "lkc-xxxxx";                         -- quote the cluster ID
SELECT * FROM "lkc-xxxxx".mysql_customers ORDER BY id;
SELECT sku, count(*) c, sum(amount) total FROM "lkc-xxxxx".orders_events GROUP BY sku;
SELECT * FROM "lkc-xxxxx".pg_products;
```

Writes must fail (`CREATE TABLE`, `DROP TABLE`): the catalog is read-only by design. If Polaris was restarted, rerun `make bootstrap-polaris` and `make trino` (new reader credentials).

### F3. Optional: Polaris Console UI

The Polaris server is API-only and this repo does not deploy a UI: the Console in `apache/polaris-tools` is early stage with no released image. If you want it, build it from source with an nginx proxy same-origin with Polaris and load it with `kind load docker-image` (see "Appendix D: Optional web UI" in the Polaris federation how-to). The demo does not need it.

---

## Part G. Demo run-through (about 15 minutes)

Keep the producer running. Tableflow commits every few minutes, so run `make demo-changes` about 5 minutes before the live part.

| # | Show | How |
|---|------|-----|
| 1 | The three source types | Console: topics `orders_events`, `mysql.shop.customers`, `pg.public.products`; Debezium in kind (`make connectors-status`); Flink statements |
| 2 | Tableflow materialization | Console Tableflow tab; the S3 bucket with Iceberg metadata and parquet files |
| 3 | Polaris catalog | `make polaris-forward` + `make query` (namespaces and tables through the Polaris REST API) |
| 4 | Query through the open catalog | Trino queries from F2; the engine has no storage credentials, Polaris vends them |
| 5 | CDC changes | `make demo-changes`; re-query after the commit |
| 6 | Governance | Reader principal can query; `CREATE TABLE`/`DROP TABLE` rejected (406/403) |
| 7 | New table appears automatically | Enable Tableflow on one more topic and `SHOW TABLES` again: no sync job, no Polaris change |

---

## Part H. Teardown

Order matters; Confluent side first, otherwise Tableflow can recreate objects in the bucket:

```bash
source env.sh && source secrets.env
for t in orders_events mysql_customers pg_products; do
  confluent tableflow topic disable $t --cluster $CLUSTER_ID --environment $ENV_ID
done
confluent flink statement list --environment $ENV_ID                       # delete the two INSERT statements
confluent flink statement delete <statement-name> --environment $ENV_ID
confluent provider-integration delete $PI_ID --environment $ENV_ID
confluent environment delete $ENV_ID                                       # also removes the cluster, topics, schemas, pool

make down-kind                  # deletes the kind cluster (databases, Connect, Polaris, Trino)
make down-aws                   # empties and deletes the bucket, deletes the IAM user and roles (asks for confirmation)
```

Also delete the API keys you created (`confluent api-key list`).

To build everything again from scratch after a teardown, the values in `env.sh` and `secrets.env` that pointed at deleted resources are stale (`ENV_ID`, `CLUSTER_ID`, `POOL_ID`, `ORG_ID`, `PI_ID`, `CONFLUENT_IAM_ROLE_ARN`, `CONFLUENT_EXTERNAL_ID`, all Confluent keys, `POLARIS_AWS_*`, `.polaris-reader.env`). Simplest: move both files away (`mv env.sh env.sh.old; mv secrets.env secrets.env.old`, delete them when done) and start again at `make init`; a stale `PI_ID` makes `make confluent-storage` try to reuse an integration that no longer exists. If provider-integration deletion returns 409 while something still uses it, disable Tableflow on all topics first.

Between sessions: the kind cluster survives a Docker restart and keeps database data (volumes). Polaris state is in memory, so after any Polaris pod restart rerun `make bootstrap-polaris`, then `make trino` with the new reader credentials. Debezium keeps its offsets in Kafka and resumes by itself. To free RAM without deleting anything: `docker stop $KIND_CLUSTER-control-plane` and `docker start` it later.

---

## Appendix A. Troubleshooting quick reference

| Symptom | Cause | Fix |
|---------|-------|-----|
| `make kind` or image load fails with "content digest ... not found" | `kind load` and Docker's containerd image store | `docker save <image> -o /tmp/i.tar && kind load image-archive /tmp/i.tar --name $KIND_CLUSTER` |
| Producer fails with `Name or service not known` (value serialization) | `SR_URL` is the private Schema Registry endpoint (`...aws.private.confluent.cloud`) | Use the public "Endpoint URL" (`psrc-...aws.confluent.cloud`), then `make confluent-secret` and `k -n clients rollout restart deploy/producer deploy/consumer` (same for `deploy/connect` in namespace `connect`) |
| Pods `Pending` or OOM-killed | Docker has too little RAM | Raise Docker memory to 8 GB or more; if tight, `helm uninstall trino -n query --kube-context kind-$KIND_CLUSTER` while you do not need it |
| `connect` pod crash-loops | Wrong Confluent credentials/bootstrap, or egress to 9092 blocked | `k -n connect logs deploy/connect`; recheck `secrets.env` and rerun `make connect`; test `nc -vz <bootstrap host> 9092` from your laptop |
| `curl: (22)` / 4xx from `make connectors` | Connect not ready or bad config | `k -n connect logs deploy/connect`; the error body is printed by the command |
| Connector `FAILED`: cannot connect to the database | DB pod not ready, wrong `cdc` password | `k -n data-sources get pods`; the passwords come from `secrets.env` at the time of `make mysql postgres` (recreate the secret and delete the pod after changing them) |
| MySQL connector: schema history / timeout errors | History topic security settings or permissions | Check `schema.history.internal.producer/consumer.*` in `connectors/mysql-cdc.json` and the API key permissions |
| MySQL connector: binlog/GTID errors | Settings missing | Re-run the `SHOW VARIABLES` check from D1 |
| Postgres connector: slot/publication errors | Publication missing or no REPLICATION | Init script runs only on an empty volume; recreate the volume or create them by hand |
| Tableflow `FAILED`: "Unable to assume the IAM role specified in the provider integration" | Trust policy lacks `sts:TagSession`, or has a wrong principal/external ID, or IAM propagation | Rerun `make confluent-storage` (writes both statements), then disable and re-enable Tableflow on the topic. Compare `aws iam get-role --role-name $TABLEFLOW_ROLE` trust with Console > Provider integrations; wait 1-2 minutes and retry |
| Tableflow rejects the bucket | Region differs from the cluster | Bucket and cluster must share a region; recreate whichever is wrong |
| Tableflow `Suspended`: "detected a Kafka record with a null value" | Tableflow enabled directly on a CDC topic | Use the Flink decode topic |
| "changelog mode ... has been modified since table materialization began" | Mode changed after first materialization | Delete the Tableflow topic and Kafka topic, recreate with the right mode |
| Table invisible through Polaris, `NoSuchTableException` | Topic name has dots (`server.db.table`) | Use the dot-free decoded topics |
| Trino query stays `QUEUED, 0 nodes` | Coordinator does not schedule work on itself | Set `coordinator.config.nodeScheduler.includeCoordinator: true` in `k8s/trino/values.yaml` and rerun `make trino` |
| Trino `Failed to list views` / `Failed to load view` | Tableflow IRC has no view endpoints | `iceberg.rest-catalog.view-endpoints-enabled=false` in the Trino catalog (already set) |
| `make connectors-status` prints `{}` | Queried right after the PUT, before the worker applied the config | Wait 10 to 20 seconds and repeat |
| Flink: "Only a single statement is supported at a time" | Several statements run at once | Run the `CREATE TABLE` and the `INSERT` separately |
| `mysql_customers` / `pg_products` exist in Polaris but return no rows | First Tableflow commit with data has not happened yet (two empty snapshots) | Wait 5 to 15 minutes and rerun `make query`; confirm the Flink `INSERT` is `RUNNING` and that `confluent kafka topic consume mysql_customers --from-beginning --value-format avro ...` shows rows |
| `make confluent-storage`: `Invalid principal` for Confluent's role | Confluent's role is not yet valid in your account | The script retries for ~3 minutes; otherwise rerun it |
| Polaris returns 401 after a restart | In-memory state lost | `make bootstrap-polaris`, then `make trino` |
| `aws iam create-access-key` denied or keys forbidden by org policy | Long-lived IAM user keys blocked | Ask for an exception, or run Polaris with temporary credentials (session token) refreshed by hand (verify) |
| Rows missing right after changes | Tableflow commit interval | Wait for the next commit (a few minutes; low-throughput topics can take up to 15) |
| `pip` fails building pyarrow on your Python | Very new Python without wheels | Use Python 3.12 for `.venv` (`mise use python@3.12`, delete `.venv`, rerun `make query`) |

## Appendix B. Repo layout

```
apps/producer/            Dockerfile, producer.py, consumer.py, requirements.txt
connect/                  Dockerfile: Kafka Connect + Debezium MySQL/PostgreSQL
k8s/                      namespaces, clients, connect, data-sources, polaris, trino manifests
connectors/               mysql-cdc.json, postgres-cdc.json (Debezium configs with ${VAR} placeholders)
flink/                    10_mysql_customers.sql, 20_pg_products.sql
scripts/                  00-init.sh, 01-aws.sh, 02-confluent-storage-access.sh, 99-down-aws.sh,
                          bootstrap-polaris.sh, cdc-changes.sh, query_polaris.py, lib.sh
Makefile                  shortcuts for every part (make help)
env.sh, secrets.env       local variables and secrets (git-ignored; templates: *.example)
```

## Appendix C. Sources (Glean)

- How to Federate Tableflow IRC (BYOS) into Self-Hosted Apache Polaris (validated on AWS S3, Polaris 1.8.0): https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6257872088
- Tableflow catalog sync to self-hosted Polaris, POC and gap analysis: https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6287097992
- Configure Storage for Tableflow (S3 permission policy, same-region rule): https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html
- Tableflow quick start with your own storage on AWS: https://docs.confluent.io/cloud/current/topics/tableflow/get-started/quick-start-custom-storage-glue.html
- Materialize CDC with Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/concepts/materialize-cdc.html
- CDC to Tableflow agent skill (Flink decode, immutability, cleanup order): https://github.com/confluentinc/agent-skills/blob/main/skills/confluent-cloud-cdc-tableflow/SKILL.md
- Query Tableflow with Trino: https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-trino.html
- Why Azure was dropped (tenant-level rights, "Confluent-Testing" tenant): https://confluentinc.atlassian.net/wiki/spaces/~457145999/pages/5089525911
- Why Polaris push sync is blocked today (INIT-10332): https://confluentinc.atlassian.net/browse/INIT-10332
