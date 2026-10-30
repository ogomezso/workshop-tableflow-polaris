# Statement of Work: Tableflow + Self-Hosted Apache Polaris on kind with AWS S3

Demo: Kafka clients, MySQL CDC and PostgreSQL CDC (both captured by self-managed Debezium on a local kind cluster) feed Confluent Cloud on AWS. Tableflow materializes Iceberg tables in one S3 bucket. A self-hosted Apache Polaris running in kind federates to the Tableflow Iceberg REST Catalog (IRC) and serves the tables to query engines (Trino, PyIceberg).

Sources: Glean (Confluent Confluence, Jira, Slack, docs.confluent.io, GitHub) and the implemented repo (Makefile, scripts, k8s manifests). Items marked **(verify)** come from general knowledge, not Glean or a tested run. Validated in a real run on 2026-10-30 (AWS `eu-west-1`): AWS and kind setup, provider integration and Tableflow on S3, Polaris federation, PyIceberg and Trino reads through Polaris with vended credentials, Debezium MySQL/PostgreSQL snapshots, Flink decode, and Tableflow on the decoded topics. Not yet validated: UPDATE/DELETE propagation (section 10, item 5).

---

## 1. Objective and scope

**Goal.** Show that streaming data from three kinds of producers lands as open Iceberg tables, discoverable and governed through the customer's own Apache Polaris catalog. Everything except Confluent Cloud and one S3 bucket runs on the presenter's laptop in a local kind cluster, so the demo needs no cloud Kubernetes cluster, no registry and no public database endpoints.

**In scope**
- Local: a kind cluster (Docker) with namespaces `data-sources`, `clients`, `connect`, `polaris`, `query`. Images are built locally and loaded with `kind load docker-image` (no registry).
- On kind: MySQL and PostgreSQL (CDC sources, ClusterIP only, never exposed outside the cluster), Kafka client apps (producer + consumer, Avro + Schema Registry), self-managed Kafka Connect with the Debezium MySQL and PostgreSQL connectors, Apache Polaris 1.8.0 (in-memory persistence), Trino (single node).
- Kafka Connect image built from `connect/Dockerfile`: `cp-kafka-connect` plus Debezium 2.7.4.Final plugins. The connectors push to Confluent Cloud over outbound TLS and use the `ExtractNewRecordState` SMT to flatten events; deletes become tombstones.
- AWS (only these): one S3 bucket and IAM: role `tableflow-role` (Confluent assumes it through the provider integration; trust = Confluent role ARN with `sts:AssumeRole` + external ID, and `sts:TagSession`) and read-only role `polaris-reader` (Polaris assumes it to vend table-scoped credentials), reached through an IAM user whose access key lives in a Kubernetes secret, because Polaris runs outside AWS.
- Confluent Cloud on AWS: environment, Standard Kafka cluster, Schema Registry, Flink compute pool, Tableflow (BYOS), AWS provider integration.
- Makefile + shell script automation, demo script, teardown (`make down-kind down-aws` after deleting the Confluent resources).

**Out of scope**
- Private networking (Tableflow does not support private networking with Polaris or catalog integrations today).
- Production hardening, HA, an external IdP for Polaris, persistent Polaris storage, Tableflow-to-Polaris push sync (see section 3).
- Polaris Console UI as part of the core demo. It is optional (`make polaris-console`, built from `apache/polaris-tools` at a pinned commit because there is no released image); Polaris REST API + Trino/PyIceberg is the core demo.
- Fully managed Confluent CDC connectors (see decisions D3 and D9).

## 2. Target architecture

```
 Laptop (Docker)                                              Confluent Cloud on AWS (region R)
 +------------------- kind cluster ----------------------+   +----------------------------------------+
 |  MySQL (StatefulSet, ClusterIP)  --+                    |   |  Kafka (Standard) + Schema Registry    |
 |  PostgreSQL (StatefulSet, ClusterIP)--+                 |   |   orders_events        (append)        |
 |                                       v                 |   |   mysql.shop.customers (Debezium)      |
 |  Kafka Connect (Debezium MySQL + PG) ------ TLS out ------>|   |   pg.public.products   (Debezium)      |
 |  Kafka clients (producer + consumer, Avro + SR) -- TLS --->|   |  Flink: decode -> upsert topics        |
 |                                                         |   |   mysql_customers, pg_products         |
 |  Apache Polaris 1.8.0 (in-memory)  <-- Trino (REST,     |   |  Tableflow (Iceberg, BYOS)             |
 |   |        ^ read-only principal       vended creds)    |   |  Tableflow IRC                         |
 |   |        +-- PyIceberg (port-forward)                 |   +------------------+---------------------+
 |   +-- federation (outbound HTTPS, read-only) ------------------------------------^                   |
 |   +-- assumes polaris-reader via IAM user key (k8s secret), vends S3 creds                           |
 +----------------------------------------------------------+                                          |
                                                    reads (vended)                    writes (assumes tableflow-role)
                                                            v                                |
                                               AWS: one S3 bucket (same region R as the Kafka cluster) <---+
                                               IAM: tableflow-role, polaris-reader, IAM user
```

Flow: producer -> `orders_events` (append); MySQL/PG -> Debezium (kind) -> `mysql.shop.customers` / `pg.public.products` -> Flink decode to upsert topics `mysql_customers` / `pg_products` -> Tableflow (Iceberg on S3); Polaris federates the Tableflow IRC read-only (BYOS); Trino / PyIceberg read through Polaris with vended credentials.

### Design decisions and why

| # | Decision | Reason (source) |
|---|----------|-----------------|
| D1 | Polaris federates to Tableflow IRC (EXTERNAL catalog with `connectionConfigInfo`), not Tableflow push sync | Tableflow's Polaris sync API rejects any host not ending in `.snowflakecomputing.com`; OSS Polaris support is only proposed (INIT-10332, target 2027-03). Federation was validated: no sync job, new snapshots visible about 5 s after commit. Polaris only needs outbound HTTPS to Confluent and S3. |
| D2 | BYOS on S3 (bucket in the same AWS region as the Confluent Kafka cluster) | Polaris federation works for BYOS only: tables in Confluent-managed storage (CMS) cannot be read through Polaris. The Iceberg REST Catalog does not vend credentials for BYOS, so Polaris does. Same region is a Tableflow requirement. |
| D3 | Everything on a local kind cluster; cloud limited to Confluent Cloud on AWS + one S3 bucket | Cheap, fast to build and tear down, nothing billable idle on the laptop. Databases stay on ClusterIP, so no public endpoint, no allowlist and no tunnel are needed. |
| D4 | Confluent on AWS, not Azure | Creating the Confluent provider integration on Azure needs tenant-level Entra rights (Cloud Application Administrator) that standard Confluent SE accounts lack; an internal "Confluent-Testing" tenant exists but needs an IT ticket. AWS needs only IAM rights in an account where you can create roles. |
| D5 | Polaris reaches S3 through an IAM user access key (Kubernetes secret `polaris-aws`) that may only `sts:AssumeRole` the read-only `polaris-reader` role | Polaris runs outside AWS (kind), so there is no instance profile or workload identity. Polaris assumes the role and vends table-scoped credentials to engines. Validated: PyIceberg and Trino read the tables with no AWS key of their own. |
| D6 | `tableflow-role` trust policy has two statements for Confluent's role: `sts:AssumeRole` with the external ID condition, and `sts:TagSession`; set by `make confluent-storage` after the provider integration exists | `make aws` creates the role with a placeholder trust (this account); `scripts/02-confluent-storage-access.sh` replaces it. Both statements are required: with only `AssumeRole`, Tableflow fails with "Unable to assume the IAM role specified in the provider integration" (found in the real run, matches the Confluent runbook). Confluent's role can take a minute to become a valid principal, so the script retries. |
| D7 | Self-managed Debezium (Kafka Connect on kind) instead of the fully managed CDC connectors | The managed connectors run inside Confluent Cloud and cannot reach databases inside a local kind cluster. Public tunnels were rejected: ngrok needs a card on file for TCP, and bore.pub could not be reached from the corporate network (its name resolved to a block address). |
| D8 | Kafka Connect uses a custom image: `cp-kafka-connect` + Debezium 2.7.4.Final plugins (`connect/Dockerfile`), Avro converter against Confluent Schema Registry | Avro converter ships with `cp-kafka-connect`; Debezium plugins come from Maven Central. Image is loaded into kind with `kind load docker-image`. |
| D9 | Both CDC sources use the `ExtractNewRecordState` SMT (flattened events, deletes as tombstones) and then a Flink decode layer, then Tableflow | Documented recommendation for upsert tables; avoids the null-value tombstone suspend. Gives clean topic names (no dots), which Polaris needs (dotted names such as `mysql.shop.customers` fail through Polaris with `NoSuchTableException`). Replaces the former no-Flink PostgreSQL path, because self-managed Debezium topics keep dotted names. |
| D10 | Flink target topics created with `changelog.mode = 'upsert'` from the start (`flink/10_mysql_customers.sql`, `flink/20_pg_products.sql`) | Tableflow changelog mode (append vs upsert) is immutable after first materialization. Decimals use `decimal.handling.mode=string`, so `price` is a STRING. |
| D11 | Plain Kafka clients write an append-only `orders_events` topic with Avro + Schema Registry | Covers the "regular Kafka clients" requirement; simplest Tableflow mode (append). |
| D12 | Polaris uses its built-in OAuth token endpoint, in-memory persistence, and `scripts/bootstrap-polaris.sh` | Simplest for a demo. State is lost on restart, so the bootstrap is idempotent and must be re-run. Writes the reader credentials to git-ignored `.polaris-reader.env`. |
| D13 | Trino single node (coordinator only, 2 GB heap) with native S3 and vended credentials | Fits next to Polaris and Connect on a laptop; `iceberg.security=read_only`. Needs `coordinator.config.nodeScheduler.includeCoordinator=true` (otherwise queries stay `QUEUED, 0 nodes`) and `iceberg.rest-catalog.view-endpoints-enabled=false` (Tableflow's catalog has no view endpoints; otherwise `SHOW TABLES` fails with "Failed to list views"). Validated with Trino 484, chart 1.45.0. |

## 3. Risks and constraints

| Risk | Impact | Mitigation |
|------|--------|------------|
| Corporate network must allow outbound 9092 and 443 to Confluent Cloud, plus Docker Hub and Maven Central (image pulls, Debezium plugin download) | Connect, clients or image builds fail on locked-down networks | Check connectivity in Phase 1 before building; pre-pull and `kind load` images; use a network without the filter if needed. |
| Long-lived IAM user access key may be blocked by org policy (SCPs, no-IAM-user rules) | Polaris cannot assume `polaris-reader` | Request an exception or a sandbox account; the key is scoped to `sts:AssumeRole` on one role and deleted by `make down-aws`. |
| Polaris reading S3 with an IAM-user-assumed role and vended credentials (validated in the 2026-10-30 run; could differ with other org policies) | Core story could fail in another account | Phase 0 spike is a go/no-go gate (`make query`). Fallback: engines read Tableflow IRC directly; show Polaris with a pushed EXTERNAL catalog (POC sync-agent approach) as a clearly labelled preview. |
| Tableflow region availability on AWS | Region choice | `AWS_REGION` defaults to `eu-west-1`; confirm Tableflow is available there. Bucket and cluster must share the region. |
| Debezium topic and format differences vs the managed connectors (topic names `<prefix>.<db>.<table>`, flattened value schema, schema-history topic, tombstones) | Flink DDL and Tableflow behaviour differ from the managed-connector docs | Flink decode layer (D9); validate subjects in SR; keep Debezium security prefixes for the schema-history topic (see open items). |
| Laptop needs about 8 GB of Docker RAM (Polaris, Trino 2 GB heap, Connect up to 1 GB heap, two databases) | OOM kills, slow demo | Size Docker Desktop accordingly; Trino runs single node; stop the cluster between sessions. |
| Polaris persistence is in-memory | Catalog, principals and reader credentials lost on pod restart | Re-run `make bootstrap-polaris` after any Polaris restart (then re-run `make trino` if the reader credentials changed). |
| Table names with dots fail through Polaris | CDC tables invisible | Flink decode to dot-free upsert topics (D9). |
| Tableflow IRC credential vending is not done for BYOS | Polaris needs its own read access to S3 | `polaris-reader` role with read on the bucket, assumed by Polaris (D5). |
| Polaris Console has no released image and is early stage | The optional UI needs a source build (npm and Red Hat registry access) and its screens were not checked in a browser | Optional (`make polaris-console`); the API tour in BUILD_GUIDE F3 and Trino/PyIceberg cover the demo without it. |

## 4. Prerequisites

**Accounts and access**
- AWS account where you can create S3 buckets, IAM roles and IAM users (`aws sts get-caller-identity` works). `OWNER_EMAIL` in `env.sh` adds an `owner_email` tag to every AWS resource if your org requires one.
- Confluent Cloud org with AWS, Stream Governance, Flink and Tableflow enabled; permission to create environments, clusters, API keys and provider integrations.

**Tools**: `aws` CLI, `kind`, `docker`, `kubectl`, `helm`, `confluent` CLI, `make`, `jq`, `envsubst`, Python 3 (PyIceberg checks, installed into `.venv` by `make query`).

**Local resources**: about 8 GB of RAM available to Docker. Outbound internet access to Confluent Cloud (9092, 443), S3, Docker Hub and Maven Central. No AKS or other cloud Kubernetes quota is needed.

## 5. Phases, steps and acceptance criteria

Effort is for one engineer who knows Confluent Cloud and Kubernetes. Total about 7-9 working days; scripted AWS and kind setup replaces most cloud provisioning time.

### Phase 0: Spike, federation on AWS (0.5-1 day, GO/NO-GO)

1. Create the S3 bucket and IAM (`make init aws`), a Confluent AWS provider integration (`make confluent-storage`), a Standard cluster, one topic and enable Tableflow (BYOS, Iceberg).
2. Run Polaris 1.8.0 in kind with `ENABLE_CATALOG_FEDERATION=true`, mounted as the Polaris config ConfigMap.
3. Create the federated catalog (`ICEBERG_REST` connection, OAuth with Tableflow API key, scope `catalog`, no `remoteCatalogName`) with an S3 storage config whose `roleArn` is `polaris-reader` and `allowedLocations` = `s3://<bucket>/`.
4. Create a read-only principal (`TABLE_READ_DATA`); read a table with PyIceberg using `X-Iceberg-Access-Delegation: vended-credentials`.

**Acceptance**: rows readable through Polaris with an engine that has no storage credentials. If it fails, stop and pick the fallback in section 3.

### Phase 1: AWS and kind foundation (0.5 day)

1. `make init` (creates git-ignored `env.sh` and `secrets.env` with generated passwords), then `make aws`: S3 bucket (public access blocked), IAM user with `sts:AssumeRole` on `polaris-reader`, `tableflow-role` (placeholder trust), `polaris-reader` (read on the bucket), access key written to `secrets.env`.
2. `make kind namespaces`: kind cluster `tf-polaris` and the five namespaces.
3. Check outbound connectivity (Docker Hub, Maven Central, Confluent Cloud).

**Acceptance**: `kubectl get nodes` healthy; bucket and both roles exist; `secrets.env` holds the Polaris access key.

### Phase 2: Confluent Cloud foundation (0.5-1 day)

1. Manual (Console/CLI): environment (Stream Governance Essentials), Standard cluster on AWS in the same region as the bucket, Schema Registry, API keys (cluster, Schema Registry, Tableflow-scoped), and IDs recorded in `env.sh`/`secrets.env`.
2. Flink compute pool in the same region.
3. `make confluent-storage`: creates the AWS provider integration and replaces the `tableflow-role` trust policy with Confluent's role ARN (`sts:AssumeRole` with the external ID, plus `sts:TagSession`).

**Acceptance**: provider integration exists and `tableflow-role` trusts Confluent; Flink `SHOW TABLES` works.

### Phase 3: Clients and first pipeline on kind (1 day)

1. `make clients`: builds `producer:v1`, loads it into kind, creates the `confluent-client` secret, topic `orders_events` and deploys producer + consumer.
2. `make tableflow-orders`: enable Tableflow (Iceberg, BYOS) on `orders_events` (append).

**Acceptance**: messages visible in Confluent; Iceberg metadata and parquet files visible in the S3 bucket.

### Phase 4: Apache Polaris and go/no-go checkpoint (1 day)

1. `make polaris`: creates secret `polaris-aws` (IAM user key, region), applies `k8s/polaris/polaris.yaml` (Polaris 1.8.0, `ENABLE_CATALOG_FEDERATION`, in-memory).
2. `make bootstrap-polaris`: federated catalog `tableflow` (EXTERNAL, `ICEBERG_REST` to the Tableflow IRC endpoint `https://tableflow.<region>.aws.confluent.cloud/iceberg/catalog/organizations/<ORG_ID>/environments/<ENV_ID>`), S3 storage config with `polaris-reader`, principal `engine_reader` with a catalog role holding `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA`.
3. `make polaris-forward` (second terminal), then `make query TABLE=orders_events`.

**Acceptance (go/no-go)**: namespace `lkc-xxxxx` with `orders_events` visible through Polaris; PyIceberg reads rows with vended credentials; the read-only principal cannot create or drop.

### Phase 5: CDC sources, Debezium, Flink, Tableflow (1.5-2 days)

1. `make mysql postgres`: MySQL (`binlog_format=ROW`, `binlog_row_image=FULL`, GTID on, `cdc` user, table `customers`) and PostgreSQL (`wal_level=logical`, publication `dbz_publication`, table `public.products`) as StatefulSets on ClusterIP.
2. `make connect connectors`: builds `connect-debezium:v1`, loads it into kind, deploys Connect, creates the schema-history topic and PUTs `connectors/mysql-cdc.json` and `connectors/postgres-cdc.json` (SMT `ExtractNewRecordState`, tombstones on delete, `decimal.handling.mode=string`). `make connectors-status` checks them.
3. Flink (`make flink-help`): run `flink/10_mysql_customers.sql` and `flink/20_pg_products.sql` to decode `mysql.shop.customers` and `pg.public.products` into upsert topics `mysql_customers` and `pg_products`.
4. `make tableflow-cdc`: enable Tableflow (Iceberg, BYOS) on both upsert topics.

**Acceptance**: three Tableflow topics `RUNNING`; tables visible through Polaris; a delete in MySQL disappears from the table **(verify delete handling through Flink upsert)**.

### Phase 6: Query engine and demo scenarios (1 day)

1. `make trino`: Trino Helm release with the Iceberg REST connector pointing at Polaris (`vended-credentials-enabled`, native S3, `iceberg.security=read_only`, view endpoints off, coordinator schedules work); PyIceberg (`make query`) as fallback. Quote cluster-ID namespaces.
2. Prepare demo scenarios (section 7) and validate each end to end (`make demo-changes`).

**Acceptance**: each scenario runs in under 2 minutes and data freshness is visible within the Tableflow commit interval (a few minutes; the first commit with rows on a low-volume topic can take up to ~15).

### Phase 7: Automation polish, docs and teardown (1 day)

1. Makefile targets and scripts as listed in section 6; `make help` documents the order.
2. Teardown order: Tableflow topics, Flink statements, topics/schemas and the Confluent cluster/environment (and the provider integration), then `make down-kind down-aws`.
3. README with the demo script, URLs, credentials handling, cost notes.

**Acceptance**: clean build from an empty AWS account and Confluent environment in under 60 minutes; teardown leaves no billable resources.

## 6. Proposed repo layout

```
workshop-tableflow-polaris/
  apps/producer/              # Kafka producer + consumer image
  connect/Dockerfile          # cp-kafka-connect + Debezium 2.7.4.Final plugins
  connectors/                 # mysql-cdc.json, postgres-cdc.json (Debezium configs)
  k8s/
    namespaces.yaml
    clients/                  # producer + consumer
    connect/                  # Kafka Connect
    data-sources/             # MySQL, PostgreSQL
    polaris/                  # Polaris 1.8.0 (+ optional console.yaml)
    trino/                    # Helm values
  flink/                      # decode DDL/INSERT statements
  scripts/
    00-init.sh                # env.sh + secrets.env with generated values
    01-aws.sh                 # S3 bucket, IAM user, tableflow-role, polaris-reader
    02-confluent-storage-access.sh  # provider integration + trust policy
    99-down-aws.sh            # delete bucket and IAM
    bootstrap-polaris.sh      # federated catalog + read-only principal
    build-console.sh          # optional Polaris UI image (apache/polaris-tools)
    cdc-changes.sh            # demo changes in both databases
    query_polaris.py          # PyIceberg read through Polaris
    lib.sh
  env.sh.example  secrets.env.example   # templates; env.sh and secrets.env are git-ignored
  Makefile
```

## 7. Demo scenarios (about 15 minutes)

1. **Kafka clients**: producer writes events, then the table appears in Polaris with no pipeline code.
2. **MySQL CDC**: `make demo-changes` inserts, updates and deletes in `customers`; query through Trino and show the row changes (upsert).
3. **PostgreSQL CDC**: same with `products` (stock update, new SKU).
4. **Open catalog**: browse namespaces/tables through the Polaris REST API; show the table location in S3.
5. **Governance**: read-only principal can query but create/drop is rejected; one catalog, many engines.
6. **Freshness**: generate load, re-query after the Tableflow commit.
7. Optional: `ALTER TABLE ... ADD COLUMN` in a source DB and show schema evolution **(verify behaviour through Flink decode)**.

## 8. Cost and housekeeping

- Main costs: Confluent Standard cluster + Flink CFUs + Tableflow, and a tiny S3 bucket. The laptop (kind, Polaris, Trino, Connect, databases) is free. Delete the Confluent resources when idle; `make down-kind down-aws` removes everything local and on AWS.
- Tag AWS resources (`purpose=demo`, and `owner_email` when `OWNER_EMAIL` is set).
- Never commit API keys, database passwords, the Polaris root secret or the IAM user key; use git-ignored `secrets.env`, `.polaris-reader.env` and Kubernetes secrets.

## 9. Resource list (all found via Glean)

**Federation and Polaris**
- How to Federate Tableflow IRC (BYOS) into Self-Hosted Apache Polaris: https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6257872088
- Polaris sync POC and gap analysis (why push sync is blocked today): https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6287097992
- INIT-10332 OSS Polaris integration: https://confluentinc.atlassian.net/browse/INIT-10332
- FF-28625 CARIAD Polaris on AKS (customer context for the Polaris request): https://confluentinc.atlassian.net/browse/FF-28625
- Slack thread on the hostname validation problem: https://confluent.slack.com/archives/C06QKCWMEQH/p1789660217483669

**Tableflow storage and provider integrations**
- Configure Storage for Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html
- Tableflow with Azure GA Quickstart (background on the Azure path that was not chosen): https://confluentinc.atlassian.net/wiki/spaces/KORAGE/pages/5000626215
- TableFlow Azure Setup (Eric Carlier, Entra privilege notes, explains decision D4): https://docs.google.com/document/d/136RsWmAv4p1iKs3tfy6O9haaSl63on8IhB1ZyiEJK9M

**CDC**
- Materialize CDC with Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/concepts/materialize-cdc.html
- CDC to Tableflow agent skill (Flink decode pattern, immutability, cleanup order): https://github.com/confluentinc/agent-skills/blob/main/skills/confluent-cloud-cdc-tableflow/SKILL.md

**Terraform starting points (AWS provider integration and Tableflow)**
- `confluentinc/workshop-tableflow-databricks`, `terraform/` and `modules/confluent-tableflow`: https://github.com/confluentinc/workshop-tableflow-databricks
- `confluentinc/tableflow-terraform-demo`: https://github.com/confluentinc/tableflow-terraform-demo

## 10. Validation status and open items

Validated on 2026-10-30: items 1 to 4 and 6 below. Still open: items 5 and 7.

1. Polaris reading S3 with a role assumed by an IAM user (access key in a Kubernetes secret) and vending table-scoped credentials to engines. **Validated** (PyIceberg and Trino).
2. Exact Tableflow trust policy shape for `tableflow-role`. **Validated**: `sts:AssumeRole` with the external ID condition plus `sts:TagSession`.
3. Trino vended credentials with native S3 through the Iceberg REST connector. **Validated** with the two settings listed in D13.
4. Debezium schema-history topic with Confluent Cloud SASL_SSL (`schema.history.internal.producer.*` / `consumer.*` prefixes). **Validated**: both connectors `RUNNING`, MySQL snapshot completed and streaming the binlog.
5. Delete handling: MySQL/PostgreSQL deletes (tombstones) through the Flink upsert decode and into the Iceberg tables **(verify)**.
6. Tableflow availability in the chosen AWS region and whether org policy allows the IAM user key. **Validated** in `eu-west-1`; confirm again for any other region or account.
7. Status and date of INIT-10332 (Polaris push sync): if shipped, add a second, push-based catalog to the demo.
