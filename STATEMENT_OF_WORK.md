# Statement of Work: Tableflow + Self-Hosted Apache Polaris on kind

Demo: Kafka clients, MySQL CDC (fully managed) and PostgreSQL CDC feed Confluent Cloud on Azure. Tableflow materializes Iceberg tables in ADLS Gen2. A self-hosted Apache Polaris on a local kind cluster federates to the Tableflow Iceberg REST Catalog (IRC) and serves the tables to query engines.

Sources: Glean (Confluent Confluence, Jira, Slack, docs.confluent.io, GitHub). Items marked **(verify)** come from general knowledge, not Glean, and must be confirmed in the Phase 0 spike.

---

## 1. Objective and scope

**Goal.** Show that streaming data from three kinds of producers lands as open Iceberg tables, discoverable and governed through the customer's own Apache Polaris catalog, with all Kubernetes workloads running on a local kind cluster and Azure used only for storage, identity and Confluent Cloud.

**In scope**
- Azure (only): resource group, ADLS Gen2 (Tableflow sink), one service principal (client secret; `Storage Blob Data Reader` + `Storage Blob Delegator`) used by Polaris via `AZURE_*` env vars.
- Local: kind cluster, Docker, ngrok TCP tunnels for the CDC connectors; images built locally and loaded with `kind load docker-image` (no registry).
- Confluent Cloud on Azure: Kafka, Schema Registry, Flink, 2 fully managed CDC connectors, Tableflow (BYOS).
- On kind: MySQL, PostgreSQL (CDC sources), Kafka client apps, Apache Polaris (+ Console), Trino (+ optional Jupyter/PyIceberg).
- Helm + Makefile + shell script automation, demo script, teardown.

**Out of scope**
- Private networking (Tableflow does not support private networking with Polaris or catalog integrations today).
- Production hardening, HA, Entra ID as Polaris IdP, Tableflow-to-Polaris push sync (see section 3).

## 2. Target architecture

```
                         Local machine (Docker + kind)
 +------------------------- kind cluster --------------------------------+
 |  MySQL (StatefulSet) --+                                              |
 |   NodePort 30306       |                                              |
 |  PostgreSQL (StatefulSet)+--> [ngrok TCP tunnels (make tunnels)]       |
 |   NodePort 30432       |           |                                  |
 |  Kafka client apps (producer + consumer, Avro + SR)  --+              |
 |                                                         |             |
 |  Apache Polaris (Helm) <-- Polaris Console             |  outbound    |
 |   |  ^ read-only principal                              |  HTTPS       |
 |   |  +-- Trino (Iceberg REST, vended creds) / PyIceberg |              |
 |   +--- federation (outbound HTTPS) ---------------------+-----+        |
 |   +--- reads ADLS with SP secret (AZURE_* env, SAS vending)      |        |
 +-----------------------------------------------------------------------+
            |                                                    |
            v                                                    v
   Azure: ADLS Gen2 (HNS on, same region)  <-- writes --  Confluent Cloud (Azure, same region)
                                                    - Kafka + Schema Registry
                                                    - MySQL CDC Source V2 (managed) --> pulls via ngrok from kind MySQL
                                                    - PostgreSQL CDC Source V2 (managed) --> pulls via ngrok from kind PG
                                                    - Flink (decode MySQL CDC -> upsert topic)
                                                    - Tableflow (Iceberg, BYOS) + Tableflow IRC
```

### Design decisions and why

| # | Decision | Reason (source) |
|---|----------|-----------------|
| D1 | Polaris federates to Tableflow IRC (EXTERNAL catalog with `connectionConfigInfo`), not Tableflow push sync | Tableflow's Polaris sync API rejects any host not ending in `.snowflakecomputing.com`; OSS Polaris support is only proposed (INIT-10332, target 2027-03). Federation was validated: no sync job, new snapshots visible about 5 s after commit. Polaris only needs outbound HTTPS, so no WAF/App Gateway. |
| D2 | BYOS on ADLS Gen2 (HNS enabled, same region as the Confluent cluster) | Confluent-managed storage tables cannot be read through Polaris (Polaris issues its own storage credentials). |
| D3 | MySQL and PostgreSQL run on kind as NodePort services, exposed to the fully managed CDC connectors through ngrok TCP tunnels (`make tunnels`) | Keeps the managed connectors (the demo point) without Azure LB/VNet or a Confluent egress IP allowlist. Trade-off: the DBs are publicly reachable through the tunnel, so use strong passwords, TLS if feasible and a short-lived demo. ngrok free-tier TCP tunnel limits (count, account verification, host/port change on restart) **(verify)**. |
| D4 | MySQL CDC: Flink decode layer, then upsert topic, then Tableflow | Documented recommendation for upsert tables from Debezium; avoids the null-value tombstone suspend. Gives clean topic names (no dots), which Polaris needs (dotted names such as `server.db.table` fail through Polaris with `NoSuchTableException`). |
| D5 | PostgreSQL CDC: `after.state.only=true` + `tombstones.on.delete=true`, Tableflow directly (upsert) | Shows the second documented path (no Flink). Topic names still contain dots, so rename with a RegexRouter SMT **(verify)** or switch Postgres to the Flink path if the SMT conflicts. |
| D6 | Plain Kafka clients write an append-only `orders`-style topic with Avro + Schema Registry | Covers the "regular Kafka clients" requirement; simplest Tableflow mode (append). |
| D7 | Polaris uses its built-in OAuth token endpoint and a bootstrap Job | Simplest for a demo. Entra ID as IdP is a later option (CARIAD plans this). |
| D8 | Service principal with client secret for Polaris to ADLS, passed as `AZURE_TENANT_ID`/`AZURE_CLIENT_ID`/`AZURE_CLIENT_SECRET` env vars | A local kind cluster has no OIDC issuer, so Workload Identity is not available; a secret-based SP is the simple alternative. **(verify)** Polaris Azure `azureConfigInfo` and SAS vending behaviour with SP credentials. |

Note: Tableflow changelog mode (append vs upsert) is immutable after first materialization. Create the Flink target topics with `changelog.mode = 'upsert'` from the start.

## 3. Risks and constraints

| Risk | Impact | Mitigation |
|------|--------|------------|
| Federation validated only on AWS S3; Azure path (ADLS, SAS vending, `abfss://` allowedLocations) untested in Glean | Core story could fail | Phase 0 spike is a go/no-go gate. Fallback: engines read Tableflow IRC directly; show Polaris with a pushed EXTERNAL catalog (POC sync-agent approach) as a clearly labelled preview. |
| Entra tenant privileges: Tableflow BYOS provider integration needs `az ad sp create`-level rights; SEs have been blocked here | Phase 2 blocked | Request a tenant/subscription with Application Administrator (or ask IT for the Confluent-Testing tenant) before starting. |
| Polaris Console has no released image (early-stage `apache/polaris-tools`) | Extra build step | Build from source, load into kind. Treat as optional; Polaris REST API + Trino is the core demo. |
| Table names with dots fail through Polaris | Postgres topics invisible | Flink decode or RegexRouter SMT (D4, D5). |
| Tableflow IRC credential vending is not done for BYOS | Polaris needs its own read access to ADLS | Grant the Polaris service principal `Storage Blob Data Reader` + `Storage Blob Delegator` on the account **(verify roles)**. |
| ngrok TCP tunnels are public, change on restart and are subject to free-tier limits | DBs exposed; connectors break on host/port change | Strong passwords, short demo window, update `env.sh` and the connectors after any `make tunnels` restart; consider alternatives (see open items) **(verify limits)**. |
| SP client secret stored in a Kubernetes secret and expires | Credential leakage / Polaris loses ADLS access | Git-ignored `secrets.env`, short secret lifetime, delete the SP at teardown. |
| Tableflow Azure region availability | Region choice | Use `westeurope` (documented in Azure quickstart examples) and confirm in Confluent regions list. |

## 4. Prerequisites

**Accounts and access**
- Azure subscription with Contributor and permission to create role assignments and app registrations/service principals (see risk above).
- Confluent Cloud org with Azure, Stream Governance, Flink, Tableflow enabled. Cloud API key/secret.
- Entra tenant ID for the provider integration.

**Local tools**: `az`, `kubectl`, `helm`, `kind`, `docker`, `ngrok` (with auth token), `confluent` CLI, `make`, `jq`, Python 3.11 (producer + PyIceberg checks).

**Local resources**: a machine with Docker and about 8 GB RAM free for kind; outbound internet access (ngrok, Confluent Cloud, ADLS).

## 5. Phases, steps and acceptance criteria

Effort is for one engineer who knows Confluent Cloud and Kubernetes. Total about 8-10 working days (similar; kind removes AKS provisioning, ngrok tunnel setup adds a little).

### Phase 0: Spike, federation on Azure (0.5-1 day, GO/NO-GO)

1. Create ADLS Gen2 account + container, a Confluent Azure provider integration, a Standard cluster, a Datagen topic and enable Tableflow (BYOS, Iceberg).
2. Run Polaris 1.8.0+ locally in Docker with `ENABLE_CATALOG_FEDERATION=true`, mounted at `/deployments/config/application.properties`.
3. Create the federated catalog (`ICEBERG_REST` connection, OAuth with Tableflow API key, scope `catalog`, no `remoteCatalogName`) with an Azure storage config, `allowedLocations` = `abfss://<container>@<account>.dfs.core.windows.net/`.
4. Create a read-only principal (`TABLE_READ_DATA`); read a table with PyIceberg using `X-Iceberg-Access-Delegation: vended-credentials`.

**Acceptance**: rows readable through Polaris with an engine that has no storage credentials. If it fails, stop and pick the fallback in section 3.

### Phase 1: Azure and kind foundation (0.5-1 day)

1. Resource group and ADLS Gen2 account (HNS on, same region as the Confluent cluster) + container `tableflow` (`make azure`).
2. Service principal for Polaris (client secret) with `Storage Blob Data Reader` + `Storage Blob Delegator` on the storage account.
3. kind cluster with NodePort host-port mappings (`make kind`); namespaces: `data-sources`, `clients`, `polaris`, `query`.

**Acceptance**: `kubectl get nodes` healthy; the SP can list the container (`az storage blob list` with SP login).

### Phase 2: Confluent Cloud foundation (1 day)

1. Environment (Stream Governance Essentials), Standard cluster in the same Azure region, service accounts + API keys (cluster, Schema Registry, Tableflow-scoped).
2. Azure provider integration: `confluent_provider_integration_setup` then `confluent_provider_integration_authorization` (provider >= 2.32); grant the Confluent enterprise app `Storage Blob Data Contributor` and `Reader` on the storage account.
3. Flink compute pool in the same region.

**Acceptance**: provider integration `ACTIVE`; Flink `SHOW TABLES` works.

### Phase 3: Data sources and clients on kind (1.5 days)

1. MySQL StatefulSet: `binlog_format=ROW`, `binlog_row_image=FULL`, GTID on, CDC user with replication privileges, sample schema (`customers`, `orders`).
2. PostgreSQL StatefulSet: `wal_level=logical`, publication, replication slot privileges, sample schema (`products`, `inventory`).
3. NodePort services (30306/30432) exposed publicly with ngrok TCP tunnels (`make tunnels`, `ngrok.yml`); copy the printed host/port into `env.sh`; TLS enabled if feasible.
4. Producer app (Python `confluent-kafka`, Avro + SR) writing `clickstream` or `orders_events`; consumer app (consumer group) to show regular consumption. Deploy with Kubernetes secrets for credentials.
5. Seed and load generator Jobs (insert/update/delete on both DBs).

**Acceptance**: the ngrok tunnel endpoints accept a MySQL/Postgres login from outside the cluster; client topic shows messages in Confluent.

### Phase 4: Connectors, Flink, Tableflow (1.5 days)

1. MySQL CDC Source V2 (managed): `output.data.format=AVRO`, `decimal.handling.mode=string`, `topic.prefix` unique, tables list; wait for schemas in SR.
2. Flink decode: `CREATE TABLE mysql_customers (... PRIMARY KEY ... NOT ENFORCED) WITH ('changelog.mode'='upsert')`, then `INSERT INTO ... SELECT ... FROM \`<prefix>.<db>.customers\``. Use `TO_TIMESTAMP_LTZ` for Debezium timestamps.
3. PostgreSQL CDC Source V2 (managed): `after.state.only=true`, `tombstones.on.delete=true`, RegexRouter SMT for dot-free names **(verify)**.
4. Enable Tableflow (Iceberg, BYOS ADLS Gen2) on: client topic (append), `mysql_customers` (upsert), Postgres topic (upsert).
5. Set Schema Registry compatibility to `FULL_TRANSITIVE` on CDC subjects (per the CDC skill guidance).

**Acceptance**: three Tableflow topics `ACTIVE`; Iceberg metadata and parquet files visible in ADLS; delete in MySQL disappears from the table.

### Phase 5: Apache Polaris on kind (1.5 days)

1. Helm install Polaris (>= 1.8.0) in `polaris`; feature flags: `ENABLE_CATALOG_FEDERATION`. Persistence: `relational-jdbc` on a small dedicated Postgres, or in-memory for the first iteration (state lost on restart; the bootstrap Job re-creates it).
2. Bootstrap Job (idempotent script against the Management API): federated catalog `tableflow` (EXTERNAL, `ICEBERG_REST` to the Tableflow IRC endpoint `https://tableflow.<region>.azure.confluent.cloud/iceberg/catalog/organizations/<ORG_ID>/environments/<ENV_ID>`), Azure storage config, `allowedLocations`, principal `demo_reader` with a catalog role holding read privileges including `TABLE_READ_DATA`.
3. Polaris API: ClusterIP + port-forward (or internal ingress); Console: build from `apache/polaris-tools`, load into kind (`kind load docker-image`), deploy with an nginx same-origin proxy (optional).
4. Smoke tests: list namespaces (cluster ID), list tables, write attempt returns 406 (read-only), new topic appears without any action.

**Acceptance**: namespace `lkc-xxxxx` with the three tables visible through Polaris; read-only principal cannot create or drop.

### Phase 6: Query engine and demo scenarios (1 day)

1. Trino Helm release with the Iceberg connector pointing at Polaris REST (`vended-credentials-enabled`, native Azure FS) **(verify Trino version support for Azure vended credentials)**; PyIceberg notebook as fallback. Quote cluster-ID namespaces.
2. Prepare demo scenarios (section 7) and validate each end to end.

**Acceptance**: each scenario runs in under 2 minutes and the data freshness is visible within the Tableflow commit interval (2-4 min).

### Phase 7: Automation, docs and teardown (1-1.5 days)

1. Shell script for Azure (`scripts/01-azure.sh`), Confluent CLI/manual steps for Confluent resources; Helm values + manifests under `k8s/`; `Makefile` targets (`make azure kind namespaces ... demo-changes`, `make down-kind down-azure`).
2. Teardown order (Confluent side): Tableflow topics, Flink statements, connectors, topics/schemas, then `make down-kind` and `make down-azure` (deletes the resource group; also delete the service principal).
3. README with the demo script, URLs, credentials handling, cost notes.

**Acceptance**: clean build from an empty subscription in under 60 minutes; teardown leaves no billable resources.

## 6. Proposed repo layout

```
workshop-tableflow-polaris/
  kind/                       # kind-config.yaml (NodePort host-port mappings)
  ngrok.yml.example           # TCP tunnels to MySQL/Postgres (copy to ngrok.yml)
  apps/producer/              # Kafka producer + consumer image
  connectors/                 # MySQL / PostgreSQL CDC connector configs
  k8s/                        # namespaces, data-sources, clients, polaris, trino
  flink/                      # decode DDL/INSERT statements
  scripts/                    # 01-azure.sh, tunnels.sh, bootstrap-polaris.sh, cdc-changes.sh, query_polaris.py
  Makefile
```

## 7. Demo scenarios (about 15 minutes)

1. **Kafka clients**: producer writes events, then the table appears in Polaris with no pipeline code.
2. **MySQL CDC**: insert, update, delete in `customers`, then query through Trino and show the row changes (upsert).
3. **PostgreSQL CDC**: same with `products`, using the no-Flink `after.state.only` path.
4. **Open catalog**: browse namespaces/tables in Polaris (Console or REST); show the table location in ADLS.
5. **Governance**: read-only principal can query but create/drop is rejected; one catalog, many engines.
6. **Freshness**: generate load, re-query after the Tableflow commit.
7. Optional: `ALTER TABLE ... ADD COLUMN` in a source DB and show schema evolution **(verify behaviour through Flink decode)**.

## 8. Cost and housekeeping

- Main costs: Confluent Standard cluster + Flink CFUs + 2 connectors + Tableflow, small ADLS (kind runs locally, no compute cost). Delete Confluent resources if idle for days; `make down-kind` frees local resources.
- Tag all Azure resources (`owner`, `purpose=demo`, `ttl`).
- Never commit API keys or the Polaris root credentials; use git-ignored `secrets.env` + Kubernetes secrets (including the service principal secret and ngrok auth token).

## 9. Resource list (all found via Glean)

**Federation and Polaris**
- How to Federate Tableflow IRC (BYOS) into Self-Hosted Apache Polaris: https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6257872088
- Polaris sync POC and gap analysis (why push sync is blocked today): https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6287097992
- INIT-10332 OSS Polaris integration: https://confluentinc.atlassian.net/browse/INIT-10332
- FF-28625 CARIAD Polaris on AKS: https://confluentinc.atlassian.net/browse/FF-28625
- Slack thread on the hostname validation problem: https://confluent.slack.com/archives/C06QKCWMEQH/p1789660217483669

**Tableflow on Azure**
- Tableflow with Azure GA Quickstart: https://confluentinc.atlassian.net/wiki/spaces/KORAGE/pages/5000626215
- Configure Storage for Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html
- Create an Azure Provider Integration: https://docs.confluent.io/cloud/current/integrations/provider-integrations/create-provider-integration-azure.html
- Azure Tableflow Setup Guide (Jeremy Ber): https://confluentinc.atlassian.net/wiki/spaces/~712020c669a3e36e6e4a9394cfccfe0f84e90b/pages/5178098392
- TableFlow Azure Setup (Eric Carlier, Entra privilege notes): https://docs.google.com/document/d/136RsWmAv4p1iKs3tfy6O9haaSl63on8IhB1ZyiEJK9M

**CDC**
- Materialize CDC with Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/concepts/materialize-cdc.html
- CDC to Tableflow agent skill (Flink decode pattern, immutability, cleanup order): https://github.com/confluentinc/agent-skills/blob/main/skills/confluent-cloud-cdc-tableflow/SKILL.md
- MySQL CDC Source V2: https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-v2-debezium/cc-mysql-source-cdc-v2-debezium.html
- PostgreSQL CDC Source V2: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html
- Public egress IPs for connectors: https://docs.confluent.io/cloud/current/connectors/static-egress-ip.html

**Terraform starting points**
- `confluentinc/moshe-training`, `ADLSGen2Create-terraform-confluent-azure-tableflow` (ADLS + provider integration + Tableflow + DuckDB query): https://github.com/confluentinc/moshe-training/blob/master/ADLSGen2Create-terraform-confluent-azure-tableflow/README.md
- `confluentinc/workshop-tableflow-databricks`, `terraform/azure-demo/` and `modules/confluent-tableflow` (two-step Azure provider integration): https://github.com/confluentinc/workshop-tableflow-databricks
- `confluentinc/tableflow-terraform-demo`: https://github.com/confluentinc/tableflow-terraform-demo

## 10. Open items to verify (Phase 0 and 1)

1. Polaris `azureConfigInfo` fields, required Azure roles for SAS/user-delegation vending, and whether SAS vending works with service principal (`AZURE_*` env var) credentials.
2. Tableflow IRC behaviour on Azure BYOS (credentials not vended, as on AWS) and exact `abfss://` locations written in table metadata.
3. Trino version supporting Azure vended credentials through the Iceberg REST connector.
4. Whether the RegexRouter SMT is accepted by the managed PostgreSQL CDC V2 connector and works with Tableflow.
5. Tunnel stability and alternatives for the managed connectors: ngrok free-tier TCP limits and host/port churn vs cloudflared, other TCP tunnels or a small public VM **(verify)**.
6. Whether the managed connectors accept the ngrok TCP host/port (TLS/SSL mode, hostname resolution) from Confluent Cloud.
7. Status and date of INIT-10332 (Polaris push sync): if shipped, add a second, push-based catalog to the demo.
