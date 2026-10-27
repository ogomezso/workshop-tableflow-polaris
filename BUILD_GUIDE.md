# Build Guide: Tableflow + Self-Hosted Apache Polaris on kind

Hands-on, in-order runbook. Companion to `STATEMENT_OF_WORK.md` (scope, risks, estimates).

**What you end up with**

```
MySQL (kind)  --ngrok-->CDC connector (managed)--> Kafka --Flink decode--> mysql_customers --\
Postgres (kind) --ngrok-->CDC connector (managed)-> Kafka --Flink decode--> pg_products ------+--> Tableflow (Iceberg)
Producer app (kind) ---------------------> Kafka orders_events (append) ------------/        |
                                                                                       ADLS Gen2
Trino / PyIceberg --> Apache Polaris (kind, federated, read-only) --> Tableflow IRC ------^ (Polaris reads ADLS)
```

All Kubernetes workloads run on a local **kind** cluster (Docker). Azure holds only the resource group, the ADLS Gen2 account (Tableflow sink) and a service principal that Polaris uses to read ADLS. The managed CDC connectors in Confluent Cloud reach the kind databases through ngrok TCP tunnels.

**Conventions**
- Commands run from the repo root in a bash/zsh shell. Placeholders in `<ANGLE_BRACKETS>` are values you copy from a previous step.
- **(verify)** = comes from general knowledge, not from Glean/Confluent sources. If a step with that tag fails, check it first.
- Facts and payloads for Polaris federation, CDC patterns, Tableflow commands come from Glean sources listed in Appendix C.

**Order of work.** Part C is the go/no-go spike: it proves Polaris can federate Tableflow tables on Azure using only the simple Kafka-client pipeline. Do not build Parts D-F until it passes.

---

## Part 0. Prerequisites

### 0.1 Access

| Need | Why |
|------|-----|
| Azure subscription, Contributor + permission to create role assignments | ADLS, service principal |
| Entra ID rights to run `az ad sp create` in your tenant | Required to authorize Confluent's Tableflow app on your storage. SEs have been blocked here before; ask IT early. |
| Confluent Cloud org with Azure, Flink and Tableflow enabled | Core services |
| ngrok account with an authtoken | Public TCP tunnels from Confluent Cloud to the local databases. TCP endpoints on the free tier may require payment/identity verification and have limits (verify current terms). |

### 0.2 Tools

```bash
az version; kubectl version --client; helm version; confluent version; docker --version; kind version; ngrok version; jq --version; python3 --version
```

Needed: `az`, `kubectl`, `helm`, `kind`, `docker`, `ngrok`, `confluent` CLI (recent), `jq`, `envsubst`, Python 3.11+.

```bash
az login
confluent login --save
ngrok config add-authtoken <NGROK_AUTHTOKEN>
```

### 0.3 Shared variables

Copy the templates once and fill them in; `source` both in every new shell. Both are git-ignored.

```bash
cp env.sh.example env.sh
cp secrets.env.example secrets.env
cp ngrok.yml.example ngrok.yml
printf 'env.sh\nsecrets.env\nngrok.yml\n.polaris-reader.env\n' >> .gitignore
sed -i.bak "s/^export SUFFIX=.*/export SUFFIX=$(openssl rand -hex 3)/" env.sh && rm env.sh.bak   # fixed once chosen
source env.sh && env | grep -E "^(LOC|RG|SA|SP_NAME|KIND_CLUSTER)="
```

`env.sh` defines `LOC`, `RG`, `SA`, `CONTAINER`, `SP_NAME`, `KIND_CLUSTER` and holds the IDs filled in later, including the tunnel endpoints `MYSQL_HOST/MYSQL_PORT/PG_HOST/PG_PORT`. `secrets.env` holds the Confluent keys and generated passwords (`openssl rand -hex 12`).

Use the same region everywhere. `westeurope` is used in Tableflow-on-Azure examples; confirm your Confluent region is offered for Kafka, Flink and Tableflow.

---

## Part A. Azure foundation and kind cluster

### A1. Resource group and ADLS Gen2 (hierarchical namespace ON)

```bash
az group create -n $RG -l $LOC --tags purpose=demo owner=$USER

az storage account create -n $SA -g $RG -l $LOC \
  --sku Standard_LRS --kind StorageV2 --hns true \
  --allow-blob-public-access false

az storage container create --account-name $SA -n $CONTAINER --auth-mode key
export SA_ID=$(az storage account show -n $SA -g $RG --query id -o tsv)
echo "export SA_ID=$SA_ID" >> env.sh
```

Storage must be in the same region as the Confluent Kafka cluster, and HNS is required.

### A2. Service principal for Polaris (reads ADLS, mints table-scoped credentials)

Polaris runs outside Azure, so it authenticates with a service principal client secret (no managed identity or Workload Identity). `scripts/01-azure.sh` (`make azure`) does A1 and A2 in one go:

```bash
make azure     # RG + ADLS + service principal + role assignments
```

The core of the SP part:

```bash
SP_JSON=$(az ad sp create-for-rbac -n "$SP_NAME" --query '{appId:appId,password:password,tenant:tenant}' -o json)
POLARIS_CLIENT_ID=$(jq -r .appId <<<"$SP_JSON")

# Read access + permission to request user-delegation keys (for SAS vending)  (verify roles)
for ROLE in "Storage Blob Data Reader" "Storage Blob Delegator"; do
  az role assignment create --assignee "$POLARIS_CLIENT_ID" --role "$ROLE" --scope "$SA_ID"
done
```

The script prints `TENANT_ID`, `SUB_ID`, `SA_ID`, `POLARIS_CLIENT_ID` (add to `env.sh`) and `POLARIS_CLIENT_SECRET` (add to `secrets.env`). The secret is shown once.

### A3. kind cluster and namespaces

`kind/kind-config.yaml` maps host ports 13306 and 15432 (127.0.0.1) to the NodePorts 30306 (MySQL) and 30432 (Postgres) so ngrok can tunnel them later.

```bash
make kind          # kind create cluster --name $KIND_CLUSTER --config kind/kind-config.yaml
make namespaces    # data-sources, clients, polaris, query
kubectl get nodes
```

**Checkpoint A**: `kubectl get nodes` shows 1 Ready node; `az storage container show --account-name $SA -n $CONTAINER --auth-mode key` succeeds.

---

## Part B. Confluent Cloud foundation

### B1. Environment, cluster, Schema Registry

```bash
confluent environment create tableflow-polaris-demo --governance-package essentials   # flag name: check `--help`
export ENV_ID=<env-xxxxx>
confluent environment use $ENV_ID

confluent kafka cluster create demo-azure --cloud azure --region $LOC --type standard
export CLUSTER_ID=<lkc-xxxxx>
confluent kafka cluster use $CLUSTER_ID
confluent kafka cluster describe $CLUSTER_ID        # note Bootstrap endpoint

confluent schema-registry cluster describe          # note Cluster ID (lsrc-...) and Endpoint
```

### B2. API keys

```bash
confluent api-key create --resource $CLUSTER_ID --description "demo kafka"         # -> KAFKA_KEY / KAFKA_SECRET
confluent api-key create --resource <lsrc-xxxxx> --description "demo schema registry"   # -> SR_KEY / SR_SECRET
confluent api-key create --resource tableflow --description "demo tableflow"       # -> TF_KEY / TF_SECRET (used by Polaris)
```

Store them in a git-ignored `secrets.env` and `source` it:

```bash
cat > secrets.env <<'EOF'
export KAFKA_BOOTSTRAP=<pkc-xxxx.westeurope.azure.confluent.cloud:9092>
export KAFKA_KEY=...  KAFKA_SECRET=...
export SR_URL=https://<psrc-xxxx>.westeurope.azure.confluent.cloud
export SR_KEY=...     SR_SECRET=...
export TF_KEY=...     TF_SECRET=...
export MYSQL_ROOT_PW=$(openssl rand -hex 12)
export MYSQL_CDC_PW=$(openssl rand -hex 12)
export PG_PW=$(openssl rand -hex 12)
export PG_CDC_PW=$(openssl rand -hex 12)
EOF
echo "secrets.env" >> .gitignore
```

### B3. Flink compute pool

```bash
confluent flink compute-pool create demo-pool --cloud azure --region $LOC --max-cfu 10
export POOL_ID=<lfcp-xxxxx>
```

### B4. Provider integration so Tableflow can write to your ADLS (BYOS)

1. Console → **Integrations / Provider Integrations** → **Add integration** → **Microsoft Entra ID**, name `azure-tableflow-integration` → **Create Entra ID app** → copy the **Entra ID app ID**.
2. Create the enterprise application in your tenant and grant it storage roles:

```bash
export CONFLUENT_APP_ID=<entra-app-id-from-console>
az ad sp create --id $CONFLUENT_APP_ID            # output shows appDisplayName like cspi-xxxxx
export CONFLUENT_SP_OID=$(az ad sp show --id $CONFLUENT_APP_ID --query id -o tsv)

az role assignment create --assignee-object-id $CONFLUENT_SP_OID --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope $SA_ID
az role assignment create --assignee-object-id $CONFLUENT_SP_OID --assignee-principal-type ServicePrincipal \
  --role "Reader" --scope $SA_ID
```

3. Back in Console, paste your `$TENANT_ID` and click **Validate**, then finish. Note the integration ID (`cspi-xxxxx`):

```bash
export PI_ID=<cspi-xxxxx>
```

To automate later, see the Terraform examples in Appendix C (`confluent_provider_integration_setup` + `confluent_provider_integration_authorization`, provider >= 2.32).

**Checkpoint B**: provider integration shows validated; `confluent flink compute-pool describe $POOL_ID` is RUNNING; role assignments exist (`az role assignment list --scope $SA_ID -o table`).

---

## Part C. First pipeline and Polaris federation (GO / NO-GO)

Goal: producer on kind, then Tableflow, then Polaris on kind, then read rows with an engine that has no storage credentials.

### C1. Topic and producer app

```bash
confluent kafka topic create orders_events --partitions 3
mkdir -p apps/producer && cd apps/producer
```

`requirements.txt`

```
confluent-kafka[avro,schemaregistry]==2.*
faker
```

`producer.py`

```python
import os, time, random, uuid
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

SCHEMA = """
{"type":"record","name":"OrderEvent","namespace":"demo",
 "fields":[{"name":"order_id","type":"string"},{"name":"customer_id","type":"int"},
           {"name":"sku","type":"string"},{"name":"qty","type":"int"},
           {"name":"amount","type":"double"},{"name":"event_ts","type":"long"}]}
"""
sr = SchemaRegistryClient({"url": os.environ["SR_URL"],
                           "basic.auth.user.info": f'{os.environ["SR_KEY"]}:{os.environ["SR_SECRET"]}'})
p = SerializingProducer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_KEY"], "sasl.password": os.environ["KAFKA_SECRET"],
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": AvroSerializer(sr, SCHEMA),
})
skus = ["SKU-A", "SKU-B", "SKU-C", "SKU-D"]
while True:
    o = {"order_id": str(uuid.uuid4()), "customer_id": random.randint(1, 5), "sku": random.choice(skus),
         "qty": random.randint(1, 5), "amount": round(random.uniform(5, 200), 2),
         "event_ts": int(time.time() * 1000)}
    p.produce("orders_events", key=o["order_id"], value=o)
    p.poll(0)
    time.sleep(float(os.environ.get("RATE_SECONDS", "1")))
```

`Dockerfile`

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py"]
```

Build the image locally, load it into kind (no registry), create secrets, deploy:

```bash
cd ../..
source env.sh; source secrets.env
docker build -t producer:v1 apps/producer
kind load docker-image producer:v1 --name $KIND_CLUSTER

kubectl -n clients create secret generic confluent-client \
  --from-literal=KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP --from-literal=KAFKA_KEY=$KAFKA_KEY --from-literal=KAFKA_SECRET=$KAFKA_SECRET \
  --from-literal=SR_URL=$SR_URL --from-literal=SR_KEY=$SR_KEY --from-literal=SR_SECRET=$SR_SECRET

kubectl -n clients apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata: { name: producer }
spec:
  replicas: 1
  selector: { matchLabels: { app: producer } }
  template:
    metadata: { labels: { app: producer } }
    spec:
      containers:
      - name: producer
        image: producer:v1
        imagePullPolicy: IfNotPresent
        envFrom: [ { secretRef: { name: confluent-client } } ]
EOF
kubectl -n clients logs deploy/producer --tail=5
```

(Shortcut: `make producer-image clients-secret clients`, which also creates the topic and applies `k8s/clients/clients.yaml`.)

Check in Console that `orders_events` receives messages and a value schema exists.

### C2. Enable Tableflow (Iceberg, BYOS on ADLS Gen2)

```bash
confluent tableflow topic enable orders_events \
  --cluster $CLUSTER_ID --environment $ENV_ID \
  --storage-type AzureDataLakeStorageGen2 \
  --provider-integration $PI_ID \
  --storage-account-name $SA --container-name $CONTAINER \
  --table-formats ICEBERG

confluent tableflow topic describe orders_events --cluster $CLUSTER_ID --environment $ENV_ID
```

Wait until status is active/running. First commit typically takes a few minutes; confirm files appear:

```bash
az storage fs file list -f $CONTAINER --account-name $SA --auth-mode key --path tableflow -o table | head
```

### C3. Verify the Tableflow Iceberg REST Catalog (IRC) directly

Find the endpoint in Console → Tableflow → API access. Expected pattern:

```bash
export ORG_ID=<organization-id>
export IRC="https://tableflow.${LOC}.azure.confluent.cloud/iceberg/catalog/organizations/${ORG_ID}/environments/${ENV_ID}"
echo "export IRC=$IRC" >> env.sh

TOKEN=$(curl -s -X POST "$IRC/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=$TF_KEY -d client_secret=$TF_SECRET -d scope=catalog | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" "$IRC/v1/namespaces" | jq
```

You should see a namespace equal to your cluster ID (`lkc-xxxxx`). Tableflow tokens last 900 seconds.

### C4. Deploy Apache Polaris on kind (plain manifests)

Using plain manifests instead of the Helm chart keeps every setting visible and mirrors the validated Docker setup (apache/polaris 1.8.0).

```bash
source env.sh; source secrets.env
# POLARIS_ROOT_SECRET (openssl rand -hex 16) and POLARIS_CLIENT_SECRET must be set in secrets.env

# Azure service principal credentials, injected as AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET
kubectl -n polaris create secret generic polaris-azure \
  --from-literal=AZURE_TENANT_ID=$TENANT_ID --from-literal=AZURE_CLIENT_ID=$POLARIS_CLIENT_ID \
  --from-literal=AZURE_CLIENT_SECRET=$POLARIS_CLIENT_SECRET

kubectl -n polaris apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: polaris
---
apiVersion: v1
kind: ConfigMap
metadata: { name: polaris-config }
data:
  application.properties: |
    polaris.features."ENABLE_CATALOG_FEDERATION"=true
    polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["AZURE"]
---
apiVersion: v1
kind: Secret
metadata: { name: polaris-bootstrap }
stringData:
  POLARIS_BOOTSTRAP_CREDENTIALS: "POLARIS,root,${POLARIS_ROOT_SECRET}"
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: polaris }
spec:
  replicas: 1
  selector: { matchLabels: { app: polaris } }
  template:
    metadata:
      labels: { app: polaris }
    spec:
      serviceAccountName: polaris
      containers:
      - name: polaris
        image: apache/polaris:1.8.0
        ports: [ { containerPort: 8181 }, { containerPort: 8182 } ]
        envFrom:
        - secretRef: { name: polaris-bootstrap }
        - secretRef: { name: polaris-azure }
        volumeMounts:
        - { name: cfg, mountPath: /deployments/config/application.properties, subPath: application.properties }
        readinessProbe: { tcpSocket: { port: 8181 }, initialDelaySeconds: 10, periodSeconds: 5 }
      volumes:
      - name: cfg
        configMap: { name: polaris-config }
---
apiVersion: v1
kind: Service
metadata: { name: polaris }
spec:
  selector: { app: polaris }
  ports: [ { name: api, port: 8181, targetPort: 8181 } ]
EOF
kubectl -n polaris rollout status deploy/polaris
kubectl -n polaris logs deploy/polaris --tail=20
```

(Shortcut: `make polaris`; the manifest is `k8s/polaris/polaris.yaml`.)

Notes:
- Polaris picks up the Azure service principal from the `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` env vars (Azure SDK default credential chain); whether this works for SAS vending with your Polaris version is (verify).
- The config file **must** be mounted at `/deployments/config/application.properties`, otherwise catalog creation fails with `406 Feature not enabled: ENABLE_CATALOG_FEDERATION`.
- The Confluent reference sets the storage type list to `["S3"]` (AWS). For ADLS use `["AZURE"]` (verify; use `["S3","AZURE"]` if Polaris rejects it).
- `POLARIS_BOOTSTRAP_CREDENTIALS` realm/client/secret format is the standard 1.x bootstrap variable (verify for your tag). With in-memory persistence, state is lost on restart, so keep the bootstrap script below idempotent and re-run it after any restart.

### C5. Bootstrap the federated catalog and a read-only principal

Port-forward and run the script (`scripts/bootstrap-polaris.sh`, or `make polaris-forward` in one shell and `make bootstrap-polaris` in another):

```bash
kubectl -n polaris port-forward svc/polaris 8181:8181 &
mkdir -p scripts
cat > scripts/bootstrap-polaris.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source env.sh; source secrets.env
P=${POLARIS_URL:-http://localhost:8181}
MGMT="$P/api/management/v1"

ROOT_TOKEN=$(curl -s -X POST "$P/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=$POLARIS_ROOT_SECRET \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
H=(-H "Authorization: Bearer $ROOT_TOKEN" -H "Content-Type: application/json")

# 1. Federated EXTERNAL catalog -> Tableflow IRC (leave remoteCatalogName unset)
curl -s -X POST "$MGMT/catalogs" "${H[@]}" -d @- <<JSON
{
  "catalog": {
    "type": "EXTERNAL",
    "name": "tableflow",
    "properties": {"default-base-location": "abfss://${CONTAINER}@${SA}.dfs.core.windows.net/"},
    "storageConfigInfo": {
      "storageType": "AZURE",
      "tenantId": "${TENANT_ID}",
      "allowedLocations": ["abfss://${CONTAINER}@${SA}.dfs.core.windows.net/"]
    },
    "connectionConfigInfo": {
      "connectionType": "ICEBERG_REST",
      "uri": "${IRC}",
      "authenticationParameters": {
        "authenticationType": "OAUTH",
        "tokenUri": "${IRC}/v1/oauth/tokens",
        "clientId": "${TF_KEY}",
        "clientSecret": "${TF_SECRET}",
        "scopes": ["catalog"]
      }
    }
  }
}
JSON

# 2. Read-only principal for engines (TABLE_READ_DATA is required for credential vending)
CREDS=$(curl -s -X POST "$MGMT/principals" "${H[@]}" -d '{"principal":{"name":"engine_reader"}}')
echo "$CREDS" | jq -r '"export POLARIS_READER_ID=\(.credentials.clientId)\nexport POLARIS_READER_SECRET=\(.credentials.clientSecret)"' > .polaris-reader.env

curl -s -X POST "$MGMT/principal-roles" "${H[@]}" -d '{"principalRole":{"name":"tableflow_reader"}}'
curl -s -X PUT  "$MGMT/principals/engine_reader/principal-roles" "${H[@]}" -d '{"principalRole":{"name":"tableflow_reader"}}'
curl -s -X POST "$MGMT/catalogs/tableflow/catalog-roles" "${H[@]}" -d '{"catalogRole":{"name":"reader"}}'
for PRIV in NAMESPACE_LIST NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  curl -s -X PUT "$MGMT/catalogs/tableflow/catalog-roles/reader/grants" "${H[@]}" \
    -d "{\"grant\":{\"type\":\"catalog\",\"privilege\":\"$PRIV\"}}"
done
curl -s -X PUT "$MGMT/principal-roles/tableflow_reader/catalog-roles/tableflow" "${H[@]}" -d '{"catalogRole":{"name":"reader"}}'
echo "Done. Reader credentials in .polaris-reader.env"
EOF
chmod +x scripts/bootstrap-polaris.sh
echo ".polaris-reader.env" >> .gitignore
./scripts/bootstrap-polaris.sh
```

Polaris validates the Tableflow connection when the catalog is created, so a wrong IRC URL or API key fails immediately. If the `curl` for `/catalogs` returns an error body, read it before continuing.

The `AZURE` `storageConfigInfo` shape (`tenantId`, `allowedLocations` as `abfss://...`) is from general Polaris knowledge (verify against the Polaris management API for your version). Every container Tableflow writes to must be listed in `allowedLocations`.

### C6. Read through Polaris with an engine that has no storage credentials

```bash
source .polaris-reader.env
python3 -m venv .venv && source .venv/bin/activate
pip install "pyiceberg[adlfs]" pandas
python - <<'EOF'
import os
from pyiceberg.catalog.rest import RestCatalog
cat = RestCatalog(
    "polaris",
    uri="http://localhost:8181/api/catalog",
    warehouse="tableflow",
    credential=f'{os.environ["POLARIS_READER_ID"]}:{os.environ["POLARIS_READER_SECRET"]}',
    scope="PRINCIPAL_ROLE:ALL",
    **{"header.X-Iceberg-Access-Delegation": "vended-credentials"},
)
print(cat.list_namespaces())
ns = cat.list_namespaces()[0]
print(cat.list_tables(ns))
t = cat.load_table(f"{ns[0]}.orders_events")
print(t.scan().to_pandas().head())
EOF
```

**Checkpoint C / GO-NO-GO**: rows from `orders_events` print, using no storage key on your machine. If you hit a problem, use this table:

| Symptom | Cause | Fix |
|---------|-------|-----|
| `406 Feature not enabled: ENABLE_CATALOG_FEDERATION` | Config file not read | Mount path must be exactly `/deployments/config/application.properties` |
| Catalog create fails on storage type | `SUPPORTED_CATALOG_STORAGE_TYPES` | Try `["S3","AZURE"]` or consult the Polaris Azure storage docs (verify) |
| `403 ... not authorized for op LOAD_TABLE_WITH_READ_DELEGATION` | Principal lacks `TABLE_READ_DATA` | Re-run grants |
| `403 ... locations outside the catalog's allowed locations` | Container not in `allowedLocations` | Add it and recreate the catalog |
| loadTable OK, then storage `AuthorizationFailure/403` | Polaris service principal cannot read ADLS or mint SAS | Check the `polaris-azure` secret values (client secret not expired), role assignments (Reader + Delegator on `$SA_ID`), and that the pod has the `AZURE_*` env vars (verify) |
| Empty namespace list | Tableflow not yet committed | Wait a few minutes; check Tableflow status |

**Fallback if Azure federation cannot be made to work**: query Tableflow IRC directly (Trino/PyIceberg against `$IRC` with the Tableflow key) and present Polaris as the roadmap (push sync is still blocked: INIT-10332). Decide before continuing.

---

## Part D. MySQL CDC (fully managed connector, Flink decode)

### D1. MySQL on kind (NodePort)

```bash
source env.sh; source secrets.env
kubectl -n data-sources create secret generic mysql-secret \
  --from-literal=root=$MYSQL_ROOT_PW --from-literal=cdc=$MYSQL_CDC_PW

# Shortcut: make mysql   (manifest: k8s/data-sources/mysql.yaml)
kubectl -n data-sources apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata: { name: mysql-init }
data:
  init.sql: |
    CREATE DATABASE IF NOT EXISTS shop;
    USE shop;
    CREATE TABLE customers (
      id INT PRIMARY KEY AUTO_INCREMENT,
      name VARCHAR(100) NOT NULL,
      email VARCHAR(150),
      city VARCHAR(80),
      tier VARCHAR(20) DEFAULT 'standard'
    );
    INSERT INTO customers (name,email,city,tier) VALUES
      ('Ana Torres','ana@example.com','Madrid','gold'),
      ('Luc Martin','luc@example.com','Paris','standard'),
      ('Eva Kramer','eva@example.com','Berlin','standard'),
      ('Ravi Patel','ravi@example.com','London','gold'),
      ('Mia Rossi','mia@example.com','Milan','standard');
    CREATE USER 'cdc'@'%' IDENTIFIED BY '${MYSQL_CDC_PW}';
    GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
    FLUSH PRIVILEGES;
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: mysql }
spec:
  serviceName: mysql
  replicas: 1
  selector: { matchLabels: { app: mysql } }
  template:
    metadata: { labels: { app: mysql } }
    spec:
      containers:
      - name: mysql
        image: mysql:8.0
        args: ["--server-id=223344","--log-bin=mysql-bin","--binlog-format=ROW","--binlog-row-image=FULL",
               "--gtid-mode=ON","--enforce-gtid-consistency=ON","--binlog-expire-logs-seconds=259200"]
        env:
        - name: MYSQL_ROOT_PASSWORD
          valueFrom: { secretKeyRef: { name: mysql-secret, key: root } }
        ports: [ { containerPort: 3306 } ]
        volumeMounts:
        - { name: data, mountPath: /var/lib/mysql }
        - { name: init, mountPath: /docker-entrypoint-initdb.d }
      volumes:
      - name: init
        configMap: { name: mysql-init }
  volumeClaimTemplates:
  - metadata: { name: data }
    spec:
      accessModes: ["ReadWriteOnce"]
      resources: { requests: { storage: 5Gi } }
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
spec:
  type: NodePort
  selector: { app: mysql }
  ports: [ { port: 3306, targetPort: 3306, nodePort: 30306 } ]
EOF
kubectl -n data-sources rollout status statefulset/mysql
```

Verify binlog settings:

```bash
kubectl -n data-sources exec statefulset/mysql -- mysql -uroot -p$MYSQL_ROOT_PW -e \
 "SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','gtid_mode','binlog_row_image');"
```

### D2. Expose MySQL and Postgres with ngrok TCP tunnels

Confluent Cloud's managed connectors cannot reach your laptop directly. `make tunnels` (`scripts/tunnels.sh`) starts ngrok with `ngrok.yml` (tunnels `127.0.0.1:13306` and `127.0.0.1:15432`, which kind maps to the NodePorts) and prints the public endpoints. Run it in its own terminal and keep it running (do this after both databases from D1/E1 are up).

```bash
make tunnels
# Update env.sh with:
# export MYSQL_HOST=0.tcp.eu.ngrok.io
# export MYSQL_PORT=12345
# export PG_HOST=...
# export PG_PORT=...
source env.sh
```

The host and port **change on every ngrok restart**; update `env.sh` and recreate the connectors (or update their config) each time. TCP tunnels on the free ngrok tier may require account verification (card on file) and have endpoint/connection limits (verify current ngrok terms).

**Security note.** The tunnels expose the databases to the public internet. Mitigate: use the generated strong passwords (`openssl rand -hex 12`), keep the demo short-lived and stop `make tunnels` when finished, and enable TLS on the databases where possible (the MySQL connector uses `database.ssl.mode=preferred`). Do not load real data.

### D3. Create the managed connector

`connectors/mysql-cdc.json`

```json
{
  "name": "mysql-cdc-shop",
  "connector.class": "MySqlCdcSourceV2",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "${KAFKA_KEY}",
  "kafka.api.secret": "${KAFKA_SECRET}",
  "database.hostname": "${MYSQL_HOST}",
  "database.port": "${MYSQL_PORT}",
  "database.user": "cdc",
  "database.password": "${MYSQL_CDC_PW}",
  "database.include.list": "shop",
  "table.include.list": "shop.customers",
  "topic.prefix": "mysql",
  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "string",
  "database.ssl.mode": "preferred",
  "tasks.max": "1"
}
```

```bash
# render ${VAR} placeholders (including the current tunnel MYSQL_HOST/MYSQL_PORT); the rendered file holds secrets, keep it out of git
source env.sh; source secrets.env
envsubst < connectors/mysql-cdc.json > connectors/mysql-cdc.rendered.json
echo "connectors/*.rendered.json" >> .gitignore
confluent connect cluster create --config-file connectors/mysql-cdc.rendered.json \
  --cluster $CLUSTER_ID --environment $ENV_ID
confluent connect cluster list --cluster $CLUSTER_ID --environment $ENV_ID
```

Shortcut for both connectors: `make connectors`. Wait 2-5 minutes for provisioning. The topic is `mysql.shop.customers` (`<prefix>.<db>.<table>`). Confirm schemas and records exist in Console.

### D4. Flink decode into an upsert topic (clean name, no dots)

Run in the Console Flink SQL workspace (or `confluent flink shell --compute-pool $POOL_ID --environment $ENV_ID`), with catalog = your environment and database = your cluster:

`flink/10_mysql_customers.sql`

```sql
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
```

Rules from the Confluent CDC guidance:
- The CDC topic appears in Flink automatically (no source DDL). Do not set connector/format/bootstrap options on `CREATE TABLE`.
- Create the target with `changelog.mode = 'upsert'` **from the start**. Tableflow's changelog mode is immutable after first materialization.
- Do not enable Tableflow directly on `mysql.shop.customers`; deletes produce tombstones that suspend an append-mode table, and dotted names fail through Polaris.
- Use `TO_TIMESTAMP_LTZ(col / 1000, 3)` etc. if you add Debezium timestamp columns.

Check that the `INSERT` statement is RUNNING and consume `mysql_customers` to see rows.

### D5. Enable Tableflow on the decoded topic

```bash
confluent tableflow topic enable mysql_customers \
  --cluster $CLUSTER_ID --environment $ENV_ID \
  --storage-type AzureDataLakeStorageGen2 --provider-integration $PI_ID \
  --storage-account-name $SA --container-name $CONTAINER --table-formats ICEBERG
```

**Checkpoint D**: after the first commit, rerun the PyIceberg script from C6 against `mysql_customers`. Then:

```bash
kubectl -n data-sources exec statefulset/mysql -- mysql -uroot -p$MYSQL_ROOT_PW shop -e \
 "UPDATE customers SET tier='gold' WHERE id=2; DELETE FROM customers WHERE id=5; INSERT INTO customers (name,email,city) VALUES ('Zoe Lind','zoe@example.com','Oslo');"
```

After the next Tableflow commit (a few minutes), id=2 shows `gold`, id=5 is gone, and Zoe is present.

---

## Part E. PostgreSQL CDC

### E1. PostgreSQL on kind (NodePort)

```bash
kubectl -n data-sources create secret generic pg-secret \
  --from-literal=postgres=$PG_PW --from-literal=cdc=$PG_CDC_PW

# Shortcut: make postgres   (manifest: k8s/data-sources/postgres.yaml)
kubectl -n data-sources apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata: { name: pg-init }
data:
  init.sql: |
    CREATE DATABASE shop;
    \c shop
    CREATE TABLE public.products (
      id SERIAL PRIMARY KEY,
      sku TEXT NOT NULL,
      name TEXT NOT NULL,
      price NUMERIC(10,2) NOT NULL,
      stock INT NOT NULL DEFAULT 0
    );
    INSERT INTO public.products (sku,name,price,stock) VALUES
      ('SKU-A','Widget',9.90,100),('SKU-B','Gadget',24.50,40),
      ('SKU-C','Gizmo',5.25,500),('SKU-D','Doohickey',99.00,12);
    CREATE ROLE cdc WITH LOGIN REPLICATION PASSWORD '${PG_CDC_PW}';
    GRANT CONNECT ON DATABASE shop TO cdc;
    GRANT USAGE ON SCHEMA public TO cdc;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc;
    CREATE PUBLICATION dbz_publication FOR TABLE public.products;
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: postgres }
spec:
  serviceName: postgres
  replicas: 1
  selector: { matchLabels: { app: postgres } }
  template:
    metadata: { labels: { app: postgres } }
    spec:
      containers:
      - name: postgres
        image: postgres:16
        args: ["-c","wal_level=logical","-c","max_wal_senders=10","-c","max_replication_slots=10"]
        env:
        - name: POSTGRES_PASSWORD
          valueFrom: { secretKeyRef: { name: pg-secret, key: postgres } }
        - { name: PGDATA, value: /var/lib/postgresql/data/pgdata }
        ports: [ { containerPort: 5432 } ]
        volumeMounts:
        - { name: data, mountPath: /var/lib/postgresql/data }
        - { name: init, mountPath: /docker-entrypoint-initdb.d }
      volumes:
      - name: init
        configMap: { name: pg-init }
  volumeClaimTemplates:
  - metadata: { name: data }
    spec:
      accessModes: ["ReadWriteOnce"]
      resources: { requests: { storage: 5Gi } }
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  type: NodePort
  selector: { app: postgres }
  ports: [ { port: 5432, targetPort: 5432, nodePort: 30432 } ]
EOF
kubectl -n data-sources rollout status statefulset/postgres
kubectl -n data-sources exec statefulset/postgres -- psql -U postgres -c "SHOW wal_level;"
```

The init script runs only on first start of an empty volume. Traffic to Postgres is unencrypted unless you configure TLS; the strong generated password and a short-lived tunnel are the demo's protection (see the security note in D2; enable TLS and set the connector's SSL mode if you want to show that).

### E2. Managed connector

`connectors/postgres-cdc.json`

```json
{
  "name": "postgres-cdc-shop",
  "connector.class": "PostgresCdcSourceV2",
  "kafka.auth.mode": "KAFKA_API_KEY",
  "kafka.api.key": "${KAFKA_KEY}",
  "kafka.api.secret": "${KAFKA_SECRET}",
  "database.hostname": "${PG_HOST}",
  "database.port": "${PG_PORT}",
  "database.user": "cdc",
  "database.password": "${PG_CDC_PW}",
  "database.dbname": "shop",
  "table.include.list": "public.products",
  "topic.prefix": "pg",
  "slot.name": "dbz_slot",
  "publication.name": "dbz_publication",
  "publication.autocreate.mode": "disabled",
  "output.data.format": "AVRO",
  "output.key.format": "AVRO",
  "decimal.handling.mode": "string",
  "tasks.max": "1"
}
```

```bash
envsubst < connectors/postgres-cdc.json > connectors/postgres-cdc.rendered.json
confluent connect cluster create --config-file connectors/postgres-cdc.rendered.json \
  --cluster $CLUSTER_ID --environment $ENV_ID
```

Topic: `pg.public.products`.

### E3. Flink decode and Tableflow

`flink/20_pg_products.sql`

```sql
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
```

(`price` is a string because of `decimal.handling.mode=string`; cast it in Flink if you want a numeric type.)

```bash
confluent tableflow topic enable pg_products \
  --cluster $CLUSTER_ID --environment $ENV_ID \
  --storage-type AzureDataLakeStorageGen2 --provider-integration $PI_ID \
  --storage-account-name $SA --container-name $CONTAINER --table-formats ICEBERG
```

**Optional variant (no Flink)**: set `"after.state.only": "true"` and keep `tombstones.on.delete=true` on the connector and enable Tableflow directly (upsert). Topic names keep dots, so add a RegexRouter SMT to produce a dot-free topic name (verify it with the managed connector). Use only to show the alternative path.

**Checkpoint E**:

```bash
kubectl -n data-sources exec statefulset/postgres -- psql -U postgres -d shop -c \
 "UPDATE products SET stock=stock-5 WHERE sku='SKU-A'; INSERT INTO products (sku,name,price,stock) VALUES ('SKU-E','Thingamajig',12.00,60);"
```

The change appears in `pg_products` after the next Tableflow commit.

---

## Part F. Trino on kind

### F1. Install Trino with a Polaris catalog

```bash
# Shortcut: make trino   (renders k8s/trino/values.yaml)
source env.sh; source .polaris-reader.env
helm repo add trino https://trinodb.github.io/charts && helm repo update

cat > k8s-trino-values.yaml <<EOF
server:
  workers: 1
additionalCatalogs:
  polaris: |
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris.polaris.svc.cluster.local:8181/api/catalog
    iceberg.rest-catalog.warehouse=tableflow
    iceberg.rest-catalog.security=OAUTH2
    iceberg.rest-catalog.oauth2.credential=${POLARIS_READER_ID}:${POLARIS_READER_SECRET}
    iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
    iceberg.rest-catalog.vended-credentials-enabled=true
    fs.native-azure.enabled=true
    iceberg.security=read_only
EOF
echo "k8s-trino-values.yaml" >> .gitignore
helm install trino trino/trino -n query -f k8s-trino-values.yaml
kubectl -n query rollout status deploy/trino-coordinator
```

The catalog properties mirror Confluent's documented Trino-to-Tableflow setup (vended credentials, read-only). For Azure, `fs.native-azure.enabled=true` and the OAuth scope property are from general knowledge (verify for your Trino version). If Trino cannot use Azure vended credentials, keep PyIceberg as the demo engine.

### F2. Query

```bash
kubectl -n query exec -it deploy/trino-coordinator -- trino --catalog polaris
```

```sql
SHOW SCHEMAS;
SHOW TABLES FROM "lkc-xxxxx";                         -- quote the cluster ID
SELECT * FROM "lkc-xxxxx".mysql_customers ORDER BY id;
SELECT sku, count(*) c, sum(amount) total FROM "lkc-xxxxx".orders_events GROUP BY sku;
SELECT * FROM "lkc-xxxxx".pg_products;
```

Writes must fail (`CREATE TABLE`, `DROP TABLE`): the catalog is read-only by design.

### F3. Optional: Polaris Console UI

The Polaris server is API-only. The Console in `apache/polaris-tools` is an early-stage project with no released image; build it from source with an nginx proxy placed same-origin with Polaris so you do not need to change Polaris CORS, load the image into kind (`kind load docker-image`), and deploy it to the `polaris` namespace with the env vars `VITE_POLARIS_REALM=POLARIS`, `VITE_POLARIS_REALM_HEADER_NAME=Polaris-Realm`, `VITE_POLARIS_PRINCIPAL_SCOPE=PRINCIPAL_ROLE:ALL` and the token URL pointing at the proxy. Sign in with the Polaris principal client ID and secret. Treat it as a bonus; the demo works without it. (See "Appendix D: Optional web UI" in the federation how-to for the exact build.)

---

## Part G. Demo run-through (about 15 minutes)

Keep the producer running. Tableflow commits every few minutes, so start the CDC changes about 5 minutes before the live part.

| # | Show | How |
|---|------|-----|
| 1 | The three source types | Console: topics `orders_events`, `mysql.shop.customers`, `pg.public.products`, connectors, Flink statements |
| 2 | Tableflow materialization | Console Tableflow tab; ADLS container with Iceberg metadata + parquet files |
| 3 | Polaris catalog | `kubectl port-forward` + PyIceberg `list_namespaces/list_tables`, or the Console UI |
| 4 | Query through the open catalog | Trino queries from F2; mention the engine has no storage credentials, Polaris vends them |
| 5 | CDC changes | Run the UPDATE/DELETE statements from D/E; re-query after the commit |
| 6 | Governance | Reader principal can query; `CREATE TABLE`/`DROP TABLE` rejected (406/403) |
| 7 | New table appears automatically | Enable Tableflow on one more topic and `SHOW TABLES` again: no sync job, no Polaris change |

---

## Part H. Teardown

Order matters (from the CDC guidance); Confluent side first:

```bash
for t in orders_events mysql_customers pg_products; do
  confluent tableflow topic disable $t --cluster $CLUSTER_ID --environment $ENV_ID
done
# Flink: delete the two INSERT statements, then drop the target tables
confluent flink statement list --environment $ENV_ID
confluent flink statement delete <statement-name> --environment $ENV_ID
# connectors, then topics, then schemas
confluent connect cluster delete <connector-id> --cluster $CLUSTER_ID --environment $ENV_ID
confluent kafka topic delete orders_events      # repeat for mysql.shop.customers, pg.public.products, mysql_customers, pg_products
confluent environment delete $ENV_ID            # deleting the environment also removes the cluster

# Local cluster and tunnels
# stop `make tunnels` (Ctrl-C), then:
make down-kind                                  # kind delete cluster

# Azure (resource group with ADLS, plus the Polaris service principal)
az ad sp delete --id $POLARIS_CLIENT_ID
make down-azure                                 # az group delete -n $RG --yes --no-wait
```

If provider-integration deletion returns 409 while Terraform-managed, remove it from state first (`terraform state rm`) and destroy the rest. Also delete the Entra enterprise app (`az ad sp delete --id $CONFLUENT_APP_ID`) and the Tableflow/Kafka API keys.

Between sessions: stop the tunnels and either leave the kind cluster (it survives a Docker restart, but ngrok tunnel URLs do not, so rerun `make tunnels`, update `env.sh` and recreate the connectors) or delete it with `make down-kind` and rebuild from Part A3. Polaris state is in memory, so after any Polaris pod restart rerun `scripts/bootstrap-polaris.sh` and regenerate the reader credentials, then update the Trino values.

---

## Appendix A. Troubleshooting quick reference

| Symptom | Cause | Fix |
|---------|-------|-----|
| Connector stays provisioning / `database.hostname` validation fails | Tunnel down or stale host/port | `make tunnels` still running? Re-source `env.sh` with the current `MYSQL_HOST/MYSQL_PORT/PG_HOST/PG_PORT`, re-render and recreate the connector |
| `make tunnels` fails: "tunnels did not come up" | ngrok authtoken missing, TCP endpoints need account verification, or tier limit | Check `/tmp/ngrok.log`, run `ngrok config add-authtoken`, verify account/plan limits (verify) |
| Connector worked, then fails after restarting ngrok or the laptop | Tunnel host/port changed | Update `env.sh`, re-render, recreate connectors |
| Tunnel is up but connection refused | NodePort not mapped or DB not ready | `kubectl -n data-sources get svc`; check `kind/kind-config.yaml` ports 13306/15432 and that the cluster was created with it |
| MySQL connector: binlog/GTID errors | Settings missing | Re-check `SHOW VARIABLES` output from D1 |
| Postgres connector: slot/publication errors | Publication missing or no REPLICATION | Re-check init script ran (it only runs on an empty volume) |
| Tableflow `Suspended`: "detected a Kafka record with a null value" | Tableflow enabled directly on a CDC topic | Use the Flink decode topic |
| "changelog mode ... has been modified since table materialization began" | Mode changed after first materialization | Delete Tableflow topic and Kafka topic, recreate with the right mode |
| Table invisible through Polaris, `NoSuchTableException` | Topic name has dots (`server.db.table`) | Use dot-free decoded topics |
| Rows missing right after changes | Tableflow commit interval | Wait for the next commit (a few minutes; low-throughput topics can take up to 15) |
| 403 locations outside allowed | Container not in `allowedLocations` | Add it, recreate catalog |

## Appendix B. Repo layout produced by this guide

```
apps/producer/            Dockerfile, producer.py, requirements.txt
kind/                     kind-config.yaml
k8s/                      namespaces, clients, polaris, data-sources, trino manifests
connectors/               mysql-cdc.json, postgres-cdc.json (rendered copies are git-ignored)
flink/                    10_mysql_customers.sql, 20_pg_products.sql
scripts/                  01-azure.sh, tunnels.sh, bootstrap-polaris.sh
Makefile                  shortcuts for every part
env.sh, secrets.env, ngrok.yml   local variables/config (git-ignored)
```

## Appendix C. Sources (Glean)

- How to Federate Tableflow IRC (BYOS) into Self-Hosted Apache Polaris: https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6257872088
- Tableflow catalog sync to self-hosted Polaris, POC and gap analysis: https://confluentinc.atlassian.net/wiki/spaces/~7120209a897d308af34bb2a6193bcc28220c05/pages/6287097992
- Tableflow with Azure GA Quickstart (provider integration steps): https://confluentinc.atlassian.net/wiki/spaces/KORAGE/pages/5000626215
- Configure Storage for Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/configure-storage.html
- Materialize CDC with Tableflow: https://docs.confluent.io/cloud/current/topics/tableflow/concepts/materialize-cdc.html
- CDC to Tableflow agent skill (Flink decode, immutability, cleanup order, `tableflow topic enable` for ADLS): https://github.com/confluentinc/agent-skills/blob/main/skills/confluent-cloud-cdc-tableflow/SKILL.md
- MySQL CDC Source V2: https://docs.confluent.io/cloud/current/connectors/cc-mysql-source-cdc-v2-debezium/cc-mysql-source-cdc-v2-debezium.html
- PostgreSQL CDC Source V2: https://docs.confluent.io/cloud/current/connectors/cc-postgresql-cdc-source-v2-debezium/cc-postgresql-cdc-source-v2-debezium.html
- Query Tableflow with Trino: https://docs.confluent.io/cloud/current/topics/tableflow/how-to-guides/query-engines/query-with-trino.html
- Public egress IPs for connectors (reference; not used here, the databases are reached through ngrok): https://docs.confluent.io/cloud/current/connectors/static-egress-ip.html
- Terraform references for automating B4: https://github.com/confluentinc/moshe-training/blob/master/ADLSGen2Create-terraform-confluent-azure-tableflow/README.md and https://github.com/confluentinc/workshop-tableflow-databricks
- Why Polaris push sync is blocked today (INIT-10332): https://confluentinc.atlassian.net/browse/INIT-10332
