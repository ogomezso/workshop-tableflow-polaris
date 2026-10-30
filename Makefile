SHELL := /bin/bash
# ENV loads env.sh; ENVS also loads secrets.env. KC pins every kubectl call to the kind cluster even if your current context changes.
ENV  := source env.sh &&
ENVS := source env.sh && source secrets.env &&
KC   := kubectl --context kind-$$KIND_CLUSTER

.PHONY: help init aws confluent-storage kind namespaces producer-image connect-image confluent-secret clients \
        polaris polaris-forward bootstrap-polaris mysql postgres connect connectors connectors-status \
        flink-help tableflow-orders tableflow-cdc trino query demo-changes down-kind down-aws

help:
	@echo "Part A  : make init aws kind namespaces"
	@echo "Part B  : Confluent env/cluster/keys are manual (BUILD_GUIDE.md Part B), then: make confluent-storage"
	@echo "Part C  : make clients tableflow-orders polaris bootstrap-polaris query TABLE=orders_events"
	@echo "Part D/E: make mysql postgres connect connectors flink-help tableflow-cdc [TOPICS=mysql_customers]"
	@echo "Part F  : make trino"
	@echo "Polaris UI (optional): make polaris-console polaris-console-forward"
	@echo "Demo    : make demo-changes"
	@echo "Cleanup : make down-kind down-aws (delete the Confluent resources first, BUILD_GUIDE.md Part H)"

init:
	./scripts/00-init.sh

aws:
	./scripts/01-aws.sh

confluent-storage:
	./scripts/02-confluent-storage-access.sh

kind:
	$(ENV) { kind get clusters 2>/dev/null | grep -qx $$KIND_CLUSTER || kind create cluster --name $$KIND_CLUSTER --wait 120s; }
	$(ENV) kubectl config use-context kind-$$KIND_CLUSTER

namespaces:
	$(ENV) $(KC) apply -f k8s/namespaces.yaml

producer-image:
	$(ENV) docker build -t producer:v1 apps/producer
	$(ENV) kind load docker-image producer:v1 --name $$KIND_CLUSTER

connect-image:
	$(ENV) docker build -t connect-debezium:v1 connect
	$(ENV) kind load docker-image connect-debezium:v1 --name $$KIND_CLUSTER

# Same Confluent credentials for the client apps and for Kafka Connect
confluent-secret:
	$(ENVS) for ns in clients connect; do $(KC) -n $$ns create secret generic confluent-client \
	  --from-literal=KAFKA_BOOTSTRAP=$$KAFKA_BOOTSTRAP --from-literal=KAFKA_KEY=$$KAFKA_KEY --from-literal=KAFKA_SECRET=$$KAFKA_SECRET \
	  --from-literal=SR_URL=$$SR_URL --from-literal=SR_KEY=$$SR_KEY --from-literal=SR_SECRET=$$SR_SECRET \
	  --dry-run=client -o yaml | $(KC) apply -f - || exit 1; done

clients: producer-image confluent-secret
	$(ENVS) confluent kafka topic create orders_events --partitions 3 --cluster $$CLUSTER_ID --environment $$ENV_ID || true
	$(ENV) $(KC) -n clients apply -f k8s/clients/clients.yaml

tableflow-orders:
	$(ENVS) confluent tableflow topic enable orders_events --cluster $$CLUSTER_ID --environment $$ENV_ID \
	  --storage-type BYOS --provider-integration $$PI_ID --bucket-name $$BUCKET --table-formats ICEBERG

polaris:
	$(ENVS) test -n "$$POLARIS_AWS_ACCESS_KEY_ID" || { echo "POLARIS_AWS_ACCESS_KEY_ID is empty in secrets.env: run make aws"; exit 1; }
	$(ENVS) $(KC) -n polaris create secret generic polaris-aws \
	  --from-literal=AWS_ACCESS_KEY_ID=$$POLARIS_AWS_ACCESS_KEY_ID --from-literal=AWS_SECRET_ACCESS_KEY=$$POLARIS_AWS_SECRET_ACCESS_KEY \
	  --from-literal=AWS_REGION=$$AWS_REGION --dry-run=client -o yaml | $(KC) apply -f -
	$(ENVS) envsubst '$$POLARIS_ROOT_SECRET' < k8s/polaris/polaris.yaml | $(KC) -n polaris apply -f -
	$(ENV) $(KC) -n polaris rollout status deploy/polaris --timeout=180s

polaris-forward:
	$(ENV) $(KC) -n polaris port-forward svc/polaris 8181:8181

bootstrap-polaris:
	./scripts/bootstrap-polaris.sh

.venv:
	python3 -m venv .venv && .venv/bin/pip install -q "pyiceberg[pyarrow,pandas]"

# Optional Polaris UI (apache/polaris-tools has no released image: built from source, see BUILD_GUIDE.md F3)
console-image:
	./scripts/build-console.sh

polaris-console: console-image
	$(ENV) $(KC) -n polaris apply -f k8s/polaris/console.yaml
	$(ENV) $(KC) -n polaris rollout status deploy/polaris-console --timeout=180s
	@echo "Run: make polaris-console-forward   then open http://localhost:8080"

polaris-console-forward:
	$(ENV) $(KC) -n polaris port-forward svc/polaris-console 8080:8080

# Needs `make polaris-forward` running in another terminal
query: .venv
	$(ENV) source .polaris-reader.env && .venv/bin/python scripts/query_polaris.py $(TABLE)

mysql:
	$(ENVS) $(KC) -n data-sources create secret generic mysql-secret \
	  --from-literal=root=$$MYSQL_ROOT_PW --from-literal=cdc=$$MYSQL_CDC_PW --dry-run=client -o yaml | $(KC) apply -f -
	$(ENVS) envsubst '$$MYSQL_CDC_PW' < k8s/data-sources/mysql.yaml | $(KC) -n data-sources apply -f -
	$(ENV) $(KC) -n data-sources rollout status statefulset/mysql --timeout=300s

postgres:
	$(ENVS) $(KC) -n data-sources create secret generic pg-secret \
	  --from-literal=postgres=$$PG_PW --from-literal=cdc=$$PG_CDC_PW --dry-run=client -o yaml | $(KC) apply -f -
	$(ENVS) envsubst '$$PG_CDC_PW' < k8s/data-sources/postgres.yaml | $(KC) -n data-sources apply -f -
	$(ENV) $(KC) -n data-sources rollout status statefulset/postgres --timeout=300s

connect: connect-image confluent-secret
	$(ENV) $(KC) -n connect apply -f k8s/connect/connect.yaml
	$(ENV) $(KC) -n connect rollout status deploy/connect --timeout=300s

# Renders connectors/*.json (secrets only travel through the pipe, never to disk) and PUTs them to the Connect REST API
connectors:
	$(ENVS) confluent kafka topic create debezium-schema-history-mysql --partitions 1 --cluster $$CLUSTER_ID --environment $$ENV_ID || true
	$(ENVS) for c in mysql-cdc postgres-cdc; do echo "== $$c-shop"; \
	  envsubst '$$KAFKA_BOOTSTRAP $$KAFKA_KEY $$KAFKA_SECRET $$MYSQL_CDC_PW $$PG_CDC_PW' < connectors/$$c.json | \
	  $(KC) -n connect exec -i deploy/connect -- sh -c 'code=$$(curl -sS -o /tmp/resp -w "%{http_code}" -X PUT \
	    -H "Content-Type: application/json" --data @- http://localhost:8083/connectors/'"$$c"'-shop/config); if [ "$$code" -lt 300 ]; then echo "HTTP $$code ok"; else echo "HTTP $$code"; cat /tmp/resp; echo; exit 1; fi' \
	    || exit 1; done

connectors-status:
	$(ENV) $(KC) -n connect exec deploy/connect -- curl -s 'http://localhost:8083/connectors?expand=status' | jq

flink-help:
	@echo "Run the statements in flink/*.sql in the Console Flink workspace (catalog = environment, database = cluster),"
	@echo "or: confluent flink shell --compute-pool \$$POOL_ID --environment \$$ENV_ID"
	@echo "Wait for topics mysql_customers and pg_products to exist before 'make tableflow-cdc'."

# TOPICS defaults to both decoded topics; e.g. make tableflow-cdc TOPICS=mysql_customers
TOPICS ?= mysql_customers pg_products
tableflow-cdc:
	$(ENVS) for t in $(TOPICS); do confluent tableflow topic enable $$t --cluster $$CLUSTER_ID --environment $$ENV_ID \
	  --storage-type BYOS --provider-integration $$PI_ID --bucket-name $$BUCKET --table-formats ICEBERG; done

trino:
	helm repo add trino https://trinodb.github.io/charts >/dev/null && helm repo update >/dev/null
	$(ENVS) source .polaris-reader.env && envsubst '$$POLARIS_READER_ID $$POLARIS_READER_SECRET $$AWS_REGION' < k8s/trino/values.yaml > k8s/trino/values.rendered.yaml
	$(ENV) helm upgrade --install trino trino/trino --kube-context kind-$$KIND_CLUSTER -n query -f k8s/trino/values.rendered.yaml
	$(ENV) $(KC) -n query rollout status deploy/trino-coordinator --timeout=300s
	@echo "Query: kubectl --context kind-<KIND_CLUSTER> -n query exec -it deploy/trino-coordinator -- trino --catalog polaris"

demo-changes:
	./scripts/cdc-changes.sh

down-kind:
	$(ENV) kind delete cluster --name $$KIND_CLUSTER

down-aws:
	./scripts/99-down-aws.sh
