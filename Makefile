SHELL := /bin/bash
ENVS  := source env.sh && source secrets.env &&
TOPICS_TF := orders_events mysql_customers pg_products

.PHONY: help azure kind namespaces clients-secret producer-image clients polaris polaris-forward bootstrap-polaris \
        mysql postgres tunnels connectors flink-help tableflow-orders tableflow-cdc trino query demo-changes down-azure

help:
	@echo "Part A  : make azure kind namespaces"
	@echo "Part C  : make producer-image clients-secret clients tableflow-orders polaris bootstrap-polaris query"
	@echo "Part D/E: make mysql postgres tunnels connectors flink-help tableflow-cdc"
	@echo "Part F  : make trino"
	@echo "Demo    : make demo-changes"
	@echo "Cleanup : make down-kind down-azure"
	@echo "Confluent env/cluster/keys/provider integration are manual: see BUILD_GUIDE.md Part B"

azure:
	./scripts/01-azure.sh

kind:
	$(ENVS) kind create cluster --name $$KIND_CLUSTER --config kind/kind-config.yaml
	kubectl config use-context kind-$$KIND_CLUSTER

namespaces:
	kubectl apply -f k8s/namespaces.yaml

producer-image:
	$(ENVS) docker build -t producer:v1 apps/producer
	$(ENVS) kind load docker-image producer:v1 --name $$KIND_CLUSTER

clients-secret:
	$(ENVS) kubectl -n clients create secret generic confluent-client \
	  --from-literal=KAFKA_BOOTSTRAP=$$KAFKA_BOOTSTRAP --from-literal=KAFKA_KEY=$$KAFKA_KEY --from-literal=KAFKA_SECRET=$$KAFKA_SECRET \
	  --from-literal=SR_URL=$$SR_URL --from-literal=SR_KEY=$$SR_KEY --from-literal=SR_SECRET=$$SR_SECRET \
	  --dry-run=client -o yaml | kubectl apply -f -

clients:
	$(ENVS) confluent kafka topic create orders_events --partitions 3 --cluster $$CLUSTER_ID --environment $$ENV_ID || true
	$(ENVS) kubectl -n clients apply -f k8s/clients/clients.yaml

tableflow-orders:
	$(ENVS) confluent tableflow topic enable orders_events --cluster $$CLUSTER_ID --environment $$ENV_ID \
	  --storage-type AzureDataLakeStorageGen2 --provider-integration $$PI_ID \
	  --storage-account-name $$SA --container-name $$CONTAINER --table-formats ICEBERG

polaris:
	$(ENVS) kubectl -n polaris create secret generic polaris-azure \
	  --from-literal=AZURE_TENANT_ID=$$TENANT_ID --from-literal=AZURE_CLIENT_ID=$$POLARIS_CLIENT_ID \
	  --from-literal=AZURE_CLIENT_SECRET=$$POLARIS_CLIENT_SECRET --dry-run=client -o yaml | kubectl apply -f -
	$(ENVS) envsubst '$$POLARIS_ROOT_SECRET' < k8s/polaris/polaris.yaml | kubectl -n polaris apply -f -
	kubectl -n polaris rollout status deploy/polaris

polaris-forward:
	kubectl -n polaris port-forward svc/polaris 8181:8181

bootstrap-polaris:
	./scripts/bootstrap-polaris.sh

query:
	$(ENVS) source .polaris-reader.env && python3 scripts/query_polaris.py $(TABLE)

mysql:
	$(ENVS) kubectl -n data-sources create secret generic mysql-secret \
	  --from-literal=root=$$MYSQL_ROOT_PW --from-literal=cdc=$$MYSQL_CDC_PW --dry-run=client -o yaml | kubectl apply -f -
	$(ENVS) envsubst '$$MYSQL_CDC_PW' < k8s/data-sources/mysql.yaml | kubectl -n data-sources apply -f -
	kubectl -n data-sources rollout status statefulset/mysql

postgres:
	$(ENVS) kubectl -n data-sources create secret generic pg-secret \
	  --from-literal=postgres=$$PG_PW --from-literal=cdc=$$PG_CDC_PW --dry-run=client -o yaml | kubectl apply -f -
	$(ENVS) envsubst '$$PG_CDC_PW' < k8s/data-sources/postgres.yaml | kubectl -n data-sources apply -f -
	kubectl -n data-sources rollout status statefulset/postgres

# Public TCP tunnels (ngrok) from Confluent Cloud to the kind DB NodePorts. Keep running; copy the printed host/port into env.sh
tunnels:
	./scripts/tunnels.sh

connectors:
	$(ENVS) envsubst < connectors/mysql-cdc.json > connectors/mysql-cdc.rendered.json
	$(ENVS) envsubst < connectors/postgres-cdc.json > connectors/postgres-cdc.rendered.json
	$(ENVS) confluent connect cluster create --config-file connectors/mysql-cdc.rendered.json --cluster $$CLUSTER_ID --environment $$ENV_ID
	$(ENVS) confluent connect cluster create --config-file connectors/postgres-cdc.rendered.json --cluster $$CLUSTER_ID --environment $$ENV_ID

flink-help:
	@echo "Run the statements in flink/*.sql in the Console Flink workspace (catalog = environment, database = cluster),"
	@echo "or: confluent flink shell --compute-pool \$$POOL_ID --environment \$$ENV_ID"
	@echo "Wait for topics mysql_customers and pg_products to exist before 'make tableflow-cdc'."

tableflow-cdc:
	$(ENVS) for t in mysql_customers pg_products; do confluent tableflow topic enable $$t --cluster $$CLUSTER_ID --environment $$ENV_ID \
	  --storage-type AzureDataLakeStorageGen2 --provider-integration $$PI_ID \
	  --storage-account-name $$SA --container-name $$CONTAINER --table-formats ICEBERG; done

trino:
	helm repo add trino https://trinodb.github.io/charts >/dev/null && helm repo update >/dev/null
	$(ENVS) source .polaris-reader.env && envsubst '$$POLARIS_READER_ID $$POLARIS_READER_SECRET' < k8s/trino/values.yaml > k8s/trino/values.rendered.yaml
	helm upgrade --install trino trino/trino -n query -f k8s/trino/values.rendered.yaml
	kubectl -n query rollout status deploy/trino-coordinator
	@echo "Query: kubectl -n query exec -it deploy/trino-coordinator -- trino --catalog polaris"

demo-changes:
	./scripts/cdc-changes.sh

down-kind:
	$(ENVS) kind delete cluster --name $$KIND_CLUSTER

down-azure:
	$(ENVS) az group delete -n $$RG --yes --no-wait
	@echo "Confluent resources must be torn down first, see BUILD_GUIDE.md Part H."
