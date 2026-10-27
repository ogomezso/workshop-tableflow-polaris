# Tableflow + Self-Hosted Apache Polaris on kind (demo)

Kafka clients, a fully managed MySQL CDC connector and a PostgreSQL CDC connector feed Confluent Cloud on Azure. Tableflow materializes Iceberg tables in ADLS Gen2. A self-hosted Apache Polaris on a local kind cluster federates to the Tableflow Iceberg REST Catalog and serves the tables read-only to Trino / PyIceberg.

- `BUILD_GUIDE.md`: the step-by-step runbook (start here)
- `STATEMENT_OF_WORK.md`: scope, architecture decisions, risks, estimates

## Layout

```
apps/producer/        Kafka producer + consumer (Avro + Schema Registry), one image
connectors/           MySQL / PostgreSQL CDC connector configs (${VAR} templates)
flink/                Decode SQL: Debezium topics -> upsert topics for Tableflow
kind/                 kind cluster config (NodePort host-port mappings)
k8s/                  namespaces, data sources, clients, Polaris, Trino values
scripts/              01-azure.sh, tunnels.sh, 02-confluent-storage-access.sh, bootstrap-polaris.sh, query_polaris.py, cdc-changes.sh
env.sh.example        Azure/Confluent/tunnel variables -> copy to env.sh
ngrok.yml.example     ngrok TCP tunnels for MySQL/Postgres -> copy to ngrok.yml
secrets.env.example   credentials and passwords      -> copy to secrets.env
Makefile              shortcuts for the steps below
```

## Quick path

```bash
cp env.sh.example env.sh; cp secrets.env.example secrets.env   # fill in SUFFIX and generate passwords first
source env.sh
make azure kind namespaces            # Part A  (then copy the printed exports into env.sh)
# Part B: Confluent env, cluster, API keys, provider integration (manual, see BUILD_GUIDE.md)
./scripts/02-confluent-storage-access.sh
make producer-image clients-secret clients tableflow-orders        # Part C
make polaris                          # then, in another terminal: make polaris-forward
make bootstrap-polaris query TABLE=orders_events                   # GO / NO-GO checkpoint
make mysql postgres                   # Parts D and E
make tunnels                          # keep running in another terminal; copy host/port into env.sh
make connectors
make flink-help                       # run flink/*.sql, then:
make tableflow-cdc
make trino
make demo-changes
```

## Notes

- `scripts/bootstrap-polaris.sh` needs a port-forward to Polaris on 8181 and re-runs after any Polaris restart (in-memory state).
- Azure-specific Polaris settings (`AZURE` storage config, service principal roles and SAS vending) are unverified; see the **(verify)** tags in `BUILD_GUIDE.md`. Do Part C first.
- Prerequisites: `kind`, `docker`, `ngrok` (auth token), `kubectl`, `helm`, `az`, `confluent`. Cleanup: `make down-kind down-azure`.
- Never commit `env.sh`, `secrets.env`, `.polaris-reader.env` or rendered connector files (all git-ignored).
