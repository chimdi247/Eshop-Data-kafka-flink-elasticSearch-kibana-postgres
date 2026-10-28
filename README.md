# FlinkCommerce: real-time e-commerce analytics

This repository runs as a Docker Compose stack on an Ubuntu VM. It contains a Java Apache Flink streaming job, a Python transaction generator, Kafka, PostgreSQL, Elasticsearch, and Kibana. There is **no Spring Boot application** in this repository; the Java application is an Apache Flink job.

## Stack and version strategy

Versions are pinned to avoid accidental upgrades during deployment. Flink and connector versions are pinned to the latest patch releases compatible with the existing DataStream APIs in this codebase rather than blindly mixing incompatible major versions.

| Component | Version / runtime |
|---|---|
| Apache Flink | 1.20.5 (latest patch in the 1.20 connector-compatible line) |
| Java | 17 LTS |
| Apache Kafka | 4.4.0 (KRaft; no ZooKeeper) |
| PostgreSQL | 18.6 |
| Elasticsearch / Kibana | 8.19.12, kept on the same version |
| Python | 3.14.10 |
| confluent-kafka | 2.15.1 |
| Faker | 40.43.0 |
| Maven | 3.10.0 in the build image |

Python does not have a formal LTS release model; Python 3.14 is pinned to a patch release here. Flink also does not publish a conventional LTS designation. Flink 2.x is newer, but this code uses the Flink 1.x DataStream JDBC sink API, so this package stays on the latest 1.20 connector-compatible line to avoid an unvalidated major-version/API migration. Check upstream release pages before planned upgrades.

## Requirements for the Ubuntu VM

- A 64-bit Ubuntu VM with Docker Engine and the Docker Compose v2 plugin.
- Recommended for this complete stack: at least 8 GB RAM and 4 CPU cores. Elasticsearch and Flink are memory-intensive.
- Configure the Elasticsearch host kernel setting before starting the stack:

  ```bash
  echo 'vm.max_map_count=262144' | sudo tee /etc/sysctl.d/99-elasticsearch.conf
  sudo sysctl --system
  ```
- Inbound access to the dashboard ports only if you need them remotely. Restrict access with the VM firewall/security group.

Install Docker Engine using the official Docker Ubuntu instructions:
https://docs.docker.com/engine/install/ubuntu/

Verify:

```bash
docker --version
docker compose version
```

## Run the entire project

1. Extract or clone this repository and enter its root directory (the directory containing `docker-compose.yml`).
2. Create a local environment file:

   ```bash
   cp .env.example .env
   nano .env
   ```

   The values in `.env.example` are for a local demo only. Change the database password before exposing any ports outside a trusted development environment.

3. Build the Flink job and Python generator images, then start the stack:

   ```bash
   docker compose up --build -d
   ```

4. Check the service status:

   ```bash
   docker compose ps
   docker compose logs -f flink-job transaction-generator
   ```

   `flink-job` submits the compiled Java job to the Flink JobManager and exits after submission. The actual streaming job runs in the Flink cluster. `transaction-generator` runs continuously until stopped.

## Web interfaces and ports

- Flink dashboard: http://localhost:8081
- Kibana: http://localhost:5601
- Elasticsearch API: http://localhost:9200
- Kafka external listener: `localhost:9092`
- PostgreSQL: `localhost:5432`

When using a remote Ubuntu VM, replace `localhost` with the VM IP or DNS name for browser/API access. Set `KAFKA_EXTERNAL_HOST` in `.env` to the VM IP/DNS name if Kafka clients outside the VM need to connect. Do not expose database or unauthenticated Elasticsearch/Kibana ports to the public internet; use an SSH tunnel or firewall rules. This compose file disables Elasticsearch security for local development.

## Verify data is flowing

Check the Flink dashboard at `http://localhost:8081`, then inspect the services:

```bash
# Kafka topic
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:29092 --list

# Sample records in the topic
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:29092 \
  --topic financial_transactions --from-beginning --max-messages 5

# PostgreSQL tables (created by the Flink job)
docker compose exec postgres psql -U postgres -d postgres -c '\dt'

# Elasticsearch indices
curl http://localhost:9200/_cat/indices?v
```

If you changed `POSTGRES_USER` or `POSTGRES_DB` in `.env`, use those values in the `psql` command.

## Lifecycle commands

```bash
# Follow logs
docker compose logs -f

# Stop containers but preserve database and Kafka data
docker compose down

# Stop and delete containers AND persisted data (destructive)
docker compose down -v

# Rebuild application images
docker compose build --no-cache flink-job transaction-generator
```

## Configuration

Compose reads `.env` in the project root. Available settings:

- `KAFKA_EXTERNAL_HOST` (use the VM IP/DNS name if clients outside the VM connect to Kafka)
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `GENERATOR_INTERVAL_SECONDS`
- `GENERATOR_DURATION_SECONDS` (`0` means continuous)

Internal container-to-container connections use service DNS names (`broker`, `postgres`, `elasticsearch`), not `localhost`.

## Notes before production

This is a single-node development/demo deployment, not a highly available production cluster. Elasticsearch security is disabled and the sample PostgreSQL credentials are intentionally simple defaults. Before production use, configure authentication/TLS, secrets management, restricted network access, resource limits, backups, and multi-node durability. Pin container images by digest if you need reproducible supply-chain deployments.

The source code currently performs simple rolling aggregations and uses processing-time dates in its sales aggregate sinks. Review the aggregation and event-time semantics before treating the results as financial reporting.
