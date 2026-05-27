# Microservices SASL/SCRAM security configs

Each Kafka Streams microservice authenticates as its own SCRAM principal via `-c config/security/<service>.properties`.

## Cluster layout (default): 2 brokers + 1 controller

| JVM | `node.id` | Role | Client port | Log directory |
|-----|-----------|------|-------------|---------------|
| Controller | 1 | `controller` | — (internal `:9093`) | `/tmp/kafka-logs-ms-sasl-controller` |
| Broker 1 | 2 | `broker` | SASL `:9092` | `/tmp/kafka-logs-ms-sasl-broker-1` |
| Broker 2 | 3 | `broker` | SASL `:9094` | `/tmp/kafka-logs-ms-sasl-broker-2` |

`controller.quorum.voters=1@localhost:9093` on all nodes. Brokers register to the controller as `User:ANONYMOUS` on PLAINTEXT `:9093` (`super.users` includes `ANONYMOUS`). Microservices use SCRAM on `:9092` and `:9094`.

**Bootstrap servers:** `localhost:9092,localhost:9094` (set in `_env.sh` as `BOOTSTRAP_SERVERS`).

### Service principals

| Service | SCRAM user | Start script | Config file |
|---------|------------|--------------|-------------|
| FraudService | `fraud-svc` | `start-fraud-service.sh` | `fraud-service.properties` |
| InventoryService | `inventory-svc` | `start-inventory-service.sh` | `inventory-service.properties` |
| OrderDetailsService | `order-details-svc` | `start-order-details-service.sh` | `order-details-service.properties` |
| ValidationsAggregatorService | `validations-agg-svc` | `start-validations-aggregator-service.sh` | `validations-aggregator-service.properties` |
| OrdersService | `orders-svc` | `start-orders-service.sh` | `orders-service.properties` |
| EmailService | `email-svc` | `start-email-service.sh` | `email-service.properties` |
| AddInventory | `inventory-svc` | `start-add-inventory.sh` | `add-inventory.properties` |

Admin / broker JAAS: `admin` / `admin-secret` (`kafka_server_jaas.conf`, `admin-client.properties`).

## Prerequisites

- `difc-for-kafka` at `../difc-for-kafka` (or set `KAFKA_HOME`)
- `mvn -DskipTests=true package` in `kafka-streams-examples`
- Confluent Schema Registry (`../confluent-8.1.1` or set `CONFLUENT_HOME`)

## Setup (first time)

```bash
cd kafka-streams-examples/config/security
chmod +x *.sh

./wipe-cluster-logs.sh          # optional: clean old single-node logs
./format-kraft-storage.sh       # formats controller + both brokers, seeds SCRAM users

# Start in order (three terminals):
./start-controller.sh           # wait until controller is ready
./start-broker-1.sh             # :9092
./start-broker-2.sh             # :9094

./start-schema-registry.sh      # http://localhost:8081
./create-acls.sh                # required: grant SCRAM principals access to topics
./create-topics.sh              # required: business input/output topics (see below)
./verify-principal.sh           # optional
./list-acls.sh fraud-svc        # optional: inspect ACLs for a principal
```

If storage was formatted without SCRAM users: `./create-scram-users.sh` (broker must be up).

### Business topics (`create-topics.sh`)

| Topic | Role in workflow |
|-------|------------------|
| `orders` | OrdersService writes; validators read; aggregator updates state |
| `order-validations` | Validators write PASS/FAIL; aggregator reads |
| `warehouse-inventory` | AddInventory seeds; InventoryService reads |
| `payments` | EmailService (join) |
| `customers` | EmailService global table |
| `orders-enriched` | EmailService output |

Defaults: `TOPIC_PARTITIONS=6`, `TOPIC_REPLICATION_FACTOR=2` (for the 2-broker cluster).

Kafka Streams **internal** topics (`FraudService-*-repartition`, changelog stores, etc.) are still created automatically when each service starts (with ACLs in place).

## Run microservices

Use **one terminal per script** (each runs in the foreground; Ctrl+C stops that service only).

Suggested order:

```bash
./start-fraud-service.sh
./start-inventory-service.sh
./start-order-details-service.sh
./start-validations-aggregator-service.sh
./start-orders-service.sh          # REST http://localhost:5432/v1/orders
./start-add-inventory.sh           # one-shot, then exit
# optional:
./start-email-service.sh
```

## Legacy single-node

Use `broker.standalone.properties` with `kafka-storage.sh format --standalone` and a single `kafka-server-start` (not recommended once you use the 2-broker layout).

## Environment variables

| Variable | Default |
|----------|---------|
| `KAFKA_HOME` | `../../difc-for-kafka` |
| `BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094` |
| `CONFLUENT_HOME` | `../../confluent-8.1.1` |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` |
| `ORDERS_PORT` | `5432` |
| `TOPIC_PARTITIONS` | `6` |
| `TOPIC_REPLICATION_FACTOR` | `2` |

## DIFC

DIFC `registerDifcClient` uses the authenticated SCRAM principal name (e.g. `orders-svc`). Idle `POLL_PRIVS_REQ` lines (`capability=-1`) are normal when no capability requests are queued.
