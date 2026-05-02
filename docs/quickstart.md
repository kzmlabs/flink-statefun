# Quickstart

Run a small StateFun job locally with Docker, send a message through Kafka, and verify the response — five minutes start to finish.

## Prerequisites

- Docker (or compatible runtime)
- `curl`
- `kafkacat` or any Kafka CLI of choice (optional, for sending test messages)

## 1. Start the local stack

```bash
git clone https://github.com/kzmlabs/flink-statefun.git
cd flink-statefun/dev
docker compose up -d
```

This brings up:

- **Flink 2.2** JobManager + TaskManager
- **Apache Kafka** (single broker)
- **A remote function HTTP server** with a sample `GreeterFn`

## 2. Submit a message

```bash
docker exec statefun-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic dev.events.test-ingress --partitions 1 --replication-factor 1

echo 'alice:{"message": "Hello!"}' | docker exec -i statefun-kafka \
  kafka-console-producer --broker-list localhost:9092 \
  --topic dev.events.test-ingress \
  --property "parse.key=true" --property "key.separator=:"
```

## 3. Observe the response

```bash
docker logs -f statefun-remote-function | grep "GreeterFn"
```

You should see a per-key invocation logged, with the function's response routed to the egress topic.

## What's next

- [Install StateFun in your own project](install.md) — Maven coordinates and BOM
- [Build from source](build.md) — full reactor build with the K8s E2E gate
- [Kafka I/O guide](guides/kafka-io.md) — ingress/egress configuration patterns
- [Kubernetes deployment](guides/k8s-deployment.md) — production layout with Flink Operator
