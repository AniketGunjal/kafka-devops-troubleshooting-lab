# Kafka DevOps Troubleshooting Lab

A hands-on Apache Kafka troubleshooting and reliability lab built using Docker Compose, KRaft mode, and Java.

This project demonstrates practical Kafka administration scenarios, including broker availability, replication, ISR recovery, consumer lag, consumer group rebalancing, and slow-consumer offset commit failures.

> **Environment:** Local demonstration lab using Apache Kafka 3.9.1. This project is separate from my professional production experience.

## Architecture

- Apache Kafka 3.9.1
- Three Kafka brokers with combined broker/controller roles
- KRaft metadata quorum
- Docker Compose
- Replication factor: 3
- Minimum in-sync replicas: 2
- Three partitions in `DistributionTopic`
- Java 17
- Maven
- Kafka Java Client 3.9.1

### Broker endpoints

| Broker | Internal endpoint | External endpoint |
|---|---|---|
| kafka1 | kafka1:9092 | localhost:19092 |
| kafka2 | kafka2:9092 | localhost:29092 |
| kafka3 | kafka3:9092 | localhost:39092 |

## Project structure

```text
Kafka-DevOps-Troubleshooting-Lab/
├── README.md
├── docker-compose.yml
├── configs/
├── scripts/
├── evidence/
├── docs/
│   └── troubleshooting.md
└── java-slow-consumer/
    ├── pom.xml
    └── src/main/java/com/devops/kafka/SlowConsumer.java

## Prerequisites

- Docker Desktop with WSL 2 integration
- Ubuntu/WSL or Linux terminal
- Java 17
- Maven

Three Kafka brokers require sufficient memory and CPU resources.

## Start the Kafka cluster

From the project directory:

```bash
docker compose up -d
```

Check broker status:

```bash
docker ps
```

Check KRaft quorum:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:9092 describe --status
```

## Inspect topic replication

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe --topic DistributionTopic
```

## Stop the cluster

```bash
docker compose down
```

This preserves persistent Kafka data volumes. Avoid `docker compose down -v` unless you intentionally want to delete them.


## Troubleshooting Scenarios Demonstrated

### 1. Kafka ISR Failure and Recovery

- Created a topic with replication factor 3 and minimum ISR 3.
- Stopped one Kafka broker.
- Observed ISR shrink from three replicas to two.
- Tested a producer with `acks=all`.
- Reproduced `NotEnoughReplicasException` and `NOT_ENOUGH_REPLICAS`.
- Restarted the broker.
- Verified ISR recovery to all three replicas.
- Successfully produced a message after recovery.

### 2. Consumer Lag and Offset Recovery

- Published messages to `DistributionTopic`.
- Consumed messages using the `exporthub` consumer group.
- Inspected committed offsets and consumer lag.
- Demonstrated lag accumulation.
- Restarted the consumer.
- Verified that consumer lag returned to zero.

### 3. Consumer Group Rebalancing

- Started multiple consumers in the same consumer group.
- Inspected partition assignments.
- Stopped one consumer and observed partition reassignment.
- Restarted the second consumer and observed another rebalance.

### 4. Slow Consumer and Commit Failure

Implemented a Java Kafka consumer with:

- Manual offset commits.
- `max.poll.records=1`.
- Simulated 15-second processing delay.
- Configurable `max.poll.interval.ms`.

With `max.poll.interval.ms=10000`, reproduced `CommitFailedException`.

Changed the interval to `30000` and verified successful processing and manual offset commits.

See `java-slow-consumer/` for the implementation.

### 5. Broker and Resource Diagnostics

Inspected:

- KRaft controller quorum and elections.
- Broker logs.
- Docker CPU and memory usage.
- Container restart counts.
- OOM-kill status.
- WSL memory and swap usage.

Final verification showed all three KRaft voters present and all three replicas in the ISR for every partition.

## Learning Outcomes

- Kafka KRaft architecture and controller quorum.
- Broker availability and replication.
- ISR monitoring and recovery.
- Producer acknowledgements and replication errors.
- Consumer offsets and lag monitoring.
- Consumer group rebalancing.
- Java Kafka consumer configuration.
- Slow-consumer poll interval and commit behavior.
- Docker-based troubleshooting and resource monitoring.

## Evidence

Screenshots and command outputs will be added to the `evidence/` directory.

> This is a local demonstration project. Lab experiments reproduce Kafka failure and recovery scenarios for learning and portfolio purposes; they are not recordings from previous production incidents.

