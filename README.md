# Kafka DevOps Troubleshooting Lab

A hands-on Apache Kafka troubleshooting and reliability lab built using Docker Compose, Apache Kafka 3.9.1, KRaft mode, and Java.

This project demonstrates practical Kafka administration, broker failure handling, replication, ISR recovery, producer reliability, consumer lag monitoring, consumer group rebalancing, and slow-consumer offset commit failures.

> **Environment:** Local demonstration lab. These experiments are separate from my professional production experience.

## Project Architecture

- Apache Kafka 3.9.1
- Three Kafka brokers with combined broker/controller roles
- KRaft metadata quorum
- Docker Compose
- Replication factor: 3
- Minimum in-sync replicas: 2 for `DistributionTopic`
- Three partitions in `DistributionTopic`
- Java 17
- Maven
- Kafka Java Client 3.9.1

### Broker Endpoints

| Broker | Internal endpoint | External endpoint |
|---|---|---|
| kafka1 | kafka1:9092 | localhost:19092 |
| kafka2 | kafka2:9092 | localhost:29092 |
| kafka3 | kafka3:9092 | localhost:39092 |

### Topics Used

| Topic | Partitions | Replication factor | Minimum ISR |
|---|---:|---:|---:|
| DistributionTopic | 3 | 3 | 2 |
| ISRFailureDemo | 3 | 3 | 3 |

## Project Structure

```text
Kafka-DevOps-Troubleshooting-Lab/
├── README.md
├── docker-compose.yml
├── configs/
├── scripts/
├── evidence/
│   ├── 07-final-cluster-health.png
│   ├── 08-broker-failure-isr-reduction.png
│   ├── 09-not-enough-replicas-error.png
│   ├── 10-isr-recovery-after-broker-restart.png
│   ├── 11-exporthub-consumer-lag-baseline.png
│   ├── 12-exporthub-consumer-lag-increased.png
│   ├── 13-exporthub-consumer-lag-recovered.png
│   ├── 14-exporthub-two-consumer-assignment.png
│   └── 15-exporthub-consumer-rebalancing.png
├── docs/
│   └── troubleshooting.md
└── java-slow-consumer/
    ├── pom.xml
    └── src/main/java/com/devops/kafka/SlowConsumer.java
```

## Prerequisites

- Docker Desktop with WSL 2 integration
- Ubuntu/WSL or Linux terminal
- Java 17
- Maven

Three Kafka brokers require sufficient memory and CPU resources.

## Start the Kafka Cluster

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
  --bootstrap-server localhost:9092 describe --status
```

## Inspect Topic Replication

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic DistributionTopic
```

Check consumer group offsets and lag:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group exporthub
```

## Stop the Kafka Cluster

```bash
docker compose down
```

This preserves persistent Kafka data volumes. Avoid `docker compose down -v` unless you intentionally want to delete them.

---

# Troubleshooting Case Studies

All results below were observed through experiments in the local three-broker Kafka lab.

## Case Study 1: Broker Failure, ISR Reduction and Recovery

**Scenario:** A Kafka broker becomes unavailable while topics have replicated partitions.

**Configuration:**
- Three Kafka brokers.
- Topic: `DistributionTopic`.
- Replication factor: 3.
- Minimum ISR: 2.

**Investigation:**

1. Verified that all three replicas were initially in the ISR.
2. Stopped broker 3.
3. Inspected partition leaders, replicas, and ISR.
4. Observed ISR decrease from three replicas to two.
5. Verified that all three partitions retained available leaders.

**Observed result:**

All three partitions retained two in-sync replicas while broker 3 was offline.

**Recovery:**

Restarted broker 3 and verified that all three replicas returned to the ISR.

**Technical learning:**

Under-replicated partitions and ISR reduction can be investigated using Kafka topic metadata. A topic configured with minimum ISR 2 can continue accepting qualifying writes while two replicas remain in sync.

**Evidence:**
- [Initial cluster health](evidence/07-final-cluster-health.png)
- [Broker failure and ISR reduction](evidence/08-broker-failure-isr-reduction.png)
- [ISR recovery after broker restart](evidence/10-isr-recovery-after-broker-restart.png)

## Case Study 2: Producer Failure — NOT_ENOUGH_REPLICAS

**Scenario:** A producer attempts to publish messages when the number of in-sync replicas is below the topic's configured minimum.

**Configuration:**
- Topic: `ISRFailureDemo`.
- Replication factor: 3.
- Minimum ISR: 3.
- Producer acknowledgement: `acks=all`.
- Available ISR during the failure test: 2.

**Investigation:**

1. Stopped broker 3.
2. Inspected the topic's ISR.
3. Confirmed that each partition had only two in-sync replicas.
4. Started a console producer with `acks=all`.
5. Attempted to publish a test message.

**Observed result:**

Kafka rejected the write with:

`NOT_ENOUGH_REPLICAS`

The producer output included:

`org.apache.kafka.common.errors.NotEnoughReplicasException`

**Technical learning:**

With `acks=all`, Kafka requires the configured minimum ISR to be satisfied before accepting a write. When the ISR falls below that threshold, the write is rejected.

**Evidence:**
- [NotEnoughReplicas error](evidence/09-not-enough-replicas-error.png)
- [ISR recovery after broker restart](evidence/10-isr-recovery-after-broker-restart.png)

## Case Study 3: Consumer Lag Monitoring and Recovery

**Scenario:** Messages accumulate while a consumer group has no active members.

**Environment:**
- Topic: `DistributionTopic`.
- Consumer group: `exporthub`.
- Three partitions.

**Investigation:**

1. Inspected consumer group offsets and lag.
2. Recorded an initial total lag of 2 messages.
3. Published 20 additional test messages while the group had no active consumers.
4. Inspected the group again.
5. Started a consumer in the existing group.
6. Verified the committed offsets and lag.

**Observed results:**

| Test stage | Total consumer lag |
|---|---:|
| Initial baseline | 2 |
| After publishing 20 messages | 22 |
| After starting the consumer | 0 |

In this experiment, the 20 additional messages landed on partition 1.

**Recovery:**

Started a consumer using the `exporthub` group. The consumer processed the pending records, and the observed lag returned to zero across all three partitions.

**Technical learning:**

Consumer lag is the difference between the log end offset and the consumer group's committed offset. Monitoring lag helps identify consumers that are not keeping up with incoming messages.

**Evidence:**
- [Consumer lag baseline](evidence/11-exporthub-consumer-lag-baseline.png)
- [Consumer lag increased](evidence/12-exporthub-consumer-lag-increased.png)
- [Consumer lag recovered](evidence/13-exporthub-consumer-lag-recovered.png)

## Case Study 4: Consumer Group Rebalancing

**Scenario:** Multiple consumers share topic partitions, and one consumer leaves the group.

**Environment:**
- Topic: `DistributionTopic`.
- Consumer group: `exporthub`.
- Three partitions.
- Two active consumers.

**Investigation:**

1. Started two consumers in the same consumer group.
2. Inspected their partition assignments.
3. Observed one consumer assigned two partitions and the other assigned one.
4. Stopped the second consumer.
5. Inspected the group again.

**Observed result:**

Kafka reassigned all three partitions to the remaining active consumer.

Consumer lag remained at zero.

**Technical learning:**

Kafka consumer groups automatically redistribute partition assignments when group membership changes. Rebalancing allows the remaining group members to take ownership of partitions when a consumer leaves.

**Evidence:**
- [Two-consumer partition assignment](evidence/14-exporthub-two-consumer-assignment.png)
- [Consumer group rebalancing](evidence/15-exporthub-consumer-rebalancing.png)

## Case Study 5: Slow Consumer and Offset Commit Failure

**Scenario:** A consumer takes longer to process records than the configured maximum poll interval.

**Implementation:**

Developed a Java Kafka consumer with:

- Manual offset commits.
- `max.poll.records=1`.
- Simulated 15-second processing delay.
- Configurable `max.poll.interval.ms`.

**Test results:**

| Configuration | Observed result |
|---|---|
| `max.poll.interval.ms=10000` | `CommitFailedException` |
| `max.poll.interval.ms=30000` | Successful processing and offset commit |

**Technical learning:**

When processing exceeds `max.poll.interval.ms`, a consumer can lose its group membership. A subsequent offset commit can fail because the consumer is no longer an active group member.

The configured poll interval must account for expected processing time.

See `java-slow-consumer/` for the implementation.

## Broker and Resource Diagnostics

Inspected the following during lab troubleshooting:

- KRaft controller quorum and elections.
- Broker logs.
- Docker CPU and memory utilization.
- Container restart counts.
- OOM-kill status.
- WSL memory and swap utilization.

Final cluster verification showed all three KRaft voters present and all three replicas in the ISR for every partition.

## Technical Skills Demonstrated

- Apache Kafka and KRaft architecture.
- Broker administration and troubleshooting.
- Topic replication and partition management.
- ISR monitoring and recovery.
- Producer acknowledgements and replication errors.
- Consumer offsets and lag monitoring.
- Consumer group membership and rebalancing.
- Java Kafka consumer development.
- Offset commit troubleshooting.
- Docker Compose and Linux command-line diagnostics.

## Evidence Gallery

The repository includes nine screenshots documenting the lab experiments.

| Evidence | Screenshot |
|---|---|
| Final cluster health | [View screenshot](evidence/07-final-cluster-health.png) |
| Broker failure and ISR reduction | [View screenshot](evidence/08-broker-failure-isr-reduction.png) |
| NotEnoughReplicas failure | [View screenshot](evidence/09-not-enough-replicas-error.png) |
| ISR recovery | [View screenshot](evidence/10-isr-recovery-after-broker-restart.png) |
| Consumer lag baseline | [View screenshot](evidence/11-exporthub-consumer-lag-baseline.png) |
| Consumer lag increase | [View screenshot](evidence/12-exporthub-consumer-lag-increased.png) |
| Consumer lag recovery | [View screenshot](evidence/13-exporthub-consumer-lag-recovered.png) |
| Two-consumer assignment | [View screenshot](evidence/14-exporthub-two-consumer-assignment.png) |
| Consumer group rebalancing | [View screenshot](evidence/15-exporthub-consumer-rebalancing.png) |

## Project Disclaimer

This repository is a local demonstration and portfolio project built to reproduce Kafka troubleshooting scenarios.

The experiments and screenshots document results from this lab environment. They should not be interpreted as recordings of production incidents or production monitoring data.

