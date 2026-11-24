
# Kafka Troubleshooting Guide

This document records the troubleshooting experiments performed in the local Apache Kafka 3.9.1 KRaft lab.

## 1. ISR Failure and Recovery

### Configuration

- Brokers: 3
- Replication factor: 3
- Minimum ISR: 3 for the failure demonstration topic
- Producer acknowledgement: all

### Failure simulation

Stopped broker 3:

```bash
docker stop kafka3
```

Inspected topic replication:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe --topic ISRFailureDemo
```

Observed that ISR reduced to two replicas.

### Error reproduced

The producer using `acks=all` failed with:

```text
NOT_ENOUGH_REPLICAS
NotEnoughReplicasException
```

### Recovery

Restarted broker 3:

```bash
docker start kafka3
```

Waited for replication recovery and checked ISR again.

Verified all three replicas returned to the ISR.

A subsequent producer write succeeded.

## 2. Consumer Lag Monitoring

Used the `exporthub` consumer group.

Inspected group offsets:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group exporthub --describe
```

Observed lag accumulation while messages were waiting to be consumed.

Restarted the consumer and verified that committed offsets caught up with log-end offsets.

Final observed lag: zero across all three partitions.

## 3. Consumer Group Rebalancing

Started two consumers using the same group ID:

```text
exporthub
```

Inspected active members and assignments:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group exporthub --describe --members --verbose
```

Stopped one consumer and observed partition reassignment to the remaining consumer.

Restarted the second consumer and observed another rebalance.

## 4. Slow Consumer and CommitFailedException

### Java consumer configuration

The Java application uses:

- `enable.auto.commit=false`
- `max.poll.records=1`
- `auto.offset.reset=earliest`
- Manual `commitSync()`
- A simulated 15-second processing delay

### Failure reproduction

Configured:

```java
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
```

The 15-second processing delay exceeded the configured 10-second poll interval.

Observed:

```text
CommitFailedException
```

The consumer was no longer an active group member when it attempted to commit.

### Corrective demonstration

Changed the configuration to:

```java
props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "30000");
```

Rebuilt the application and reran it.

Processing and manual offset commits succeeded.

### Verification

Inspected the consumer group:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 \
  --group slow-consumer-demo --describe
```

Observed committed offsets matching log-end offsets, with zero lag.

## 5. KRaft Quorum Verification

Checked controller quorum status:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka1:9092 describe --status
```

Final observed state:

- Controller leader: broker 2
- Current voters: 3
- Maximum follower lag: 0
- Maximum follower lag time: 279 ms

All three voters were present.

## 6. Topic Replication Verification

Checked `DistributionTopic`:

```bash
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe --topic DistributionTopic
```

Final observed state:

| Partition | Leader | ISR |
|---|---|---|
| 0 | 1 | 1,2,3 |
| 1 | 2 | 2,1,3 |
| 2 | 3 | 1,2,3 |

All three replicas were in the ISR for every partition.

## 7. Docker and Resource Diagnostics

Checked container resource utilization:

```bash
docker stats --no-stream
```

Checked container status, OOM state and restart count:

```bash
docker inspect -f '{{.Name}} | Status={{.State.Status}} | OOMKilled={{.State.OOMKilled}} | RestartCount={{.RestartCount}}' kafka1 kafka2 kafka3
```

Checked WSL memory:

```bash
free -h
```

Checked high-memory processes:

```bash
ps aux --sort=-%mem | head -15
```

Jenkins was identified as a significant WSL memory consumer and temporarily stopped when not in use.

Available WSL memory increased from approximately 1.1 GiB to 1.6 GiB.

No broker was OOM-killed, and all three containers had a restart count of zero.

## 8. Final Assessment

The lab successfully demonstrated:

- ISR shrink and recovery.
- Producer failure with insufficient replicas.
- Consumer lag accumulation and recovery.
- Consumer group rebalancing.
- Slow-consumer commit failure and corrective configuration.
- KRaft quorum inspection.
- Broker and Docker resource diagnostics.

The final cluster verification showed all three KRaft voters present and all replicas synchronized.

**Scope limitation:** These results belong to the local demonstration environment. The earlier controller elections were observed, but their root cause was not conclusively established.

