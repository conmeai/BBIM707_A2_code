#!/usr/bin/env bash
# BBIM707 A2 | Part B - Task 4: start the cluster, create the topic, inspect replication, simulate a broker failure
docker compose -f docker-compose-kafka.yml up -d
sleep 20
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create \
  --topic insurance.contracts --partitions 3 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create \
  --topic insurance.contracts.dlq --partitions 1 --replication-factor 3 --config min.insync.replicas=2
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic insurance.contracts
# --- after starting producer + 2 consumers, simulate failure of one broker and describe again:
# docker stop kafka2
# docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic insurance.contracts
# docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --describe --group insurance-analytics
