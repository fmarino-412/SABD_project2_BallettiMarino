#!/bin/bash
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-topic

docker-compose down
