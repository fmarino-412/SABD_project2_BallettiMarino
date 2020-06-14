#!/bin/bash
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query1-daily

docker-compose down
