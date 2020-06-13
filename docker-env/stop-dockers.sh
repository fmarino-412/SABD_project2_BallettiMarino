#!/bin/bash
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink_topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka_streams_topic

docker-compose down
