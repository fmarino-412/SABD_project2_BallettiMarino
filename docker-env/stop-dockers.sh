#!/bin/bash
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic flink-topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-topic
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query1-daily
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query1-weekly
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query1-monthly
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query2-daily
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query2-weekly
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query3-daily
$KAFKA_HOME/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic kafka-streams-output-topic-query3-weekly

docker-compose down