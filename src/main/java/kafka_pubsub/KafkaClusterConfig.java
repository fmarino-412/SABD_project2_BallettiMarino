package kafka_pubsub;

public class KafkaClusterConfig {
    public static final String FLINK_TOPIC = "flink-topic";
    public static final String KAFKA_STREAMS_TOPIC = "kafka-streams-topic";
    public static final String QUERY_1_DAILY_TOPIC = "kafka-streams-output-topic-query1-daily";

    public static final String KAFKA_SINGLE_BROKER = "localhost:9092";

    public static final String BOOTSTRAP_SERVERS = KAFKA_SINGLE_BROKER;

    public static final String PRODUCER_ID = "single-producer";
    public static final String CONSUMER_ID = "single-flink-consumer";
}
