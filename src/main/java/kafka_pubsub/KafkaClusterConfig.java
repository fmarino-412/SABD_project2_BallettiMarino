package kafka_pubsub;

public class KafkaClusterConfig {
    public static final String FLINK_TOPIC = "flink-topic";
    public static final String KAFKA_STREAMS_TOPIC = "kafka-streams-topic";
    public static final String QUERY_1_DAILY_TOPIC = "kafka-streams-output-topic-query1-daily";
    public static final String QUERY_1_WEEKLY_TOPIC = "kafka-streams-output-topic-query1-weekly";
    public static final String QUERY_1_MONTHLY_TOPIC = "kafka-streams-output-topic-query1-monthly";
    public static final String QUERY_2_DAILY_TOPIC = "kafka-streams-output-topic-query2-daily";
    public static final String QUERY_2_WEEKLY_TOPIC = "kafka-streams-output-topic-query2-weekly";
    public static final String QUERY_3_DAILY_TOPIC = "kafka-streams-output-topic-query3-daily";
    public static final String QUERY_3_WEEKLY_TOPIC = "kafka-streams-output-topic-query3-weekly";


    public static final String KAFKA_BROKER_1 = "localhost:9092";
    public static final String KAFKA_BROKER_2 = "localhost:9093";
    public static final String KAFKA_BROKER_3 = "localhost:9094";

    public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_1 + "," +
                                                    KAFKA_BROKER_2 + "," +
                                                    KAFKA_BROKER_3;

    public static final String PRODUCER_ID = "single-producer";
    public static final String CONSUMER_ID = "single-flink-consumer";
}
