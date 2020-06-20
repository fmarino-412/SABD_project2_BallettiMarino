package kafka_pubsub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaClusterConfig {
	public static final String FLINK_TOPIC = "flink-topic";
	public static final String FLINK_QUERY_1_DAILY_TOPIC = "flink-output-topic-query1-daily";
	public static final String FLINK_QUERY_1_WEEKLY_TOPIC = "flink-output-topic-query1-weekly";
	public static final String FLINK_QUERY_1_MONTHLY_TOPIC = "flink-output-topic-query1-monthly";
	public static final String FLINK_QUERY_2_DAILY_TOPIC = "flink-output-topic-query2-daily";
	public static final String FLINK_QUERY_2_WEEKLY_TOPIC = "flink-output-topic-query2-weekly";
	public static final String FLINK_QUERY_3_DAILY_TOPIC = "flink-output-topic-query3-daily";
	public static final String FLINK_QUERY_3_WEEKLY_TOPIC = "flink-output-topic-query3-weekly";
	public static final String KAFKA_STREAMS_TOPIC = "kafka-streams-topic";
	public static final String KAFKA_QUERY_1_DAILY_TOPIC = "kafka-streams-output-topic-query1-daily";
	public static final String KAFKA_QUERY_1_WEEKLY_TOPIC = "kafka-streams-output-topic-query1-weekly";
	public static final String KAFKA_QUERY_1_MONTHLY_TOPIC = "kafka-streams-output-topic-query1-monthly";
	public static final String KAFKA_QUERY_2_DAILY_TOPIC = "kafka-streams-output-topic-query2-daily";
	public static final String KAFKA_QUERY_2_WEEKLY_TOPIC = "kafka-streams-output-topic-query2-weekly";
	public static final String KAFKA_QUERY_3_DAILY_TOPIC = "kafka-streams-output-topic-query3-daily";
	public static final String KAFKA_QUERY_3_WEEKLY_TOPIC = "kafka-streams-output-topic-query3-weekly";
	public static final String[] FLINK_TOPICS = {FLINK_QUERY_1_DAILY_TOPIC, FLINK_QUERY_1_WEEKLY_TOPIC,
			FLINK_QUERY_1_MONTHLY_TOPIC, FLINK_QUERY_2_DAILY_TOPIC, FLINK_QUERY_2_WEEKLY_TOPIC,
			FLINK_QUERY_3_DAILY_TOPIC, FLINK_QUERY_3_WEEKLY_TOPIC};
	public static final String[] KAFKA_TOPICS = {KAFKA_QUERY_1_DAILY_TOPIC, KAFKA_QUERY_1_WEEKLY_TOPIC,
			KAFKA_QUERY_1_MONTHLY_TOPIC, KAFKA_QUERY_2_DAILY_TOPIC, KAFKA_QUERY_2_WEEKLY_TOPIC,
			KAFKA_QUERY_3_DAILY_TOPIC, KAFKA_QUERY_3_WEEKLY_TOPIC};

	// if consumer has no offset for the queue starts from the first record
	public static final String CONSUMER_FIRST_OFFSET = "earliest";

	public static final String KAFKA_BROKER_1 = "localhost:9092";
	public static final String KAFKA_BROKER_2 = "localhost:9093";
	public static final String KAFKA_BROKER_3 = "localhost:9094";

	public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_1 + "," +
			KAFKA_BROKER_2 + "," +
			KAFKA_BROKER_3;

	private static final String CONSUMER_ID = "single-flink-consumer";

	public static Properties getFlinkSourceProperties() {

		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_ID);

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		return props;
	}

	public static Properties getFlinkSinkProperties(String producerId) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
		return props;
	}
}
