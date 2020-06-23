package kafka_pubsub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Class with all the topics' name and the getter for all the kind of properties
 */
public class KafkaClusterConfig {
	// topics
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
	private static final String CONSUMER_FIRST_OFFSET = "earliest";
	// for exactly once production
	private static final boolean ENABLE_PRODUCER_EXACTLY_ONCE = true;
	private static final String ENABLE_CONSUMER_EXACTLY_ONCE = "read_committed";

	// brokers
	public static final String KAFKA_BROKER_1 = "localhost:9092";
	public static final String KAFKA_BROKER_2 = "localhost:9093";
	public static final String KAFKA_BROKER_3 = "localhost:9094";

	// bootstrap servers
	public static final String BOOTSTRAP_SERVERS = KAFKA_BROKER_1 + "," + KAFKA_BROKER_2 + "," + KAFKA_BROKER_3;

	/**
	 * Creates properties for a Kafka Consumer representing the Flink stream source
	 * @param consumerGroupId id of consumer group
	 * @return created properties
	 */
	public static Properties getFlinkSourceProperties(String consumerGroupId) {
		Properties props = new Properties();

		// specify brokers
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		// set consumer group id
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		// start reading from beginning of partition if no offset was created
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_FIRST_OFFSET);
		// exactly once semantic
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

		// key and value deserializers
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		return props;
	}

	/**
	 * Creates properties for a Kafka Producer respresenting the one Flink processing sink
	 * @param producerId producer's id
	 * @return created properties
	 */
	public static Properties getFlinkSinkProperties(String producerId) {
		Properties props = new Properties();

		// specify brokers
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		// set producer id
		props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
		// exactly once semantic
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

		return props;
	}

	/**
	 * Creates properties for a Kafka Consumer representing one output subscriber
	 * @param consumerGroupId id of consumer group
	 * @return created properties
	 */
	public static Properties getKafkaParametricConsumerProperties(String consumerGroupId) {
		Properties props = new Properties();

		// specify brokers
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);
		// set consumer group id
		props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		// start reading from beginning of partition if no offset was created
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaClusterConfig.CONSUMER_FIRST_OFFSET);
		// exactly once semantic
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ENABLE_CONSUMER_EXACTLY_ONCE);

		// key and value deserializers
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		return props;
	}

	/**
	 * Creates properties for a Kafka Producer representing the entire stream processing source
	 * @param producerId producer's id
	 * @return created properties
	 */
	public static Properties getKafkaSingleProducerProperties(String producerId) {
		Properties props = new Properties();

		// specify brokers
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		// set producer id
		props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
		// exactly once semantic
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_PRODUCER_EXACTLY_ONCE);

		// key and value serializers
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return props;
	}
}
