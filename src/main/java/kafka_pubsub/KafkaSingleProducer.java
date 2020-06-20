package kafka_pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaSingleProducer {

	private static final String PRODUCER_ID = "single-producer";
	private final Producer<Long, String> producer;

	public KafkaSingleProducer() {
		producer = createProducer();
	}

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<>(props);
	}

	public void produce(Long key, String value, Long timestamp) {

		// no need to put timestamp for Flink
		final ProducerRecord<Long, String> recordA = new ProducerRecord<>(KafkaClusterConfig.FLINK_TOPIC, null,
				value);
		final ProducerRecord<Long, String> recordB = new ProducerRecord<>(KafkaClusterConfig.KAFKA_STREAMS_TOPIC,
				null,
				timestamp,
				key,
				value);

		producer.send(recordA);
		producer.send(recordB);
	}

	public void close() {
		producer.flush();
		producer.close();
	}
}
