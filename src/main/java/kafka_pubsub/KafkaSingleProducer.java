package kafka_pubsub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSingleProducer {

	private static final String PRODUCER_ID = "single-producer";
	private final Producer<Long, String> producer;

	public KafkaSingleProducer() {
		producer = createProducer();
	}

	private static Producer<Long, String> createProducer() {
		Properties props = KafkaClusterConfig.getKafkaSingleProducerProperties(PRODUCER_ID);
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
