package utility.serdes;

import kafka_pubsub.KafkaClusterConfig;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkStringToKafkaSerializer implements KafkaSerializationSchema<String> {

	private String topic;

	public FlinkStringToKafkaSerializer(String topic) {
		super();
		this.topic = topic;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(String value, @Nullable Long aLong) {
		return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
	}

	public static Properties getProperties(String producerId) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
		return props;
	}
}
