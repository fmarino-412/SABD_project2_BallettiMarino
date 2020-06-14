package utility.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static utility.kafka.KafkaConfig.*;

public class KafkaSingleProducer {

    private final Producer<String, String> producer;

    public KafkaSingleProducer() {
        producer = createProducer();
    }

    private static Producer<String, String> createProducer() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public void produce(String key, String value, Long timestamp) {

        // no need to put timestamp for Flink
        final ProducerRecord<String, String> recordA = new ProducerRecord<>(FLINK_TOPIC, key, value);
        final ProducerRecord<String, String> recordB = new ProducerRecord<>(KAFKA_STREAMS_TOPIC,
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
