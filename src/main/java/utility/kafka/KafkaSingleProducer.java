package utility.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static utility.kafka.KafkaConfig.*;

public class KafkaSingleProducer {

    private Producer<String, String> producer;

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

        final ProducerRecord<String, String> recordA = new ProducerRecord<>(FLINK_TOPIC,
                null,
                timestamp,
                key,
                value);
        final ProducerRecord<String, String> recordB = new ProducerRecord<>(KAFKA_STREAMS_TOPIC,
                null,
                timestamp,
                key,
                value);

        // TODO - may need .get()
        try {
            RecordMetadata metadataA = producer.send(recordA).get();
            RecordMetadata metadataB = producer.send(recordB).get();

            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                    recordA.key(), recordA.value(), metadataA.partition(), metadataA.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
