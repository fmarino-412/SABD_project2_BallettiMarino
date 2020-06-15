package kafkastreams_dsp;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import kafka_pubsub.KafkaClusterConfig;

import java.util.Properties;

public class KafkaStreamsConfig {

    public static Properties createStreamProperties() {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-queries");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "kafka-streams-queries-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName());

        return props;
    }
}