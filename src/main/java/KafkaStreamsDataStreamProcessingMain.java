import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.KafkaStreamsConfig;
import kafkastreams_dsp.query1.Query1TopologyBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsDataStreamProcessingMain {

    public static void main(String[] args) {

        Properties props = KafkaStreamsConfig.createStreamProperties();
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Long, String> inputStream = builder.stream(KafkaClusterConfig.KAFKA_STREAMS_TOPIC);

        Query1TopologyBuilder.buildTopology(inputStream);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
