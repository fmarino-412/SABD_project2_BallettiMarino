import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.KafkaStreamsConfig;
import kafkastreams_dsp.queries.Query1TopologyBuilder;
import kafkastreams_dsp.queries.Query2TopologyBuilder;
import kafkastreams_dsp.queries.Query3TopologyBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Class used to start Kafka Streams data stream processing
 */
public class KafkaStreamsDSPMain {

	public static void main(String[] args) {

		// create kafka streams properties
		Properties props = KafkaStreamsConfig.createStreamProperties();
		StreamsBuilder builder = new StreamsBuilder();

		// define input
		KStream<Long, String> inputStream = builder.stream(KafkaClusterConfig.KAFKA_STREAMS_TOPIC);

		// build query 1 topology
		Query1TopologyBuilder.buildTopology(inputStream);

		// build query 2 topology
		Query2TopologyBuilder.buildTopology(inputStream);

		// build query 3 topology
		Query3TopologyBuilder.buildTopology(inputStream);

		// build, cleanup and start
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.cleanUp();
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
}
