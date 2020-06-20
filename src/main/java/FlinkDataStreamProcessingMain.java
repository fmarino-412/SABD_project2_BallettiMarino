import flink_dsp.query1.Query1TopologyBuilder;
import flink_dsp.query2.Query2TopologyBuilder;
import flink_dsp.query3.Query3TopologyBuilder;
import kafka_pubsub.KafkaClusterConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

import static kafka_pubsub.KafkaClusterConfig.FLINK_TOPIC;

@SuppressWarnings("Convert2Lambda")
public class FlinkDataStreamProcessingMain {

	private static final String CONSUMER_GROUP_ID = "single-flink-consumer";

	public static void main(String[] args) {

		//setup flink environment
		Configuration conf = new Configuration();
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		//add the source and handle watermarks
		Properties props = KafkaClusterConfig.getFlinkSourceProperties(CONSUMER_GROUP_ID);

		DataStream<Tuple2<Long, String>> stream = environment
				.addSource(new FlinkKafkaConsumer<>(FLINK_TOPIC, new SimpleStringSchema(), props))
				.name("stream-source")
				.flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
					@Override
					public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
						String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
						DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
						try {
							collector.collect(new Tuple2<>(format.parse(info[7]).getTime(), s));
						} catch (ParseException ignored) {

						}
					}
				})
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
					@Override
					public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {
						// specify event time
						// kafka's auto-watermarks generation is only related to offset not to event time
						return tuple._1();
					}
				});

		//build query 1 topology
		Query1TopologyBuilder.buildTopology(stream);

		//build query 2 topology
		Query2TopologyBuilder.buildTopology(stream);

		// build query 3 topology
		Query3TopologyBuilder.buildTopology(stream);

		try {
			//execute the environment for DSP
			environment.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
