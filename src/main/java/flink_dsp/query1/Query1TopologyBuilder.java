package flink_dsp.query1;

import kafka_pubsub.KafkaClusterConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Tuple2;
import utility.BusData;
import utility.OutputFormatter;
import utility.delay_utility.DelayFormatException;
import utility.serdes.FlinkStringToKafkaSerializer;

import java.text.ParseException;
import java.util.Properties;

@SuppressWarnings("Convert2Lambda")
public class Query1TopologyBuilder {

	public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

		DataStream<BusData> stream = source
				.flatMap(new FlatMapFunction<Tuple2<Long, String>, BusData>() {
					@Override
					public void flatMap(Tuple2<Long, String> tuple, Collector<BusData> collector) {
						BusData data;
						String[] info = tuple._2().split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
						try {
							data = new BusData(info[7], info[11], info[9]);
							collector.collect(data);
						} catch (ParseException | DelayFormatException | NumberFormatException ignored) {
							// ignored
						}
					}
				})
				.name("stream-query1-decoder");

		// 1 day statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(4)))
				.aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
				.name("query1-daily-mean")
				.map(new ExtractStringMapper())
				.addSink(new FlinkKafkaProducer<String>(KafkaClusterConfig.FLINK_QUERY_1_DAILY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_1_DAILY_TOPIC),
						getProperties("producer" + KafkaClusterConfig.FLINK_QUERY_1_DAILY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

		// 7 days statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
				.aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
				.name("query1-weekly-mean")
				.addSink(new SinkFunction<AverageDelayOutcome>() {
					public void invoke(AverageDelayOutcome outcome, Context context) {
						OutputFormatter.writeOutputQuery1(OutputFormatter.QUERY1_WEEKLY_CSV_FILE_PATH, outcome);
					}
				});

		// 1 month statistics
		stream.windowAll(new MonthlyWindowAssigner())
				.aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
				.name("query1-monthly-mean")
				.addSink(new SinkFunction<AverageDelayOutcome>() {
					public void invoke(AverageDelayOutcome outcome, Context context) {
						OutputFormatter.writeOutputQuery1(OutputFormatter.QUERY1_MONTHLY_CSV_FILE_PATH, outcome);
					}
				});

	}

	private static Properties getProperties(String producerId) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaClusterConfig.BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, producerId);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return props;
	}

	private static class ExtractStringMapper implements MapFunction<AverageDelayOutcome, String> {

		@Override
		public String map(AverageDelayOutcome averageDelayOutcome) {
			return OutputFormatter.query1OutcomeFormatter(averageDelayOutcome);
		}
	}
}