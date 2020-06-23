package flink_dsp.query1;

import kafka_pubsub.KafkaClusterConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utility.BusData;
import utility.OutputFormatter;
import utility.delay_utility.DelayFormatException;
import utility.serdes.FlinkStringToKafkaSerializer;

import java.text.ParseException;

/**
 * Class that build the topology for the first query
 */
@SuppressWarnings("Convert2Lambda")
public class Query1TopologyBuilder {

	/**
	 * Based on a source it constructs the correct transformation to the data stream for the first query topology
	 * @param source DataStream to be transformed
	 */
	public static void buildTopology(DataStream<Tuple2<Long, String>> source) {
		// parse the correct information needed in the first query, ignoring all the malformed lines
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
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
				.name("query1-daily-mean")
				// parse the AverageDelayOutcome to a String
				.map(new ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_1_DAILY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_1_DAILY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_1_DAILY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query1-daily-mean-sink");

		// 7 days statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
				.name("query1-weekly-mean")
				// parse the AverageDelayOutcome to a String
				.map(new ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_1_WEEKLY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_1_WEEKLY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_1_WEEKLY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query1-weekly-mean-sink");

		// 1 month statistics
		stream.windowAll(new MonthlyWindowAssigner())
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
				.name("query1-monthly-mean")
				// parse the AverageDelayOutcome to a String
				.map(new ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_1_MONTHLY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_1_MONTHLY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_1_MONTHLY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query1-monthly-mean-sink");

	}

	/**
	 * Class used to extract the result string form the AverageDelayOutcome
	 */
	private static class ExtractStringMapper implements MapFunction<AverageDelayOutcome, String> {
		@Override
		public String map(AverageDelayOutcome averageDelayOutcome) {
			return OutputFormatter.query1OutcomeFormatter(averageDelayOutcome);
		}
	}
}