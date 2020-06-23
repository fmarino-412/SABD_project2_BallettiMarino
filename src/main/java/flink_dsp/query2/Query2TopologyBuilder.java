package flink_dsp.query2;

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
import utility.serdes.FlinkStringToKafkaSerializer;

import java.text.ParseException;

/**
 * Class that build the topology for the second query in flink
 */
public class Query2TopologyBuilder {

	/**
	 * Based on a source it constructs the correct transformation to the data stream for the second query topology
	 * @param source DataStream to be transformed
	 */
	public static void buildTopology(DataStream<Tuple2<Long, String>> source) {
		// parse the correct information needed in the second query, ignoring all the malformed lines
		DataStream<BusData> stream = source
				.flatMap(new FlatMapFunction<Tuple2<Long, String>, BusData>() {
					@Override
					public void flatMap(Tuple2<Long, String> tuple, Collector<BusData> collector) {
						BusData data;
						String[] info = tuple._2().split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
						try {
							data = new BusData(info[7], info[5]);
							collector.collect(data);
						} catch (ParseException | NumberFormatException ignored) {
							// ignored
						}
					}
				})
				.name("stream-query2-decoder");

		// 1 day statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(4)))
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new ReasonRankingAggregator(), new ReasonRankingProcessWindow())
				.name("query2-daily-ranking")
				// parse the ReasonRankingOutcome to a String
				.map(new Query2TopologyBuilder.ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_2_DAILY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_2_DAILY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_2_DAILY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query2-daily-ranking-sink");

		// 7 days statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new ReasonRankingAggregator(), new ReasonRankingProcessWindow())
				.name("query2-weekly-ranking")
				// parse the ReasonRankingOutcome to a String
				.map(new Query2TopologyBuilder.ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_2_WEEKLY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_2_WEEKLY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_2_WEEKLY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query2-weekly-ranking-sink");
	}

	/**
	 * Class used to extract the result string form the ReasonRankingOutcome
	 */
	private static class ExtractStringMapper implements MapFunction<ReasonRankingOutcome, String> {
		@Override
		public String map(ReasonRankingOutcome reasonRankingOutcome) {
			return OutputFormatter.query2OutcomeFormatter(reasonRankingOutcome);
		}
	}
}
