package flink_dsp.query3;

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
 * Class that build the topology for the third query in flink
 */
@SuppressWarnings("Convert2Lambda")
public class Query3TopologyBuilder {

	/**
	 * Based on a source it constructs the correct transformation to the data stream for the third query topology
	 * @param source DataStream to be transformed
	 */
	public static void buildTopology(DataStream<Tuple2<Long, String>> source) {
		// parse the correct information needed in the third query, ignoring all the malformed lines
		DataStream<BusData> stream = source
				.flatMap(new FlatMapFunction<Tuple2<Long, String>, BusData>() {
					@Override
					public void flatMap(Tuple2<Long, String> tuple, Collector<BusData> collector) {
						BusData data;
						String[] info = tuple._2().split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
						try {
							data = new BusData(info[7], info[11], info[5], info[10]);
							collector.collect(data);
						} catch (ParseException | DelayFormatException | NumberFormatException ignored) {
						}
					}
				})
				.name("stream-query3-decoder");

		// 1 day statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(4)))
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new CompanyRankingAggregator(), new CompanyRankingProcessWindow())
				.name("query3-daily-ranking")
				// parse the CompanyRankingOutcome to a String
				.map(new Query3TopologyBuilder.ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_3_DAILY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_3_DAILY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_3_DAILY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query3-daily-ranking-sink");

		// 7 days statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
				// specify the aggregation function and the process window to correctly assign start date fo the window
				.aggregate(new CompanyRankingAggregator(), new CompanyRankingProcessWindow())
				.name("query3-weekly-ranking")
				// parse the CompanyRankingOutcome to a String
				.map(new Query3TopologyBuilder.ExtractStringMapper())
				// write the output string to the correct topic in kafka
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_3_WEEKLY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_3_WEEKLY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_3_WEEKLY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE))
				.name("query3-weekly-ranking-sink");
	}

	/**
	 * Class used to extract the result string form the CompanyRankingOutcome
	 */
	private static class ExtractStringMapper implements MapFunction<CompanyRankingOutcome, String> {
		@Override
		public String map(CompanyRankingOutcome companyRankingOutcome) {
			return OutputFormatter.query3OutcomeFormatter(companyRankingOutcome);
		}
	}
}
