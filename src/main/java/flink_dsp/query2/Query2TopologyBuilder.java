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

@SuppressWarnings("Convert2Lambda")
public class Query2TopologyBuilder {

	public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

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
				.aggregate(new ReasonRankingAggregator(), new ReasonRankingProcessWindow())
				.name("query2-daily-ranking")
				.map(new Query2TopologyBuilder.ExtractStringMapper())
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_2_DAILY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_2_DAILY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_2_DAILY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

		// 7 days statistics
		stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
				.aggregate(new ReasonRankingAggregator(), new ReasonRankingProcessWindow())
				.name("query2-weekly-ranking")
				.map(new Query2TopologyBuilder.ExtractStringMapper())
				.addSink(new FlinkKafkaProducer<>(KafkaClusterConfig.FLINK_QUERY_2_WEEKLY_TOPIC,
						new FlinkStringToKafkaSerializer(KafkaClusterConfig.FLINK_QUERY_2_WEEKLY_TOPIC),
						KafkaClusterConfig.getFlinkSinkProperties("producer" +
								KafkaClusterConfig.FLINK_QUERY_2_WEEKLY_TOPIC),
						FlinkKafkaProducer.Semantic.EXACTLY_ONCE));
	}

	private static class ExtractStringMapper implements MapFunction<ReasonRankingOutcome, String> {

		@Override
		public String map(ReasonRankingOutcome reasonRankingOutcome) {
			return OutputFormatter.query2OutcomeFormatter(reasonRankingOutcome);
		}
	}
}
