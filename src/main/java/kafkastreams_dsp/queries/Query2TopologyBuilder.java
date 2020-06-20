package kafkastreams_dsp.queries;

import kafka_pubsub.KafkaClusterConfig;
import utility.serdes.SerDesBuilders;
import kafkastreams_dsp.windows.DailyTimeWindows;
import kafkastreams_dsp.windows.WeeklyTimeWindows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.ReasonRankingAccumulator;

import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static utility.DataCommonTransformation.formatDate;

public class Query2TopologyBuilder {

	public static void buildTopology(KStream<Long, String> source) {

		KStream<Long, BusData> preprocessed = source.flatMapValues(s -> {
			ArrayList<BusData> result = new ArrayList<>();
			String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			try {
				result.add(new BusData(info[7], info[5]));
			} catch (ParseException | NumberFormatException ignored) {

			}
			return result;
		});

		// 1 day statistics
		preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
					DataCommonTransformation.toDailyKeyed(busData)).groupByKey(Grouped.with(Serdes.String(),
					SerDesBuilders.getSerdes(BusData.class)))
				.windowedBy(new DailyTimeWindows(ZoneId.systemDefault(), Duration.ofHours(8L)))
				.aggregate(new ReasonRankingInitializer(), new ReasonRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(ReasonRankingAccumulator.class)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				.map(new ReasonRankingMapper())
				.to(KafkaClusterConfig.KAFKA_QUERY_2_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

		// 7 days statistics
		preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
					DataCommonTransformation.toWeeklyKeyed(busData)).groupByKey(Grouped.with(Serdes.String(),
					SerDesBuilders.getSerdes(BusData.class)))
				.windowedBy(new WeeklyTimeWindows(ZoneId.systemDefault(), Duration.ofDays(5L)))
				.aggregate(new ReasonRankingInitializer(), new ReasonRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(ReasonRankingAccumulator.class)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				.map(new ReasonRankingMapper())
				.to(KafkaClusterConfig.KAFKA_QUERY_2_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
	}

	private static class ReasonRankingInitializer implements Initializer<ReasonRankingAccumulator> {
		@Override
		public ReasonRankingAccumulator apply() {
			return new ReasonRankingAccumulator();
		}
	}

	private static class ReasonRankingAggregator implements Aggregator<String, BusData, ReasonRankingAccumulator> {
		@Override
		public ReasonRankingAccumulator apply(String s, BusData busData, ReasonRankingAccumulator accumulator) {
			accumulator.add(busData.getEventTime(), busData.getReason(), 1L);
			return accumulator;
		}
	}

	private static class ReasonRankingMapper implements KeyValueMapper<Windowed<String>, ReasonRankingAccumulator,
			KeyValue<String, String>> {
		private static final String AM = "05:00-11:59";
		private static final String PM = "12:00-19:00";
		private static final int RANK_SIZE = 3;

		@Override
		public KeyValue<String, String> apply(Windowed<String> stringWindowed, ReasonRankingAccumulator accumulator) {
			StringBuilder outcomeBuilder = new StringBuilder();

			// Create the lists from elements of HashMap and sort them
			List<Map.Entry<String, Long>> amList = new LinkedList<>(accumulator.getAmRanking().entrySet());
			amList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
			List<Map.Entry<String, Long>> pmList = new LinkedList<>(accumulator.getPmRanking().entrySet());
			pmList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

			outcomeBuilder.append(formatDate(stringWindowed.window().startTime().toEpochMilli()))
					.append(";")
					.append(AM)
					.append(";[");

			addElements(outcomeBuilder, amList);

			outcomeBuilder.append("];")
					.append(PM)
					.append(";[");

			addElements(outcomeBuilder, pmList);

			outcomeBuilder.append("]");

			return new KeyValue<>(stringWindowed.key(), outcomeBuilder.toString());
		}

		private void addElements(StringBuilder outcomeBuilder, List<Map.Entry<String, Long>> list) {
			String elem;
			boolean added = false;

			for (int i = 0; i < RANK_SIZE; i++) {
				try {
					elem = list.get(i).getKey();
					outcomeBuilder.append(elem).append(",");
					added = true;
				} catch (IndexOutOfBoundsException ignored) {
					// Less than RANK_SIZE elements
				}
			}

			if (added) {
				outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);
			}
		}
	}
}
