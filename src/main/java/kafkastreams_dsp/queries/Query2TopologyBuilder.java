package kafkastreams_dsp.queries;

import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.windows.DailyTimeWindows;
import kafkastreams_dsp.windows.WeeklyTimeWindows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.ReasonRankingAccumulator;
import utility.serdes.SerDesBuilders;

import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static utility.DataCommonTransformation.formatDate;

/**
 * Class that build the topology for the second query in kafka streams
 */
public class Query2TopologyBuilder {

	/**
	 * Based on a source it constructs the correct transformation to the data stream for the second query topology in
	 * kafka streams
	 * @param source DataStream to be transformed
	 */
	public static void buildTopology(KStream<Long, String> source) {
		// parse the correct information needed in the first query, ignoring all the malformed lines
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
				// used a custom daily window
				.windowedBy(new DailyTimeWindows(ZoneId.systemDefault(), Duration.ofHours(8L)))
				// set up function to aggregate daily data for reasons ranking
				.aggregate(new ReasonRankingInitializer(), new ReasonRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(ReasonRankingAccumulator.class)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				// parse the aggregate outcome to a string
				.map(new ReasonRankingMapper())
				// publish results to the correct kafka topic
				.to(KafkaClusterConfig.KAFKA_QUERY_2_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

		// 7 days statistics
		preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
					DataCommonTransformation.toWeeklyKeyed(busData)).groupByKey(Grouped.with(Serdes.String(),
					SerDesBuilders.getSerdes(BusData.class)))
				// used a custom weekly window
				.windowedBy(new WeeklyTimeWindows(ZoneId.systemDefault(), Duration.ofDays(5L)))
				// set up function to aggregate weekly data for reasons ranking
				.aggregate(new ReasonRankingInitializer(), new ReasonRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(ReasonRankingAccumulator.class)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				// parse the aggregate outcome to a string
				.map(new ReasonRankingMapper())
				// publish results to the correct kafka topic
				.to(KafkaClusterConfig.KAFKA_QUERY_2_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
	}

	/**
	 * Custom initializer that create a new ReasonRankingAccumulator
	 */
	private static class ReasonRankingInitializer implements Initializer<ReasonRankingAccumulator> {
		@Override
		public ReasonRankingAccumulator apply() {
			return new ReasonRankingAccumulator();
		}
	}

	/**
	 * Custom aggregator that calls the ReasonRankingAccumulator's add
	 */
	private static class ReasonRankingAggregator implements Aggregator<String, BusData, ReasonRankingAccumulator> {
		@Override
		public ReasonRankingAccumulator apply(String s, BusData busData, ReasonRankingAccumulator accumulator) {
			accumulator.add(busData.getEventTime(), busData.getReason());
			return accumulator;
		}
	}

	/**
	 * Mapper used to extract the result string form the ReasonRankingAccumulator
	 */
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

			// For benchmark purposes
			//SynchronizedCounter.incrementCounter();

			return new KeyValue<>(stringWindowed.key(), outcomeBuilder.toString());
		}

		/**
		 * Method to add list's elements to a string builder separated by a comma
		 * @param outcomeBuilder where to append elements
		 * @param list
		 */
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
