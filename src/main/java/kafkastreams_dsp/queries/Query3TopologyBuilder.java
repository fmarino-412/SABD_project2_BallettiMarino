package kafkastreams_dsp.queries;

import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.windows.DailyTimeWindows;
import kafkastreams_dsp.windows.WeeklyTimeWindows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.CompanyRankingAccumulator;
import utility.delay.DelayFormatException;
import utility.serdes.SerDesBuilders;

import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;

import static utility.DataCommonTransformation.formatDate;

/**
 * Class that build the topology for the third query in kafka streams
 */
public class Query3TopologyBuilder {

	/**
	 * Based on a source it constructs the correct transformation to the data stream for the third query topology in
	 * kafka streams
	 * @param source DataStream to be transformed
	 */
	public static void buildTopology(KStream<Long, String> source) {
		// parse the correct information needed in the first query, ignoring all the malformed lines
		KStream<Long, BusData> preprocessed = source.flatMapValues(s -> {
			ArrayList<BusData> result = new ArrayList<>();
			String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			try {
				result.add(new BusData(info[7], info[11], info[5], info[10]));
			} catch (ParseException | DelayFormatException | NumberFormatException ignored) {

			}
			return result;
		});

		// 1 day statistics
		preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
					DataCommonTransformation.toDailyKeyed(busData))
				.groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
				// used a custom daily window
				.windowedBy(new DailyTimeWindows(ZoneId.systemDefault(), Duration.ofHours(8L)))
				// set up function to aggregate daily data for company ranking
				.aggregate(new CompanyRankingInitializer(), new CompanyRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(CompanyRankingAccumulator.class)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				// parse the aggregate outcome to a string
				.map(new CompanyRankingMapper())
				// publish results to the correct kafka topic
				.to(KafkaClusterConfig.KAFKA_QUERY_3_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

		// 7 day statistics
		preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
					DataCommonTransformation.toWeeklyKeyed(busData))
				.groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
				// used a custom weekly window
				.windowedBy(new WeeklyTimeWindows(ZoneId.systemDefault(), Duration.ofDays(5L)))
				// set up function to aggregate weekly data for company ranking
				.aggregate(new CompanyRankingInitializer(), new CompanyRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(CompanyRankingAccumulator.class)))
				.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
				.toStream()
				// parse the aggregate outcome to a string
				.map(new CompanyRankingMapper())
				// publish results to the correct kafka topic
				.to(KafkaClusterConfig.KAFKA_QUERY_3_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
	}

	/**
	 * Custom initializer that create a new CompanyRankingAccumulator
	 */
	private static class CompanyRankingInitializer implements Initializer<CompanyRankingAccumulator> {
		@Override
		public CompanyRankingAccumulator apply() {
			return new CompanyRankingAccumulator();
		}
	}

	/**
	 * Custom aggregator that calls the CompanyRankingAccumulator's add
	 */
	private static class CompanyRankingAggregator implements Aggregator<String, BusData, CompanyRankingAccumulator> {
		@Override
		public CompanyRankingAccumulator apply(String s, BusData busData,
											   CompanyRankingAccumulator companyRankingAccumulator) {
			companyRankingAccumulator.add(busData.getCompanyName(), busData.getReason(), busData.getDelay());
			return companyRankingAccumulator;
		}
	}

	/**
	 * Mapper used to extract the result string form the CompanyRankingAccumulator
	 */
	private static class CompanyRankingMapper implements KeyValueMapper<Windowed<String>, CompanyRankingAccumulator,
			KeyValue<String, String>> {
		@Override
		public KeyValue<String, String> apply(Windowed<String> stringWindowed,
											  CompanyRankingAccumulator companyRankingAccumulator) {

			String outcomeBuilder = formatDate(stringWindowed.window().startTime().toEpochMilli()) + ";" +
					DataCommonTransformation.buildCompanyRankingString(companyRankingAccumulator);

			// For benchmark purposes
			//SynchronizedCounter.incrementCounter();

			return new KeyValue<>(stringWindowed.key(), outcomeBuilder);
		}
	}
}
