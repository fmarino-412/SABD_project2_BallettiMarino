package kafkastreams_dsp.queries;

import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.serdes.SerDesBuilders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.CompanyRankingAccumulator;
import utility.delay_parsing.DelayFormatException;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;

public class Query3TopologyBuilder {

	public static void buildTopology(KStream<Long, String> source) {

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
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				.aggregate(new CompanyRankingInitializer(), new CompanyRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(CompanyRankingAccumulator.class)))
				.toStream()
				.map(new CompanyRankingMapper())
				.to(KafkaClusterConfig.QUERY_3_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
		// 7 day statistics
		preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
					DataCommonTransformation.toWeeklyKeyed(busData))
				.groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
				.windowedBy(TimeWindows.of(Duration.ofDays(7)))
				.aggregate(new CompanyRankingInitializer(), new CompanyRankingAggregator(),
						Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(CompanyRankingAccumulator.class)))
				.toStream()
				.map(new CompanyRankingMapper())
				.to(KafkaClusterConfig.QUERY_3_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
	}

	private static class CompanyRankingInitializer implements Initializer<CompanyRankingAccumulator> {
		@Override
		public CompanyRankingAccumulator apply() {
			return new CompanyRankingAccumulator();
		}
	}

	private static class CompanyRankingAggregator implements Aggregator<String, BusData, CompanyRankingAccumulator> {
		@Override
		public CompanyRankingAccumulator apply(String s, BusData busData, CompanyRankingAccumulator companyRankingAccumulator) {
			companyRankingAccumulator.add(busData.getCompanyName(), busData.getReason(), busData.getDelay());
			return companyRankingAccumulator;
		}
	}

	private static class CompanyRankingMapper implements KeyValueMapper<Windowed<String>, CompanyRankingAccumulator,
			KeyValue<String, String>> {

		@Override
		public KeyValue<String, String> apply(Windowed<String> stringWindowed, CompanyRankingAccumulator companyRankingAccumulator) {
			StringBuilder outcomeBuilder = new StringBuilder();
			outcomeBuilder.append(stringWindowed.window().startTime().toEpochMilli()).append(";");

			companyRankingAccumulator.getCompanyRanking().forEach((k, v) ->
					outcomeBuilder.append(k).append(";").append(v).append(";"));

			outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);
			return new KeyValue<>(stringWindowed.key(), outcomeBuilder.toString());
		}
	}
}
