package kafkastreams_dsp.query1;

import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.serdes.SerDesBuilders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.KeyEvaluator;
import utility.accumulators.AverageDelayAccumulator;
import utility.delay_parsing.DelayFormatException;

import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;

public class Query1TopologyBuilder {

    public static void buildTopology(KStream<Long, String> source) {

        KStream<Long, BusData> preprocessed = source.flatMapValues(s -> {
            ArrayList<BusData> result = new ArrayList<>();
            String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            try {
                result.add(new BusData(info[7], info[11], info[9]));
            } catch (ParseException | DelayFormatException | NumberFormatException ignored) {

            }
            return result;
        });

        // 1 day statistics
        preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
                        KeyEvaluator.toDailyKeyed(busData))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .aggregate(new AverageDelayInitializer(), new AverageDelayAggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AverageDelayAccumulator.class)))
                .toStream()
                .map(new AverageDelayMapper())
                .to(KafkaClusterConfig.QUERY_1_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 7 days statistics
        preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
                        KeyEvaluator.toWeeklyKeyed(busData))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
                .windowedBy(TimeWindows.of(Duration.ofDays(7)))
                .aggregate(new AverageDelayInitializer(), new AverageDelayAggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AverageDelayAccumulator.class)))
                .toStream()
                .map(new AverageDelayMapper())
                .to(KafkaClusterConfig.QUERY_1_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 1 month statistics
        preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
                        KeyEvaluator.toMonthlyKeyed(busData))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
                .windowedBy(TimeWindows.of(Duration.ofDays(31)))
                .aggregate(new AverageDelayInitializer(), new AverageDelayAggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AverageDelayAccumulator.class)))
                .toStream()
                .map(new AverageDelayMapper())
                .to(KafkaClusterConfig.QUERY_1_MONTHLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static class AverageDelayInitializer implements Initializer<AverageDelayAccumulator> {
        @Override
        public AverageDelayAccumulator apply() {
            return new AverageDelayAccumulator();
        }
    }

    private static class AverageDelayAggregator implements Aggregator<String, BusData, AverageDelayAccumulator> {
        @Override
        public AverageDelayAccumulator apply(String s, BusData busData, AverageDelayAccumulator averageDelayAccumulator) {
            averageDelayAccumulator.add(busData.getBoro(), busData.getDelay(), 1L);
            return averageDelayAccumulator;
        }
    }

    private static class AverageDelayMapper implements KeyValueMapper<Windowed<String>, AverageDelayAccumulator,
            KeyValue<String, String>> {
        @Override
        public KeyValue<String, String> apply(Windowed<String> stringWindowed, AverageDelayAccumulator averageDelayAccumulator) {
            StringBuilder outcomeBuilder = new StringBuilder();
            outcomeBuilder.append(stringWindowed.window().startTime().toEpochMilli()).append(";");

            averageDelayAccumulator.getBoroMap().forEach((k, v) ->
                    outcomeBuilder.append(k).append(";").append(v.getTotal()/v.getCounter()).append(";"));

            outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);
            return new KeyValue<>(stringWindowed.key(), outcomeBuilder.toString());
        }
    }
}
