package kafkastreams_dsp.queries;

import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.windows.DailyTimeWindows;
import kafkastreams_dsp.windows.MonthlyTimeWindows;
import kafkastreams_dsp.windows.WeeklyTimeWindows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.AverageDelayAccumulator;
import utility.delay.DelayFormatException;
import utility.serdes.SerDesBuilders;

import java.text.ParseException;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;

import static utility.DataCommonTransformation.formatDate;

/**
 * Class that build the topology for the first query in kafka streams
 */
public class Query1TopologyBuilder {

    /**
     * Based on a source it constructs the correct transformation to the data stream for the first query topology in
     * kafka streams
     * @param source DataStream to be transformed
     */
    public static void buildTopology(KStream<Long, String> source) {
        // parse the correct information needed in the first query, ignoring all the malformed lines
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
                        DataCommonTransformation.toDailyKeyed(busData))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
                // used a custom daily window
                .windowedBy(new DailyTimeWindows(ZoneId.systemDefault(), Duration.ofHours(8L)))
                // set up function to aggregate daily data for average delay
                .aggregate(new AverageDelayInitializer(), new AverageDelayAggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AverageDelayAccumulator.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                // parse the aggregate outcome to a string
                .map(new AverageDelayMapper())
                // publish results to the correct kafka topic
                .to(KafkaClusterConfig.KAFKA_QUERY_1_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 7 days statistics
        preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
                        DataCommonTransformation.toWeeklyKeyed(busData))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
                // used a custom weekly window
                .windowedBy(new WeeklyTimeWindows(ZoneId.systemDefault(), Duration.ofDays(7L)))
                // set up function to aggregate weekly data for average delay
                .aggregate(new AverageDelayInitializer(), new AverageDelayAggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AverageDelayAccumulator.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                // parse the aggregate outcome to a string
                .map(new AverageDelayMapper())
                // publish results to the correct kafka topic
                .to(KafkaClusterConfig.KAFKA_QUERY_1_WEEKLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 1 month statistics
        preprocessed.map((KeyValueMapper<Long, BusData, KeyValue<String, BusData>>) (aLong, busData) ->
                        DataCommonTransformation.toMonthlyKeyed(busData))
                .groupByKey(Grouped.with(Serdes.String(), SerDesBuilders.getSerdes(BusData.class)))
                // used a custom monthly window
                .windowedBy(new MonthlyTimeWindows(ZoneId.systemDefault(), Duration.ofDays(20L)))
                // set up function to aggregate monthly data for average delay
                .aggregate(new AverageDelayInitializer(), new AverageDelayAggregator(),
                        Materialized.with(Serdes.String(), SerDesBuilders.getSerdes(AverageDelayAccumulator.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                // parse the aggregate outcome to a string
                .map(new AverageDelayMapper())
                // publish results to the correct kafka topic
                .to(KafkaClusterConfig.KAFKA_QUERY_1_MONTHLY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    /**
     * Custom initializer that create a new AverageDelayAccumulator
     */
    private static class AverageDelayInitializer implements Initializer<AverageDelayAccumulator> {
        @Override
        public AverageDelayAccumulator apply() {
            return new AverageDelayAccumulator();
        }
    }

    /**
     * Custom aggregator that calls the AverageDelayAccumulator's add
     */
    private static class AverageDelayAggregator implements Aggregator<String, BusData, AverageDelayAccumulator> {
        @Override
        public AverageDelayAccumulator apply(String s, BusData busData, AverageDelayAccumulator averageDelayAccumulator) {
            averageDelayAccumulator.add(busData.getBoro(), busData.getDelay(), 1L);
            return averageDelayAccumulator;
        }
    }

    /**
     * Mapper used to extract the result string form the AverageDelayAccumulator
     */
    private static class AverageDelayMapper implements KeyValueMapper<Windowed<String>, AverageDelayAccumulator,
            KeyValue<String, String>> {
        @Override
        public KeyValue<String, String> apply(Windowed<String> stringWindowed, AverageDelayAccumulator averageDelayAccumulator) {
            StringBuilder outcomeBuilder = new StringBuilder();
            outcomeBuilder.append(formatDate(stringWindowed.window().startTime().toEpochMilli())).append(";");

            averageDelayAccumulator.getBoroMap().forEach((k, v) -> {
                if (!String.valueOf(k).equals("")) {
                    outcomeBuilder.append(k)
                            .append(";")
                            .append(v.getTotal() / v.getCounter())
                            .append(";");
                }
            });

            outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);

            // For benchmark purposes
            // SynchronizedCounter.incrementCounter();

            return new KeyValue<>(stringWindowed.key(), outcomeBuilder.toString());
        }
    }
}
