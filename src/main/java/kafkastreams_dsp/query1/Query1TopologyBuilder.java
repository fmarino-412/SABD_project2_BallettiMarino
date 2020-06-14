package kafkastreams_dsp.query1;

import kafka_pubsub.KafkaClusterConfig;
import kafkastreams_dsp.serdes.SerDesBuilders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import utility.accumulators.AverageDelayAccumulator;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utility.BusData;
import utility.delay_parsing.DelayFormatException;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class Query1TopologyBuilder {

    public static void buildTopology(KStream<Long, String> source) {

        KStream<Long, BusData> preprocessed = source.flatMapValues(new ValueMapper<String, Iterable<? extends BusData>>() {
            @Override
            public Iterable<? extends BusData> apply(String s) {
                ArrayList<BusData> result = new ArrayList<>();
                String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                try {
                    result.add(new BusData(info[7], info[11], info[9]));
                } catch (ParseException | DelayFormatException | NumberFormatException ignored) {

                }
                return result;
            }
        });

        // 1 day statistics
        KStream<String, String> stream = preprocessed.map(new KeyValueMapper<Long, BusData, KeyValue<String, BusData>>() {
            @Override
            public KeyValue<String, BusData> apply(Long aLong, BusData busData) {
                StringBuilder newKey = new StringBuilder();

                Calendar calendar = Calendar.getInstance(Locale.US);
                calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                calendar.setTime(busData.getEventTime());

                newKey.append(calendar.get(Calendar.DAY_OF_MONTH))
                        .append("/")
                        .append(calendar.get(Calendar.MONTH))
                        .append("/")
                        .append(calendar.get(Calendar.YEAR));

                return new KeyValue<>(newKey.toString(), busData);
            }
        }).groupByKey(Serialized.with(Serdes.String(), SerDesBuilders.getBusDataSerdes()))
                .windowedBy(TimeWindows.of(Duration.ofDays(1)))
                .aggregate(new Initializer<AverageDelayAccumulator>() {
                    @Override
                    public AverageDelayAccumulator apply() {
                        return new AverageDelayAccumulator();
                    }
                }, new Aggregator<String, BusData, AverageDelayAccumulator>() {
                    @Override
                    public AverageDelayAccumulator apply(String s, BusData busData, AverageDelayAccumulator averageDelayAccumulator) {
                        averageDelayAccumulator.add(busData.getBoro(), busData.getDelay(), 1L);
                        return averageDelayAccumulator;
                    }
                }, Materialized.with(Serdes.String(), SerDesBuilders.getAverageDelayAccumulatorSerdes()))
                .toStream().map(new KeyValueMapper<Windowed<String>, AverageDelayAccumulator, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(Windowed<String> stringWindowed, AverageDelayAccumulator averageDelayAccumulator) {
                        StringBuilder outcomeBuilder = new StringBuilder();
                        outcomeBuilder.append(stringWindowed.window().startTime().toEpochMilli());
                        averageDelayAccumulator.getBoroMap().forEach((k, v) -> {
                            outcomeBuilder.append(k).append(",").append(v.getTotal()/v.getCounter()).append(",");
                        });
                        outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);
                        return new KeyValue<>(stringWindowed.key(), outcomeBuilder.toString());
                    }
                });
        stream.to(KafkaClusterConfig.QUERY_1_DAILY_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // 7 days statistics
        // 1 month statistics
    }
}
