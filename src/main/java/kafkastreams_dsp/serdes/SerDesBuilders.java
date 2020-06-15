package kafkastreams_dsp.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import utility.BusData;
import utility.accumulators.AverageDelayAccumulator;
import utility.accumulators.ReasonRankingAccumulator;

import java.util.HashMap;
import java.util.Map;

public class SerDesBuilders {
    public static Serde<BusData> getBusDataSerdes() {

        Map<String, Object> serdeProps = new HashMap<>();

        Serializer<BusData> serializer = new JsonPOJOSerializer<>();
        Deserializer<BusData> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", BusData.class);
        serializer.configure(serdeProps, false);
        serdeProps.put("JsonPOJOClass", BusData.class);
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<AverageDelayAccumulator> getAverageDelayAccumulatorSerdes() {
        Map<String, Object> serdeProps = new HashMap<>();

        Serializer<AverageDelayAccumulator> serializer = new JsonPOJOSerializer<>();
        Deserializer<AverageDelayAccumulator> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", AverageDelayAccumulator.class);
        serializer.configure(serdeProps, false);
        serdeProps.put("JsonPOJOClass", AverageDelayAccumulator.class);
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<ReasonRankingAccumulator> getReasonRankingAccumulatorSerdes() {
        Map<String, Object> serdeProps = new HashMap<>();

        Serializer<ReasonRankingAccumulator> serializer = new JsonPOJOSerializer<>();
        Deserializer<ReasonRankingAccumulator> deserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", ReasonRankingAccumulator.class);
        serializer.configure(serdeProps, false);
        serdeProps.put("JsonPOJOClass", ReasonRankingAccumulator.class);
        deserializer.configure(serdeProps, false);

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
