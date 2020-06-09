import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import query1.Query1TopologyBuilder;
import utility.BusData;
import utility.StreamGenerator;

public class DataStreamProcessingMain {

    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamGenerator source = new StreamGenerator();
        DataStream<BusData> stream = environment
                .addSource(source)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<BusData>() {
                    @Override
                    public long extractAscendingTimestamp(BusData busData) {
                        // specify event time
                        return busData.getEventTime().getTime();
                    }
        }).name("stream-source");

        Query1TopologyBuilder.buildTopology(stream);

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
