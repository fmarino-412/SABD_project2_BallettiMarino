import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import utility.BusData;
import utility.StreamGenerator;

public class DataStreamProcessingMain {

    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamGenerator source = new StreamGenerator();
        DataStream<BusData> stream = environment.addSource(source).name("stream-source");

        // Query 1 topology


        DataStream<String> debugStream = stream.map(new MapFunction<BusData, String>() {
            public String map(BusData busData) throws Exception {
                return "Date: " + busData.getEventTime() + " - value: " + busData.getDelay();
            }
        }).name("debug-map");
        debugStream.addSink(new SinkFunction<String>() {
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
            }
        }).name("debug-sink");

        try {
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
