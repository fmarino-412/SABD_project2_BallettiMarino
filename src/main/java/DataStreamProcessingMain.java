import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import query1.Query1TopologyBuilder;
import query2.Query2TopologyBuilder;
import query3.Query3TopologyBuilder;
import utility.CSVOutputFormatter;
import utility.StreamGenerator;

public class DataStreamProcessingMain {

    public static void main(String[] args) {

        //cleaning result directory to store data results
        CSVOutputFormatter.cleanResultsFolder();

        //setup flink environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //add the source and handle watermarks
        StreamGenerator source = new StreamGenerator();
        DataStream<String> stream = environment
                .addSource(source)
                .name("stream-source");

        //build query 1 topology
        Query1TopologyBuilder.buildTopology(stream);

        //build query 2 topology
        Query2TopologyBuilder.buildTopology(stream);

        // build query 3 topology
        Query3TopologyBuilder.buildTopology(stream);

        try {
            //execute the environment for DSP
            environment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
