import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import query1.Query1TopologyBuilder;
import query2.Query2TopologyBuilder;
import query3.Query3TopologyBuilder;
import scala.Tuple2;
import utility.OutputFormatter;
import utility.StreamGenerator;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@SuppressWarnings("Convert2Lambda")
public class DataStreamProcessingMain {

    public static void main(String[] args) {

        //cleaning result directory to store data results
        OutputFormatter.cleanResultsFolder();

        //setup flink environment
        Configuration conf = new Configuration();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //add the source and handle watermarks
        StreamGenerator source = new StreamGenerator();
        DataStream<Tuple2<Long, String>> stream = environment
                .addSource(source)
                .name("stream-source")
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
                        String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                        try {
                            collector.collect(new Tuple2<>(format.parse(info[7]).getTime(), s));
                        } catch (ParseException ignored) {

                        }
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, String> tuple) {
                        // specify event time
                        return tuple._1();
                    }
                });

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
