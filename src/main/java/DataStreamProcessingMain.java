import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import query1.Query1TopologyBuilder;
import query2.Query2TopologyBuilder;
import query3.Query3TopologyBuilder;
import scala.Tuple2;
import utility.OutputFormatter;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

import static utility.kafka.KafkaConfig.*;

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
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        DataStream<Tuple2<Long, String>> stream = environment
                .addSource(new FlinkKafkaConsumer<>(FLINK_TOPIC, new SimpleStringSchema(), props))
                .name("stream-source")
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Long, String>> collector) {
                        String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
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
                        // kafka's auto-watermarks generation is only related to offset not to event time
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
