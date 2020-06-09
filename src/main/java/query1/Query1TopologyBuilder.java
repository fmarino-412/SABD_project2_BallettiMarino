package query1;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;
import utility.BusData;

public class Query1TopologyBuilder {

    public static void buildTopology(DataStream<BusData> stream) {

        // 1 day statistics
        stream.timeWindowAll(Time.hours(24)).aggregate(new AverageDelayAggregator()).name("query1-daily-mean")
                .addSink(new SinkFunction<Tuple2<String, Double>>() {
                    public void invoke(Tuple2<String, Double> value, Context context) {
                        System.out.println("Daily, " + value._1() + ": " + value._2());
                    }
                });

        // 7 days statistics
        stream.timeWindowAll(Time.days(7)).aggregate(new AverageDelayAggregator()).name("query1-weekly-mean")
                .addSink(new SinkFunction<Tuple2<String, Double>>() {
                    public void invoke(Tuple2<String, Double> value, Context context) {
                        System.out.println("Weekly, " + value._1() + ": " + value._2());
                    }
                });

        // 1 month statistics
        stream.timeWindowAll(Time.days(30)).aggregate(new AverageDelayAggregator()).name("query1-monthly-mean")
                .addSink(new SinkFunction<Tuple2<String, Double>>() {
                    public void invoke(Tuple2<String, Double> value, Context context) {
                        System.out.println("Monthly, " + value._1() + ": " + value._2());
                    }
                });
    }
}