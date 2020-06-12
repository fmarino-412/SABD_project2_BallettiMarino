package query2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utility.BusData;
import utility.CSVOutputFormatter;

import java.text.ParseException;

public class Query2TopologyBuilder {

    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

        DataStream<BusData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, BusData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<BusData> collector) {
                        BusData data;
                        String[] info = tuple._2().split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        try {
                            data = new BusData(info[7], info[5]);
                            collector.collect(data);
                        } catch (ParseException | NumberFormatException ignored) {
                            // ignored
                        }
                    }
                })
                .name("stream-query2-decoder");

        // 1 day statistics
        stream.timeWindowAll(Time.hours(24))
                .aggregate(new ReasonRankingAggregator())
                .name("query2-daily-ranking")
                .addSink(new SinkFunction<ReasonRankingOutcome>() {
                    public void invoke(ReasonRankingOutcome outcome, Context context) {
                        CSVOutputFormatter.writeOutputQuery2(CSVOutputFormatter.QUERY2_DAILY_CSV_FILE_PATH, outcome);
                    }
                });

        // 7 days statistics
        stream.timeWindowAll(Time.days(7))
                .aggregate(new ReasonRankingAggregator())
                .name("query2-weekly-ranking")
                .addSink(new SinkFunction<ReasonRankingOutcome>() {
                    public void invoke(ReasonRankingOutcome outcome, Context context) {
                        CSVOutputFormatter.writeOutputQuery2(CSVOutputFormatter.QUERY2_WEEKLY_CSV_FILE_PATH, outcome);
                    }
                });
    }
}
