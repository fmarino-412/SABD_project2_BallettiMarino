package query3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import query1.AverageDelayOutcome;
import scala.Tuple2;
import utility.BusData;
import utility.CSVOutputFormatter;
import utility.delay_parsing.DelayFormatException;

import java.text.ParseException;

public class Query3TopologyBuilder {

    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

        DataStream<BusData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, BusData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<BusData> collector) {
                        BusData data;
                        String[] info = tuple._2().split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        try {
                            data = new BusData(info[7], info[11], info[5], info[10]);
                            collector.collect(data);
                        } catch (ParseException | DelayFormatException | NumberFormatException ignored) {
                            // ignored
                        }
                    }
                })
                .name("stream-query3-decoder");

        // 1 day statistics
        stream.timeWindowAll(Time.hours(24))
                .aggregate(new CompanyRankingAggregator())
                .name("query3-daily-ranking")
                .addSink(new SinkFunction<CompanyRankingOutcome>() {
                    public void invoke(CompanyRankingOutcome outcome, Context context) {
                        CSVOutputFormatter.writeOutputQuery3(CSVOutputFormatter.QUERY3_DAILY_CSV_FILE_PATH, outcome);
                    }
                });

        // 7 days statistics
        stream.timeWindowAll(Time.days(7))
                .aggregate(new CompanyRankingAggregator())
                .name("query3-weekly-ranking")
                .addSink(new SinkFunction<CompanyRankingOutcome>() {
                    public void invoke(CompanyRankingOutcome outcome, Context context) {
                        CSVOutputFormatter.writeOutputQuery3(CSVOutputFormatter.QUERY3_WEEKLY_CSV_FILE_PATH, outcome);
                    }
                });
    }
}
