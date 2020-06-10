package query2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import query1.AverageDelayOutcome;
import utility.BusData;
import utility.CSVOutputFormatter;

import java.text.ParseException;

public class Query2TopologyBuilder {

    public static void buildTopology(DataStream<String> source) {

        DataStream<BusData> stream = source
                .flatMap(new FlatMapFunction<String, BusData>() {
                    @Override
                    public void flatMap(String s, Collector<BusData> collector) {
                        BusData data;
                        String[] info = s.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        try {
                            data = new BusData(info[7], info[5]);
                            collector.collect(data);
                        } catch (ParseException ignored) {
                            // ignored
                        }
                    }
                })
                .name("stream-query2-decoder")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<BusData>() {
                    @Override
                    public long extractAscendingTimestamp(BusData busData) {
                        // specify event time
                        return busData.getEventTime().getTime();
                    }
                });

        // 1 day statistics
        stream.timeWindowAll(Time.hours(24))
                .aggregate(new RankingAggregator())
                .name("query2-daily-ranking")
                .addSink(new SinkFunction<RankingOutcome>() {
                    public void invoke(RankingOutcome outcome, Context context) {
                        CSVOutputFormatter.writeOutputQuery2(CSVOutputFormatter.QUERY2_DAILY_CSV_FILE_PATH, outcome);
                    }
                });

        // 7 days statistics
        stream.timeWindowAll(Time.days(7))
                .aggregate(new RankingAggregator())
                .name("query2-weekly-ranking")
                .addSink(new SinkFunction<RankingOutcome>() {
                    public void invoke(RankingOutcome outcome, Context context) {
                        CSVOutputFormatter.writeOutputQuery2(CSVOutputFormatter.QUERY2_WEEKLY_CSV_FILE_PATH, outcome);
                    }
                });
    }
}
