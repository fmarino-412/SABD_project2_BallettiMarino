package query1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import utility.BusData;
import utility.OutputFormatter;
import utility.delay_parsing.DelayFormatException;

import java.text.ParseException;

@SuppressWarnings("Convert2Lambda")
public class Query1TopologyBuilder {

    public static void buildTopology(DataStream<Tuple2<Long, String>> source) {

        DataStream<BusData> stream = source
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, BusData>() {
                    @Override
                    public void flatMap(Tuple2<Long, String> tuple, Collector<BusData> collector) {
                        BusData data;
                        String[] info = tuple._2().split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                        try {
                            data = new BusData(info[7], info[11], info[9]);
                            collector.collect(data);
                        } catch (ParseException | DelayFormatException | NumberFormatException ignored) {
                            // ignored
                        }
                    }
                })
                .name("stream-query1-decoder");

        // 1 day statistics
        stream.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(4)))
                .aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
                .name("query1-daily-mean")
                .addSink(new SinkFunction<AverageDelayOutcome>() {
                    public void invoke(AverageDelayOutcome outcome, Context context) {
                        OutputFormatter.writeOutputQuery1(OutputFormatter.QUERY1_DAILY_CSV_FILE_PATH, outcome);
                    }
                });

        // 7 days statistics
        stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
                .aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
                .name("query1-weekly-mean")
                .addSink(new SinkFunction<AverageDelayOutcome>() {
                    public void invoke(AverageDelayOutcome outcome, Context context) {
                        OutputFormatter.writeOutputQuery1(OutputFormatter.QUERY1_WEEKLY_CSV_FILE_PATH, outcome);
                    }
                });

        // 1 month statistics
        stream.windowAll(TumblingEventTimeWindows.of(Time.days(30), Time.hours(4)))
                .aggregate(new AverageDelayAggregator(), new AverageDelayProcessWindow())
                .name("query1-monthly-mean")
                .addSink(new SinkFunction<AverageDelayOutcome>() {
                    public void invoke(AverageDelayOutcome outcome, Context context) {
                        OutputFormatter.writeOutputQuery1(OutputFormatter.QUERY1_MONTHLY_CSV_FILE_PATH, outcome);
                    }
                });
    }
}