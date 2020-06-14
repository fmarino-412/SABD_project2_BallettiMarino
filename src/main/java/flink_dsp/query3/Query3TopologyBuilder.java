package flink_dsp.query3;

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
        stream.windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(4)))
                .aggregate(new CompanyRankingAggregator(), new CompanyRankingProcessWindow())
                .name("query3-daily-ranking")
                .addSink(new SinkFunction<CompanyRankingOutcome>() {
                    public void invoke(CompanyRankingOutcome outcome, Context context) {
                        OutputFormatter.writeOutputQuery3(OutputFormatter.QUERY3_DAILY_CSV_FILE_PATH, outcome);
                    }
                });

        // 7 days statistics
        stream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.hours(4)))
                .aggregate(new CompanyRankingAggregator(), new CompanyRankingProcessWindow())
                .name("query3-weekly-ranking")
                .addSink(new SinkFunction<CompanyRankingOutcome>() {
                    public void invoke(CompanyRankingOutcome outcome, Context context) {
                        OutputFormatter.writeOutputQuery3(OutputFormatter.QUERY3_WEEKLY_CSV_FILE_PATH, outcome);
                    }
                });
    }
}
