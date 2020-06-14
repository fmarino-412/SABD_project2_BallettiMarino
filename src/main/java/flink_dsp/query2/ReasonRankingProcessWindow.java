package flink_dsp.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

public class ReasonRankingProcessWindow extends ProcessAllWindowFunction<ReasonRankingOutcome,
        ReasonRankingOutcome, TimeWindow> {
    @Override
    public void process(Context context, Iterable<ReasonRankingOutcome> iterable,
                        Collector<ReasonRankingOutcome> collector) {
        iterable.forEach(k -> {
            Calendar calendar = Calendar.getInstance(Locale.US);
            calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            calendar.setTimeInMillis(context.window().getStart());
            k.setStartDate(calendar.getTime());
            collector.collect(k);
        });
    }
}

