package query3;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Locale;

public class CompanyRankingProcessWindow extends ProcessAllWindowFunction<CompanyRankingOutcome,
        CompanyRankingOutcome, TimeWindow> {
    @Override
    public void process(Context context, Iterable<CompanyRankingOutcome> iterable,
                        Collector<CompanyRankingOutcome> collector) {
        iterable.forEach(k -> {
            Calendar calendar = Calendar.getInstance(Locale.US);
            calendar.setTimeInMillis(context.window().getStart());
            k.setStartDate(calendar.getTime());
            collector.collect(k);
        });
    }
}