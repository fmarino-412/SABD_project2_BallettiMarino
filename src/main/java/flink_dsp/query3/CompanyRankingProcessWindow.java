package flink_dsp.query3;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utility.DataCommonTransformation;

public class CompanyRankingProcessWindow extends ProcessAllWindowFunction<CompanyRankingOutcome,
		CompanyRankingOutcome, TimeWindow> {
	@Override
	public void process(Context context, Iterable<CompanyRankingOutcome> iterable,
						Collector<CompanyRankingOutcome> collector) {
		iterable.forEach(k -> {
			k.setStartDate(DataCommonTransformation.getCalendarAtTime(context.window().getStart()).getTime());
			collector.collect(k);
		});
	}
}