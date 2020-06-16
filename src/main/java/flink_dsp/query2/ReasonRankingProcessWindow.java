package flink_dsp.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utility.DataCommonTransformation;

public class ReasonRankingProcessWindow extends ProcessAllWindowFunction<ReasonRankingOutcome,
		ReasonRankingOutcome, TimeWindow> {
	@Override
	public void process(Context context, Iterable<ReasonRankingOutcome> iterable,
						Collector<ReasonRankingOutcome> collector) {
		iterable.forEach(k -> {
			k.setStartDate(DataCommonTransformation.getCalendarAtTime(context.window().getStart()).getTime());
			collector.collect(k);
		});
	}
}

