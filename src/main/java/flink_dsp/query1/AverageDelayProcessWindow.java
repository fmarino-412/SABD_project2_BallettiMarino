package flink_dsp.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utility.DataCommonTransformation;

/**
 * Used to add the correct window start date to the AverageDelayOutcome
 */
public class AverageDelayProcessWindow extends ProcessAllWindowFunction<AverageDelayOutcome, AverageDelayOutcome,
		TimeWindow> {
	@Override
	public void process(Context context, Iterable<AverageDelayOutcome> iterable,
						Collector<AverageDelayOutcome> collector) {
		iterable.forEach(k -> {
			k.setStartDate(DataCommonTransformation.getCalendarAtTime(context.window().getStart()).getTime());
			collector.collect(k);
		});
	}
}
