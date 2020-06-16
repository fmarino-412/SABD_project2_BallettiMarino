package flink_dsp.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utility.DataCommonTransformation;

public class AverageDelayProcessKeyedWindow extends ProcessWindowFunction<AverageDelayOutcome, AverageDelayOutcome,
		String, TimeWindow> {
	@Override
	public void process(String s, Context context, Iterable<AverageDelayOutcome> iterable, Collector<AverageDelayOutcome> collector) throws Exception {
		iterable.forEach(k -> {
			k.setStartDate(DataCommonTransformation.getCalendarAtTime(context.window().getStart()).getTime());
			collector.collect(k);
		});
	}
}
