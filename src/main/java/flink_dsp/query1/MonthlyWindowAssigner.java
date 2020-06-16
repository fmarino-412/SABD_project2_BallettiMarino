package flink_dsp.query1;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import utility.DataCommonTransformation;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;

public class MonthlyWindowAssigner extends TumblingEventTimeWindows {

	protected MonthlyWindowAssigner() {
		super(1, 0);
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		// due to override every result is now considered respect to NY time zone. No need to set any hour offset
		Calendar calendar = DataCommonTransformation.getCalendarAtTime(timestamp);

		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);

		calendar.set(Calendar.DAY_OF_MONTH, 1);
		// first day of current month at 00:00:00.000...
		long startDate = calendar.getTimeInMillis();

		calendar.add(Calendar.MONTH, 1);
		// last day of current month at 23:59:59.999...
		long endDate = calendar.getTimeInMillis() - 1;

		return Collections.singletonList(new TimeWindow(startDate, endDate));
	}
}
