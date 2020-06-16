package flink_dsp.query1;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import utility.BusData;
import utility.DataCommonTransformation;

import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class MonthlyWindowAssigner extends TumblingEventTimeWindows {

	protected MonthlyWindowAssigner() {
		super(1, 0);
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		BusData busData = (BusData) element;
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

		//System.out.println("Date from TS: " + new Date(timestamp) + " -- Date from BD: " + busData.getEventTime() + " -- Start: " + new Date(startDate) + " -- End: " + new Date(endDate));

		return Collections.singletonList(new TimeWindow(startDate, endDate));
	}
}
