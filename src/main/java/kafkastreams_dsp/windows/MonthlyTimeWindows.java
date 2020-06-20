package kafkastreams_dsp.windows;

import org.apache.kafka.streams.kstream.internals.TimeWindow;
import utility.DataCommonTransformation;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

//Implementation of a monthly custom window with a given timezone
public class MonthlyTimeWindows extends CustomTimeWindows {

	// maximum size is of 31 days, it's shorter for February, June, September, November
	private final static long SIZE_IN_MILLIS = Duration.ofDays(31L).toMillis();

	@SuppressWarnings("deprecation")
	public MonthlyTimeWindows(final ZoneId zoneId, final Duration grace) {
		super(zoneId, grace);

		// use of a deprecated method is the only possible solution to define a retention time different from 1 day
		this.until(SIZE_IN_MILLIS + grace.toMillis());
	}

	@Override
	public Map<Long, TimeWindow> windowsFor(final long timestamp) {
		final Instant instant = Instant.ofEpochMilli(timestamp);
		final ZonedDateTime zonedDateTime = instant.atZone(zoneId);

		final Calendar calendar = DataCommonTransformation.getCalendarAtTime(toEpochMilli(zonedDateTime));
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		final long startTime = calendar.getTimeInMillis();

		calendar.add(Calendar.MONTH, 1);
		final long endTime = calendar.getTimeInMillis() - 1;

		final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
		windows.put(startTime, new TimeWindow(startTime, endTime));
		return windows;
	}

	@Override
	public long size() {
		return SIZE_IN_MILLIS;
	}
}
