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

/**
 * Implementation of a monthly custom window with a given timezone
 */
public class MonthlyTimeWindows extends CustomTimeWindows {

	// maximum size is of 31 days, it's shorter for February, June, September, November
	private final static long SIZE_IN_MILLIS = Duration.ofDays(31L).toMillis();

	/**
	 * Default constructor
	 * @param zoneId to be setted
	 * @param grace Duration representing the grace period
	 */
	@SuppressWarnings("deprecation")
	public MonthlyTimeWindows(final ZoneId zoneId, final Duration grace) {
		super(zoneId, grace);

		// set up retention time
		this.until(SIZE_IN_MILLIS + grace.toMillis());
	}

	/**
	 * Function that assign an event to the correct monthly window based on the timestamp
	 * @param timestamp of the event
	 * @return the map of the windows
	 */
	@Override
	public Map<Long, TimeWindow> windowsFor(final long timestamp) {
		final Instant instant = Instant.ofEpochMilli(timestamp);
		final ZonedDateTime zonedDateTime = instant.atZone(zoneId);

		// get the first instant of the current month
		final Calendar calendar = DataCommonTransformation.getCalendarAtTime(toEpochMilli(zonedDateTime));
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		final long startTime = calendar.getTimeInMillis();

		// get the last instant of the current month
		calendar.add(Calendar.MONTH, 1);
		final long endTime = calendar.getTimeInMillis() - 1;

		final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
		windows.put(startTime, new TimeWindow(startTime, endTime));
		return windows;
	}

	/**
	 * Getter for the size
	 * @return the size of the window
	 */
	@Override
	public long size() {
		return SIZE_IN_MILLIS;
	}
}
