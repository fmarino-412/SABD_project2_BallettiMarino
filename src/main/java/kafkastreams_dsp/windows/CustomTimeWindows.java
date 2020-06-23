package kafkastreams_dsp.windows;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Abstract class representing a time window of custom duration
 */
public abstract class CustomTimeWindows extends Windows<TimeWindow> {

	// local zone id
	protected final ZoneId zoneId;
	// grace period milliseconds
	protected final long grace;

	/**
	 * Default constructor
	 * @param zoneId local zone id
	 * @param grace grace period milliseconds
	 */
	protected CustomTimeWindows(final ZoneId zoneId, final Duration grace) {
		this.zoneId = zoneId;
		this.grace = grace.toMillis();
	}

	/**
	 * Converts the zoned time to milliseconds
	 * @param zonedDateTime time with an associated time zone
	 * @return timestamp in milliseconds
	 */
	protected long toEpochMilli(final ZonedDateTime zonedDateTime) {
		return zonedDateTime.toInstant().toEpochMilli();
	}

	@Override
	public long gracePeriodMs() {
		return grace;
	}
}
