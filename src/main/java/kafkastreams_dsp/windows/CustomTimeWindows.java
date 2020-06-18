package kafkastreams_dsp.windows;

import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public abstract class CustomTimeWindows extends Windows<TimeWindow> {

	protected final ZoneId zoneId;
	protected final long grace;


	protected CustomTimeWindows(final ZoneId zoneId, final Duration grace) {
		this.zoneId = zoneId;
		this.grace = grace.toMillis();
	}

	protected long toEpochMilli(final ZonedDateTime zonedDateTime) {
		return zonedDateTime.toInstant().toEpochMilli();
	}

	@Override
	public long gracePeriodMs() {
		return grace;
	}
}
