package kafkastreams_dsp.windows;

import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;

//Implementation of a daily custom window with a given timezone
public class DailyTimeWindows extends CustomTimeWindows {

    private final int startHour;

    public DailyTimeWindows(final ZoneId zoneId, final Duration grace) {
        super(zoneId, grace);
        this.startHour = 0;
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        final Instant instant = Instant.ofEpochMilli(timestamp);

        final ZonedDateTime zonedDateTime = instant.atZone(zoneId);
        final ZonedDateTime startTime = zonedDateTime.getHour() >= startHour ?
                zonedDateTime.truncatedTo(ChronoUnit.DAYS).withHour(startHour) :
                zonedDateTime.truncatedTo(ChronoUnit.DAYS).minusDays(1).withHour(startHour);
        final ZonedDateTime endTime = startTime.plusDays(1);

        final Map<Long, TimeWindow> windows = new LinkedHashMap<>();
        windows.put(toEpochMilli(startTime), new TimeWindow(toEpochMilli(startTime), toEpochMilli(endTime)));
        return windows;
    }

    @Override
    public long size() {
        return Duration.ofDays(1).toMillis();
    }
}