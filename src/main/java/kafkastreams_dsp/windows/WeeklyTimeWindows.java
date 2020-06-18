package kafkastreams_dsp.windows;

import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

import static utility.DataCommonTransformation.getCalendarAtTime;

//Implementation of a weekly custom window with a given timezone
public class WeeklyTimeWindows extends CustomTimeWindows {

    private final static long SIZE_IN_MILLIS = Duration.ofDays(7L).toMillis();

    @SuppressWarnings("deprecation")
    public WeeklyTimeWindows(final ZoneId zoneId, final Duration grace) {
        super(zoneId, grace);

        // use of a deprecated method is the only possible solution to define a retention time different from 1 day
        this.until(SIZE_IN_MILLIS);
    }

    @Override
    public Map<Long, TimeWindow> windowsFor(final long timestamp) {
        final Instant instant = Instant.ofEpochMilli(timestamp);
        final ZonedDateTime zonedDateTime = instant.atZone(zoneId);

        Calendar calendar = getCalendarAtTime(toEpochMilli(zonedDateTime));
        calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        final long startTime = calendar.getTimeInMillis();

        calendar.add(Calendar.WEEK_OF_YEAR, 1);
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