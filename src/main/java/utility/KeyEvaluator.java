package utility;

import org.apache.kafka.streams.KeyValue;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class KeyEvaluator {

    public static KeyValue<String, BusData> toDailyKeyed(BusData busData) {
        Calendar calendar = getCalendarAtTime(busData.getEventTime());

        String newDailyKey = calendar.get(Calendar.DAY_OF_MONTH) +
                "/" +
                calendar.get(Calendar.MONTH) +
                "/" +
                calendar.get(Calendar.YEAR);

        return new KeyValue<>(newDailyKey, busData);
    }

    public static KeyValue<String, BusData> toWeeklyKeyed(BusData busData) {
        Calendar calendar = getCalendarAtTime(busData.getEventTime());

        String newWeeklyKey = calendar.get(Calendar.WEEK_OF_YEAR) +
                "/" +
                calendar.get(Calendar.YEAR);

        return new KeyValue<>(newWeeklyKey, busData);
    }

    public static KeyValue<String, BusData> toMonthlyKeyed(BusData busData) {
        Calendar calendar = getCalendarAtTime(busData.getEventTime());

        String newMonthlyKey = calendar.get(Calendar.MONTH) +
                "/" +
                calendar.get(Calendar.YEAR);

        return new KeyValue<>(newMonthlyKey, busData);
    }

    private static Calendar getCalendarAtTime(Date date) {
        Calendar calendar = Calendar.getInstance(Locale.US);
        calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        calendar.setTime(date);
        return calendar;
    }
}
