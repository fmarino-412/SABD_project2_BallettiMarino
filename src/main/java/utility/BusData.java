package utility;

import utility.delay_parsing.DelayFormatException;
import utility.delay_parsing.DelayParsingUtility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BusData {
    private Date eventTime;
    private Long delay; // expressed in minutes

    public BusData(String eventTime, String delay) throws ParseException, DelayFormatException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        this.eventTime = format.parse(eventTime);
        this.delay = DelayParsingUtility.parseDelay(delay);
    }

    public Date getEventTime() {
        return eventTime;
    }

    public Long getDelay() {
        return delay;
    }
}
