package utility;

import utility.delay_parsing.DelayFormatException;
import utility.delay_parsing.DelayParsingUtility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BusData {
    private Date eventTime;
    private Double delay; // expressed in minutes
    private String boro;

    /**
     * Query 1 scope
     * @param eventTime
     * @param delay
     * @param boro
     * @throws ParseException
     * @throws DelayFormatException
     */
    public BusData(String eventTime, String delay, String boro) throws ParseException, DelayFormatException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        this.eventTime = format.parse(eventTime);
        this.delay = DelayParsingUtility.parseDelay(delay);
        this.boro = boro;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public Double getDelay() {
        return delay;
    }

    public String getBoro() {
        return boro;
    }
}
