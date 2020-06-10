package utility;

import utility.delay_parsing.DelayFormatException;
import utility.delay_parsing.DelayParsingUtility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BusData {
    private Date eventTime;

    /* Query 1 scope */
    private Double delay; // expressed in minutes
    private String boro;

    /* Query 2 scope */
    private String reason;

    /* Query 1 scope */
    public BusData(String eventTime, String delay, String boro) throws ParseException, DelayFormatException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        this.eventTime = format.parse(eventTime);
        this.delay = DelayParsingUtility.parseDelay(delay);
        this.boro = boro;
    }

    /* Query 2 scope */
    public BusData(String eventTime, String reason) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        this.eventTime = format.parse(eventTime);
        this.reason = reason;
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

    public String getReason() {
        return reason;
    }
}
