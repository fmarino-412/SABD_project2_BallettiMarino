package utility;

import utility.delay_utility.DelayFormatException;
import utility.delay_utility.DelayParsingUtility;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

@SuppressWarnings("unused")
public class BusData implements Serializable {

	private Date eventTime;
	private Double delay; // expressed in minutes
	private String boro;
	private String reason;
	private String companyName;

	/* SERDES SCOPE */
	public BusData() {
	}

	public BusData(Date eventTime, Double delay, String boro, String reason, String companyName) {
		this.eventTime = eventTime;
		this.delay = delay;
		this.boro = boro;
		this.reason = reason;
		this.companyName = companyName;
	}

	/* Query 1 scope */
	public BusData(String eventTime, String delay, String boro) throws ParseException, DelayFormatException {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
		this.eventTime = format.parse(eventTime);
		this.delay = DelayParsingUtility.parseDelay(delay);
		this.boro = boro;
	}

	/* Query 2 scope */
	public BusData(String eventTime, String reason) throws ParseException {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
		this.eventTime = format.parse(eventTime);
		this.reason = reason;
	}

	/* Query 3 scope */
	public BusData(String eventTime, String delay, String reason, String companyName) throws ParseException,
			DelayFormatException {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
		this.eventTime = format.parse(eventTime);
		this.delay = DelayParsingUtility.parseDelay(delay);
		this.reason = reason;
		this.companyName = companyName;
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

	public String getCompanyName() {
		return companyName;
	}

	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	public void setDelay(Double delay) {
		this.delay = delay;
	}

	public void setBoro(String boro) {
		this.boro = boro;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
}
