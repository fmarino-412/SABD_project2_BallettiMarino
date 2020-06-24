package utility;

import utility.delay.DelayFormatException;
import utility.delay.DelayParsingUtility;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Scope: global
 * Class representing tuple information needed for queries evaluation
 */
@SuppressWarnings("unused")
public class BusData implements Serializable {

	// time of the event as Date
	private Date eventTime;
	// dealy minutes
	private Double delay;
	// neighbourhood identifier
	private String boro;
	// delay reason
	private String reason;
	// bus company name
	private String companyName;

	/**
	 * Scope: Kafka Streams' serdes
	 * Default constructor
	 */
	public BusData() {
	}

	/**
	 * Scope: Kafka Streams' serdes
	 * @param eventTime time of the event as Date
	 * @param delay in minutes
	 * @param boro neighbourhood identifier
	 * @param reason of delay
	 * @param companyName bus company name
	 */
	public BusData(Date eventTime, Double delay, String boro, String reason, String companyName) {
		this.eventTime = eventTime;
		this.delay = delay;
		this.boro = boro;
		this.reason = reason;
		this.companyName = companyName;
	}

	/**
	 * Scope: Global - Query 1
	 * Constructor with information needed for the first query evaluation
	 * @param eventTime time of the event as Date
	 * @param delay in minutes
	 * @param boro of delay
	 * @throws ParseException if there was an error parsing the date
	 * @throws DelayFormatException if no information was found in the delay string
	 */
	public BusData(String eventTime, String delay, String boro) throws ParseException, DelayFormatException {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
		this.eventTime = format.parse(eventTime);
		this.delay = DelayParsingUtility.parseDelay(delay);
		this.boro = boro;
	}

	/**
	 * Scope: Global - Query 2
	 * Constructor with information needed for the second query evaluation
	 * @param eventTime time of the event as Date
	 * @param reason of delay
	 * @throws ParseException if there was an error parsing the date
	 */
	public BusData(String eventTime, String reason) throws ParseException {
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS", Locale.US);
		this.eventTime = format.parse(eventTime);
		this.reason = reason;
	}

	/**
	 * Scope: Global - Query 3
	 * Constructor with information needed for the third query evaluation
	 * @param eventTime time of the event as Date
	 * @param delay in minutes
	 * @param reason of delay
	 * @param companyName bus company name
	 * @throws ParseException if there was an error parsing the date
	 * @throws DelayFormatException if no information was found in the delay string
	 */
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

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setEventTime(Date eventTime) {
		this.eventTime = eventTime;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setDelay(Double delay) {
		this.delay = delay;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setBoro(String boro) {
		this.boro = boro;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setReason(String reason) {
		this.reason = reason;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
}
