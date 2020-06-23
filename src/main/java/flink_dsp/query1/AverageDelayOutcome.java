package flink_dsp.query1;

import java.util.Date;
import java.util.HashMap;

/**
 * Class that contains first query's window processing outcome
 */
public class AverageDelayOutcome {
	private Date startDate;
	// hashmap to keep track of per boro's average delay
	private final HashMap<String, Double> boroMeans = new HashMap<>();

	/**
	 * Default constructor
	 * @param startDate to be set
	 */
	public AverageDelayOutcome(Date startDate) {
		this.startDate = startDate;
	}

	/**
	 * Add a boro-mean pair to the hash map, if boro is "" then it isn't added
	 * @param boro String, the boro's name
	 * @param mean Double, the boro's average delay
	 */
	public void addMean(String boro, Double mean) {
		if (boro.equals("")) {
			return;
		}
		this.boroMeans.put(boro, mean);
	}

	/**
	 * Getter for startDate
	 * @return the startDate
	 */
	public Date getStartDate() {
		return startDate;
	}

	/**
	 * Setter for startDate
	 * @param startDate to be set
	 */
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	/**
	 * Getter for the boro's hashmap
	 * @return the hashmap
	 */
	public HashMap<String, Double> getBoroMeans() {
		return boroMeans;
	}
}
