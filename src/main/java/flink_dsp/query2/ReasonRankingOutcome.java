package flink_dsp.query2;

import java.util.ArrayList;
import java.util.Date;

/**
 * Class that contains second query's window processing outcome
 */
public class ReasonRankingOutcome {
	private Date startDate;
	private final ArrayList<String> amRanking;
	private final ArrayList<String> pmRanking;

	/**
	 * Constructor for the outcome that sets the start date and initializes the lists
	 */
	public ReasonRankingOutcome() {
		this.amRanking = new ArrayList<>();
		this.pmRanking = new ArrayList<>();
	}

	/**
	 * Adds a reason to the outcome's am list
	 * @param reason to be added
	 */
	public void addAmRanking(String reason) {
		this.amRanking.add(reason);
	}

	/**
	 * Adds a reason to the outcome's pm list
	 * @param reason to be added
	 */
	public void addPmRanking(String reason) {
		this.pmRanking.add(reason);
	}

	/**
	 * Setter for startDate
	 * @param startDate to be set
	 */
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	/**
	 * Getter for startDate
	 * @return the startDate
	 */
	public Date getStartDate() {
		return startDate;
	}

	/**
	 * Getter for the am ranking list
	 * @return the list
	 */
	public ArrayList<String> getAmRanking() {
		return amRanking;
	}

	/**
	 * Getter for the pm ranking list
	 * @return the list
	 */
	public ArrayList<String> getPmRanking() {
		return pmRanking;
	}
}
