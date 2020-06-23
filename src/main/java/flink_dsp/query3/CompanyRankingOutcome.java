package flink_dsp.query3;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;

/**
 * Class that contains third query's window processing outcome
 */
public class CompanyRankingOutcome {
	private Date startDate;
	// list that keep track of companies and their ranking value
	private final ArrayList<Tuple2<String, Double>> companyRanking;

	/**
	 * Constructor for the outcome that sets the start date and initializes the lists
	 * @param startDate to be set
	 */
	public CompanyRankingOutcome(Date startDate) {
		this.startDate = startDate;
		this.companyRanking = new ArrayList<>();
	}

	/**
	 * Adds a company to the outcome's list
	 * @param score tuple containing company name and score
	 */
	public void addRanking(Tuple2<String, Double> score) {
		this.companyRanking.add(score);
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
	 * Getter for the company ranking list
	 * @return the list
	 */
	public ArrayList<Tuple2<String, Double>> getCompanyRanking() {
		return companyRanking;
	}
}
