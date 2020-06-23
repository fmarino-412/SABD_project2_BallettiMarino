package utility.accumulators;

/**
 * Scope: Global - Query 1
 * Class used to maintain info about delays for every neighbour
 */
@SuppressWarnings("unused")
public class AverageDelayStatistics {
	// total delay time
	private Double total;
	// total delay occurrences
	private Long counter;

	/**
	 * Scope: Kafka Streams' serdes
	 * Default constructor
	 */
	public AverageDelayStatistics() {
	}

	/**
	 * Creates a new statistic
	 * @param total delay minutes
	 * @param counter of delay occurrences
	 */
	public AverageDelayStatistics(Double total, Long counter) {
		this.total = total;
		this.counter = counter;
	}

	public Double getTotal() {
		return total;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setTotal(Double total) {
		this.total = total;
	}

	public Long getCounter() {
		return counter;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setCounter(Long counter) {
		this.counter = counter;
	}
}
