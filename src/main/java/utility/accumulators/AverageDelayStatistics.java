package utility.accumulators;

@SuppressWarnings("unused")
public class AverageDelayStatistics {
	private Double total;
	private Long counter;

	public AverageDelayStatistics() {
	}

	public AverageDelayStatistics(Double total, Long counter) {
		this.total = total;
		this.counter = counter;
	}

	public Double getTotal() {
		return total;
	}

	public void setTotal(Double total) {
		this.total = total;
	}

	public Long getCounter() {
		return counter;
	}

	public void setCounter(Long counter) {
		this.counter = counter;
	}
}
