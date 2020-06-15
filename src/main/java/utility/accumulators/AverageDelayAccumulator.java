package utility.accumulators;

import java.util.Date;
import java.util.HashMap;

public class AverageDelayAccumulator {

	private HashMap<String, AverageDelayStatistics> boroMap;
	private Date startDate;

	/* SERDE SCOPE */
	public AverageDelayAccumulator(HashMap<String, AverageDelayStatistics> boroMap, Date startDate) {
		this.boroMap = boroMap;
		this.startDate = startDate;
	}

	public AverageDelayAccumulator() {
		this.startDate = new Date(Long.MAX_VALUE);
		this.boroMap = new HashMap<>();
	}

	public void add(String boro, Double total, Long counter) {
		AverageDelayStatistics elem = this.boroMap.get(boro);
		if (elem == null) {
			this.boroMap.put(boro, new AverageDelayStatistics(total, counter));
		} else {
			this.boroMap.put(boro, new AverageDelayStatistics(elem.getTotal() + total,
					elem.getCounter() + counter));
		}
	}

	public HashMap<String, AverageDelayStatistics> getBoroMap() {
		return boroMap;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public void setBoroMap(HashMap<String, AverageDelayStatistics> boroMap) {
		this.boroMap = boroMap;
	}
}
