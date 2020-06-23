package utility.accumulators;

import java.util.Date;
import java.util.HashMap;

/**
 * Scope: Global - Query 1
 * Accumulator used on time windows for average delay statistics by neighbourhood evaluation
 */
@SuppressWarnings("unused")
public class AverageDelayAccumulator {

	// map of [neighbourhood - statistics] couples
	private HashMap<String, AverageDelayStatistics> boroMap;
	// evaluation start date
	private Date startDate;

	/**
	 * Scope: Kafka Streams' serdes
	 * Default constructor
	 * @param boroMap neighbourhood statistics hash map
	 * @param startDate evaluation start date
	 */
	public AverageDelayAccumulator(HashMap<String, AverageDelayStatistics> boroMap, Date startDate) {
		this.boroMap = boroMap;
		this.startDate = startDate;
	}

	/**
	 * No arguments constructor
	 */
	public AverageDelayAccumulator() {
		// max possible start date, it will be then set to evaluation window start date
		this.startDate = new Date(Long.MAX_VALUE);
		// create the neighbourhood map
		this.boroMap = new HashMap<>();
	}

	/**
	 * Adds new info to the current neighbourhood map
	 * @param boro neighbourhood of the info to add
	 * @param total total delay
	 * @param counter delay's occurrences number
	 */
	public void add(String boro, Double total, Long counter) {
		// get current elem
		AverageDelayStatistics elem = this.boroMap.get(boro);
		// if the element was not in the map add it
		if (elem == null) {
			this.boroMap.put(boro, new AverageDelayStatistics(total, counter));
		} else {
			// if the element was in the map replace it with a new element containing merged info
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

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	/**
	 * Scope: Kafka Streams' serdes
	 */
	public void setBoroMap(HashMap<String, AverageDelayStatistics> boroMap) {
		this.boroMap = boroMap;
	}
}
