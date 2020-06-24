package utility.accumulators;

import java.util.*;

/**
 * Scope: Global - Query 2
 * Accumulator used on time windows for delay reason ranking
 */
@SuppressWarnings("unused")
public class ReasonRankingAccumulator {
	// ranking of morning hours as couples [reason - occurrences]
	private final HashMap<String, Long> amRanking;
	// ranking of afternoon hours as couples [reason - occurrences]
	private final HashMap<String, Long> pmRanking;

	/**
	 * No arguments constructor
	 */
	public ReasonRankingAccumulator() {
		// initialize structures
		this.amRanking = new HashMap<>();
		this.pmRanking = new HashMap<>();
	}

	/**
	 * Adds new info to the current delay reason ranking
	 * @param date event date (with hours and minutes too)
	 * @param reason of the delay
	 */
	public void add(Date date, String reason) {
		// threshold setup
		Calendar threshold = Calendar.getInstance(Locale.US);
		threshold.setTime(date);

		// current event date setup
		Calendar elem = Calendar.getInstance(Locale.US);
		elem.setTime(date);

		threshold.set(Calendar.MINUTE, 0);
		threshold.set(Calendar.SECOND, 0);
		threshold.set(Calendar.MILLISECOND, 0);

		// check if before 05:00
		threshold.set(Calendar.HOUR_OF_DAY, 5);

		// out of both time intervals
		if (elem.before(threshold)) {
			return;
		}

		// check if after 19:00
		threshold.set(Calendar.HOUR_OF_DAY, 19);

		// out of both time intervals
		if (elem.after(threshold)) {
			return;
		}

		// set threshold at 12:00 of the same day
		threshold.set(Calendar.HOUR_OF_DAY, 12);

		//check if it falls in am or pm
		if (elem.before(threshold)) {
			// add to morning ranking
			this.amRanking.merge(reason, 1L, Long::sum);
		} else {
			// add to afternoon ranking
			this.pmRanking.merge(reason, 1L, Long::sum);
		}
	}

	/**
	 * Merge ranking maps to the current ones
	 * @param am morning ranking map
	 * @param pm afternoon ranking map
	 */
	public void mergeRankings(HashMap<String, Long> am, HashMap<String, Long> pm) {
		am.forEach((k, v) -> this.amRanking.merge(k, v, Long::sum));
		pm.forEach((k, v) -> this.pmRanking.merge(k, v, Long::sum));
	}

	public HashMap<String, Long> getAmRanking() {
		return amRanking;
	}

	public HashMap<String, Long> getPmRanking() {
		return pmRanking;
	}
}
