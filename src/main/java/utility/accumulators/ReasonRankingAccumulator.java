package utility.accumulators;

import java.util.*;

public class ReasonRankingAccumulator {
	private Date startDate;
	private final HashMap<String, Long> amRanking;
	private final HashMap<String, Long> pmRanking;

	public ReasonRankingAccumulator() {
		this.startDate = new Date(Long.MAX_VALUE);
		this.amRanking = new HashMap<>();
		this.pmRanking = new HashMap<>();
	}

	public void add(Date date, String reason, Long counter) {
		//threshold setup
		Calendar threshold = Calendar.getInstance(Locale.US);
		threshold.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		threshold.setTime(date);

		//element setup
		Calendar elem = Calendar.getInstance(Locale.US);
		elem.setTimeZone(TimeZone.getTimeZone("America/New_York"));
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
			this.amRanking.merge(reason, counter, Long::sum);
		} else {
			this.pmRanking.merge(reason, counter, Long::sum);
		}
	}

	public void mergeRankings(HashMap<String, Long> am, HashMap<String, Long> pm) {
		am.forEach((k, v) -> this.amRanking.merge(k, v, Long::sum));
		pm.forEach((k, v) -> this.pmRanking.merge(k, v, Long::sum));
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public HashMap<String, Long> getAmRanking() {
		return amRanking;
	}

	public HashMap<String, Long> getPmRanking() {
		return pmRanking;
	}
}
