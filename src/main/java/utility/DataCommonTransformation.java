package utility;

import flink_dsp.query3.CompanyRankingOutcome;
import org.apache.kafka.streams.KeyValue;
import scala.Tuple2;
import utility.accumulators.CompanyRankingAccumulator;

import java.util.*;

public class DataCommonTransformation {

	/* Query 3 scope */
	private static final int RANK_SIZE = 5;

	public static KeyValue<String, BusData> toDailyKeyed(BusData busData) {
		Calendar calendar = getCalendarAtTime(busData.getEventTime());

		String newDailyKey = calendar.get(Calendar.DAY_OF_MONTH) +
				"/" +
				calendar.get(Calendar.MONTH) +
				"/" +
				calendar.get(Calendar.YEAR);

		return new KeyValue<>(newDailyKey, busData);
	}

	public static KeyValue<String, BusData> toWeeklyKeyed(BusData busData) {
		Calendar calendar = getCalendarAtTime(busData.getEventTime());

		String newWeeklyKey = calendar.get(Calendar.WEEK_OF_YEAR) +
				"/" +
				calendar.get(Calendar.YEAR);

		return new KeyValue<>(newWeeklyKey, busData);
	}

	public static String getMonthlyKey(Date date) {
		Calendar calendar = getCalendarAtTime(date);

		return calendar.get(Calendar.MONTH) +
				"/" +
				calendar.get(Calendar.YEAR);
	}

	public static KeyValue<String, BusData> toMonthlyKeyed(BusData busData) {
		String newMonthlyKey = getMonthlyKey(busData.getEventTime());

		return new KeyValue<>(newMonthlyKey, busData);
	}

	private static Calendar getCalendar() {
		Calendar calendar = Calendar.getInstance(Locale.US);
		calendar.setTimeZone(TimeZone.getTimeZone("America/New_York"));
		return calendar;
	}

	private static Calendar getCalendarAtTime(Date date) {
		Calendar calendar = getCalendar();
		calendar.setTime(date);
		return calendar;
	}

	public static Calendar getCalendarAtTime(Long milliseconds) {
		Calendar calendar = getCalendar();
		calendar.setTimeInMillis(milliseconds);
		return calendar;
	}

	/* QUERY 3 SCOPE */
	public static List<Map.Entry<String, Double>> buildCompanyRanking(HashMap<String, Double> unorderedRanking) {
		// Create the list from elements of HashMap
		List<Map.Entry<String, Double>> ranking = new LinkedList<>(unorderedRanking.entrySet());

		// Sort the list
		ranking.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

		return ranking;
	}

	public static CompanyRankingOutcome buildCompanyRankingOutcome(CompanyRankingAccumulator accumulator) {

		// Create the list
		List<Map.Entry<String, Double>> ranking = buildCompanyRanking(accumulator.getCompanyRanking());

		// Generating outcome
		CompanyRankingOutcome outcome = new CompanyRankingOutcome(accumulator.getStartDate());
		for (int i = 0; i < RANK_SIZE; i++) {
			try {
				outcome.addRanking(new Tuple2<>(ranking.get(i).getKey(), ranking.get(i).getValue()));
			} catch (IndexOutOfBoundsException ignored) {
				// less than RANK_SIZE elements
			}
		}

		return outcome;
	}

	public static String buildCompanyRankingString(CompanyRankingAccumulator accumulator) {

		// Create the list
		List<Map.Entry<String, Double>> ranking = buildCompanyRanking(accumulator.getCompanyRanking());

		// Generating outcome
		StringBuilder outcomeBuilder = new StringBuilder();
		for (int i = 0; i < RANK_SIZE; i++) {
			try {
				outcomeBuilder.append(ranking.get(i).getKey()).append(";").append(ranking.get(i).getValue())
						.append(";");
			} catch (IndexOutOfBoundsException ignored) {
				// less than RANK_SIZE elements
			}
		}
		outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);

		return outcomeBuilder.toString();
	}
}
