package utility;

import flink_dsp.query3.CompanyRankingOutcome;
import org.apache.kafka.streams.KeyValue;
import scala.Tuple2;
import utility.accumulators.CompanyRankingAccumulator;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Scope: Global
 * Class containing queries evaluation needed transformations
 */
public class DataCommonTransformation {

	/**
	 * Scope: Global - Query 3
	 * Ranking size of the third query, how many companies must be inserted building the ranking
	 */
	private static final int RANK_SIZE = 5;

	/**
	 * Scope: Kafka Streams - Global
	 * Converts a busData to a daily keyed busData
	 * @param busData which a daily key must be assigned to
	 * @return a key value pair, key is a dd/MM/yyyy string, value is the original busData
	 */
	public static KeyValue<String, BusData> toDailyKeyed(BusData busData) {
		Calendar calendar = getCalendarAtTime(busData.getEventTime());

		String newDailyKey = calendar.get(Calendar.DAY_OF_MONTH) +
				"/" +
				calendar.get(Calendar.MONTH) +
				"/" +
				calendar.get(Calendar.YEAR);

		return new KeyValue<>(newDailyKey, busData);
	}

	/**
	 * Scope: Kafka Streams - Global
	 * Converts a busData to a weekly keyed busData
	 * @param busData which a weekly key must be assigned to
	 * @return a key value pair, key is a ww/yyyy (w stands for week of year) string, value is the original busData
	 */
	public static KeyValue<String, BusData> toWeeklyKeyed(BusData busData) {
		Calendar calendar = getCalendarAtTime(busData.getEventTime());

		String newWeeklyKey = calendar.get(Calendar.WEEK_OF_YEAR) +
				"/" +
				calendar.get(Calendar.YEAR);

		return new KeyValue<>(newWeeklyKey, busData);
	}

	/**
	 * Scope: Kafka Streams - Global
	 * Converts a busData to a monthly keyed busData
	 * @param busData which a monthly key must be assigned to
	 * @return a key value pair, key is a MM/yyyy string, value is the original busData
	 */
	public static KeyValue<String, BusData> toMonthlyKeyed(BusData busData) {
		Calendar calendar = getCalendarAtTime(busData.getEventTime());

		String newMonthlyKey = calendar.get(Calendar.MONTH) +
				"/" +
				calendar.get(Calendar.YEAR);

		return new KeyValue<>(newMonthlyKey, busData);
	}

	/**
	 * Creates a Gregorian Calendar with the current date
	 * @param date of the new Gregorian Calendar
	 * @return created Gregorian Calendar
	 */
	public static Calendar getCalendarAtTime(Date date) {
		Calendar calendar = new GregorianCalendar(Locale.US);
		calendar.setTime(date);
		return calendar;
	}

	/**
	 * Creates a Gregorian Calendar with the current date expressed as timestamp
	 * @param milliseconds timestamp of the new Gregorian Calendar
	 * @return created Gregorian Calendar
	 */
	public static Calendar getCalendarAtTime(Long milliseconds) {
		Calendar calendar = new GregorianCalendar(Locale.US);
		calendar.setTimeInMillis(milliseconds);
		return calendar;
	}

	/**
	 * Converts a date expressed as timestamp to a yyyy/MM/dd string
	 * @param milliseconds timestamp
	 * @return formatted timestamp as string
	 */
	public static String formatDate(Long milliseconds) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd", Locale.US);
		return format.format(new Date(milliseconds));
	}

	/**
	 * Scope: Global - Query 3
	 * Builds an ordered ranking from an hashmap of couples [name - ranking score]
	 * @param unorderedRanking hashmap
	 * @return list of map entries sorted basing on ranking score
	 */
	private static List<Map.Entry<String, Double>> buildCompanyRanking(HashMap<String, Double> unorderedRanking) {
		// Create the list from elements of HashMap
		List<Map.Entry<String, Double>> ranking = new LinkedList<>(unorderedRanking.entrySet());

		// Sort the list
		ranking.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

		return ranking;
	}

	/**
	 * Scope: Flink - Query 3
	 * Builds a CompanyRankingOutcome (accumulator outcome) from a CompanyRankingAccumulator
	 * @param accumulator final accumulator version from which outcome must be produced
	 * @return outcome
	 */
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

	/**
	 * Scope: Kafka Streams - Query 3
	 * Builds an outcome string from a CompanyRankingAccumulator
	 * @param accumulator final accumulator version from which outcome must be produced
	 * @return outcome string
	 */
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
		// removes last ";" character
		outcomeBuilder.deleteCharAt(outcomeBuilder.length() - 1);

		return outcomeBuilder.toString();
	}
}
