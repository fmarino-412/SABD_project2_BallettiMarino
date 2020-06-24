package utility;

import flink_dsp.query1.AverageDelayOutcome;
import flink_dsp.query2.ReasonRankingOutcome;
import flink_dsp.query3.CompanyRankingOutcome;
import org.apache.commons.io.FileUtils;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static utility.DataCommonTransformation.formatDate;

/**
 * Class used to format the queries' outcomes into string
 */
@SuppressWarnings({"ResultOfMethodCallIgnored", "DuplicatedCode"})
public class OutputFormatter {
	private static final boolean CONSOLE_RESULTS_PRINT = false;
	private static final String RESULTS_DIRECTORY = "Results";
	private static final String CSV_SEP = ";";
	private static final String NEW_LINE = "\n";
	private static final String AM = "05:00-11:59";
	private static final String PM = "12:00-19:00";
	private static final String DAILY_HEADER = "DAILY: \t";
	private static final String WEEKLY_HEADER = "WEEKLY: \t";
	private static final String MONTHLY_HEADER = "MONTHLY: \t";
	private static final String QUERY1_HEADER = "QUERY 1 - ";
	private static final String QUERY2_HEADER = "QUERY 2 - ";
	private static final String QUERY3_HEADER = "QUERY 3 - ";

	/**
	 * Scope: Flink - Query 1
	 */
	public static final String QUERY1_DAILY_CSV_FILE_PATH = "Results/query1_daily.csv";
	public static final String QUERY1_WEEKLY_CSV_FILE_PATH = "Results/query1_weekly.csv";
	public static final String QUERY1_MONTHLY_CSV_FILE_PATH = "Results/query1_monthly.csv";

	/**
	 * Scope: Flink - Query 2
	 */
	public static final String QUERY2_DAILY_CSV_FILE_PATH = "Results/query2_daily.csv";
	public static final String QUERY2_WEEKLY_CSV_FILE_PATH = "Results/query2_weekly.csv";

	/**
	 * Scope: Flink - Query 3
	 */
	public static final String QUERY3_DAILY_CSV_FILE_PATH = "Results/query3_daily.csv";
	public static final String QUERY3_WEEKLY_CSV_FILE_PATH = "Results/query3_weekly.csv";

	public static final String[] FLINK_OUTPUT_FILES = {QUERY1_DAILY_CSV_FILE_PATH, QUERY1_WEEKLY_CSV_FILE_PATH,
			QUERY1_MONTHLY_CSV_FILE_PATH, QUERY2_DAILY_CSV_FILE_PATH, QUERY2_WEEKLY_CSV_FILE_PATH,
			QUERY3_DAILY_CSV_FILE_PATH, QUERY3_WEEKLY_CSV_FILE_PATH};

	/**
	 * Scope: Global
	 * Used to remove old files in Results directory
	 */
	public static void cleanResultsFolder() {
		try {
			FileUtils.cleanDirectory(new File(RESULTS_DIRECTORY));
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Could not clean Results directory");
		}
	}

	/**
	 * Scope: Flink - Query 1
	 * Formats the processing outcome as a string to be published to Kafka
	 * @param outcome to format
	 * @return formatted string
	 */
	public static String query1OutcomeFormatter(AverageDelayOutcome outcome) {
		StringBuilder builder = new StringBuilder();
		builder.append(formatDate(outcome.getStartDate().getTime()));
		outcome.getBoroMeans().forEach((k, v) -> builder.append(CSV_SEP).append(k).append(CSV_SEP).append(v));
		return builder.toString();
	}

	/**
	 * Scope: Flink - Query 2
	 * Formats the processing outcome as a string to be published to Kafka
	 * @param outcome to format
	 * @return formatted string
	 */
	public static String query2OutcomeFormatter(ReasonRankingOutcome outcome) {
		return formatDate(outcome.getStartDate().getTime()) +
				CSV_SEP + AM + CSV_SEP +
				outcome.getAmRanking().toString() +
				CSV_SEP + PM + CSV_SEP +
				outcome.getPmRanking().toString();
	}

	/**
	 * Scope: Flink - Query 3
	 * Formats the processing outcome as a string to be published to Kafka
	 * @param outcome to format
	 * @return formatted string
	 */
	public static String query3OutcomeFormatter(CompanyRankingOutcome outcome) {
		StringBuilder builder = new StringBuilder();
		builder.append(formatDate(outcome.getStartDate().getTime()));
		for (Tuple2<String, Double> score : outcome.getCompanyRanking()) {
			builder.append(CSV_SEP)
					.append(score._1())
					.append(CSV_SEP)
					.append(score._2());
		}
		return builder.toString();
	}

	/**
	 * @deprecated - needed before Kafka Flink output topic creation
	 * Scope: Flink - Query 1
	 * Save outcome to csv file and print result
	 * @param path where to store csv
	 * @param outcome to be stored
	 */
	@Deprecated
	public static void writeOutputQuery1(String path, AverageDelayOutcome outcome) {
		try {
			// output structures
			File file = new File(path);
			if (!file.exists()) {
				// creates the file if it does not exist
				file.createNewFile();
			}

			// append to existing version of the same file
			FileWriter writer = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(writer);
			StringBuilder builder = new StringBuilder();

			builder.append(formatDate(outcome.getStartDate().getTime()));
			outcome.getBoroMeans().forEach((k, v) -> builder.append(CSV_SEP).append(k).append(CSV_SEP).append(v));
			builder.append(NEW_LINE);
			bw.append(builder.toString());
			bw.close();
			writer.close();

			// prints formatted output
			if (CONSOLE_RESULTS_PRINT) {
				switch (path) {
					case QUERY1_DAILY_CSV_FILE_PATH:
						System.out.println(QUERY1_HEADER + DAILY_HEADER + builder.toString());
						break;
					case QUERY1_WEEKLY_CSV_FILE_PATH:
						System.out.println(QUERY1_HEADER + WEEKLY_HEADER + builder.toString());
						break;
					case QUERY1_MONTHLY_CSV_FILE_PATH:
						System.out.println(QUERY1_HEADER + MONTHLY_HEADER + builder.toString());
						break;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Could not export query 1 result to CSV file");
		}
	}

	/**
	 * @deprecated - needed before Kafka Flink output topic creation
	 * Scope: Flink - Query 2
	 * Save outcome to csv file and print result
	 * @param path where to store csv
	 * @param outcome to be stored
	 */
	@Deprecated
	public static void writeOutputQuery2(String path, ReasonRankingOutcome outcome) {
		try {
			// output structures
			File file = new File(path);
			if (!file.exists()) {
				// creates the file if it does not exist
				file.createNewFile();
			}

			// append to existing version of the same file
			FileWriter writer = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(writer);
			StringBuilder builder = new StringBuilder();

			builder.append(formatDate(outcome.getStartDate().getTime()));
			builder.append(CSV_SEP + AM + CSV_SEP);
			builder.append(outcome.getAmRanking().toString());
			builder.append(CSV_SEP + PM + CSV_SEP);
			builder.append(outcome.getPmRanking().toString());
			builder.append(NEW_LINE);

			bw.append(builder.toString());
			bw.close();
			writer.close();

			// prints formatted output
			if (CONSOLE_RESULTS_PRINT) {
				switch (path) {
					case QUERY2_DAILY_CSV_FILE_PATH:
						System.out.println(QUERY2_HEADER + DAILY_HEADER + builder.toString());
						break;
					case QUERY2_WEEKLY_CSV_FILE_PATH:
						System.out.println(QUERY2_HEADER + WEEKLY_HEADER + builder.toString());
						break;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Could not export query 2 result to CSV file");
		}
	}

	/**
	 * @deprecated - needed before Kafka Flink output topic creation
	 * Scope: Flink - Query 3
	 * Save outcome to csv file and print result
	 * @param path where to store csv
	 * @param outcome to be stored
	 */
	@Deprecated
	public static void writeOutputQuery3(String path, CompanyRankingOutcome outcome) {
		try {
			// output structures
			File file = new File(path);
			if (!file.exists()) {
				// creates the file if it does not exist
				file.createNewFile();
			}

			// append to existing version of the same file
			FileWriter writer = new FileWriter(file, true);
			BufferedWriter bw = new BufferedWriter(writer);
			StringBuilder builder = new StringBuilder();

			builder.append(formatDate(outcome.getStartDate().getTime()));
			for (Tuple2<String, Double> score : outcome.getCompanyRanking()) {
				builder.append(CSV_SEP)
						.append(score._1())
						.append(CSV_SEP)
						.append(score._2());
			}
			builder.append(NEW_LINE);

			bw.append(builder.toString());
			bw.close();
			writer.close();

			// prints formatted output
			if (CONSOLE_RESULTS_PRINT) {
				switch (path) {
					case QUERY3_DAILY_CSV_FILE_PATH:
						System.out.println(QUERY3_HEADER + DAILY_HEADER + builder.toString());
						break;
					case QUERY3_WEEKLY_CSV_FILE_PATH:
						System.out.println(QUERY3_HEADER + WEEKLY_HEADER + builder.toString());
						break;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Could not export query 3 result to CSV file");
		}
	}
}
