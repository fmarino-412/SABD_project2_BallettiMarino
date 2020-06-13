package utility;

import org.apache.commons.io.FileUtils;
import query1.AverageDelayOutcome;
import query2.ReasonRankingOutcome;
import query3.CompanyRankingOutcome;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@SuppressWarnings({"ResultOfMethodCallIgnored", "DuplicatedCode"})
public class OutputFormatter {
    private static final boolean CONSOLE_RESULTS_PRINT = true;
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

    /* Query1 scope */
    public static final String QUERY1_DAILY_CSV_FILE_PATH = "Results/query1_daily.csv";
    public static final String QUERY1_WEEKLY_CSV_FILE_PATH = "Results/query1_weekly.csv";
    public static final String QUERY1_MONTHLY_CSV_FILE_PATH = "Results/query1_monthly.csv";

    /* Query2 scope */
    public static final String QUERY2_DAILY_CSV_FILE_PATH = "Results/query2_daily.csv";
    public static final String QUERY2_WEEKLY_CSV_FILE_PATH = "Results/query2_weekly.csv";

    /* Query3 scope */
    public static final String QUERY3_DAILY_CSV_FILE_PATH = "Results/query3_daily.csv";
    public static final String QUERY3_WEEKLY_CSV_FILE_PATH = "Results/query3_weekly.csv";

    /**
     * Global scope
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
     * Query1 scope
     * Save outcome to csv file and print result
     * @param path where to store csv
     * @param outcome to be stored
     */
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

            builder.append(outcome.getStartDate().getTime());
            outcome.getBoroMeans().forEach((k, v) -> builder.append(CSV_SEP).append(k).append(CSV_SEP).append(v));
            builder.append(NEW_LINE);
            bw.append(builder.toString());
            bw.close();
            writer.close();

            //print output formatted
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
     * Query2 scope
     * Save outcome to csv file and print result
     * @param path where to store csv
     * @param outcome to be stored
     */
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

            builder.append(outcome.getStartDate().getTime());
            builder.append(CSV_SEP + AM + CSV_SEP);
            builder.append(outcome.getAmRanking().toString());
            builder.append(CSV_SEP + PM + CSV_SEP);
            builder.append(outcome.getPmRanking().toString());
            builder.append(NEW_LINE);

            bw.append(builder.toString());
            bw.close();
            writer.close();

            //print output formatted
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
     * Query3 scope
     * Save outcome to csv file and print result
     * @param path where to store csv
     * @param outcome to be stored
     */
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

            builder.append(outcome.getStartDate().getTime());
            for (Tuple2<String, Double> score : outcome.getCompanyRanking()) {
                builder.append(CSV_SEP)
                        .append(score._1())
                        .append(CSV_SEP)
                        .append(score._2());
            }
            builder.append("\n");

            bw.append(builder.toString());
            bw.close();
            writer.close();

            //print output formatted
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
