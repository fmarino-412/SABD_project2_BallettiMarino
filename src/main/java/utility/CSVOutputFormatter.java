package utility;

import org.apache.commons.io.FileUtils;
import query1.AverageDelayOutcome;
import query2.RankingOutcome;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CSVOutputFormatter {
    private static final String RESULTS_DIRECTORY = "Results";

    /* Query1 scope */
    public static final String QUERY1_DAILY_CSV_FILE_PATH = "Results/query1_daily.csv";
    public static final String QUERY1_WEEKLY_CSV_FILE_PATH = "Results/query1_weekly.csv";
    public static final String QUERY1_MONTHLY_CSV_FILE_PATH = "Results/query1_monthly.csv";

    /* Query2 scope */
    public static final String QUERY2_DAILY_CSV_FILE_PATH = "Results/query2_daily.csv";
    public static final String QUERY2_WEEKLY_CSV_FILE_PATH = "Results/query2_weekly.csv";

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
     * @param path
     * @param outcome
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
            outcome.getBoroMeans().forEach((k, v) -> builder.append(";").append(k).append(";").append(v));
            builder.append("\n");
            bw.append(builder.toString());
            bw.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 1 result to CSV file");
        }
    }

    /**
     * Query2 scope
     * @param path
     * @param outcome
     */
    public static void writeOutputQuery2(String path, RankingOutcome outcome) {
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
            builder.append(";05:00-11:59;");
            builder.append(outcome.getAmRanking().toString());
            builder.append(";12:00-19:00;");
            builder.append(outcome.getPmRanking().toString());
            builder.append("\n");

            bw.append(builder.toString());
            bw.close();
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not export query 2 result to CSV file");
        }
    }
}
