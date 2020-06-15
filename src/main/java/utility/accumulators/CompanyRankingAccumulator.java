package utility.accumulators;

import flink_dsp.query3.DelayScorer;

import java.util.Date;
import java.util.HashMap;

public class CompanyRankingAccumulator {

    private static final int THRESHOLD_FOR_DOUBLE_SCORE = 30;
    // TODO: check date update basing on new processWindow method
    private Date startDate;
    private final HashMap<String, Double> companyRanking;

    public CompanyRankingAccumulator() {
        this.startDate = new Date(Long.MAX_VALUE);
        companyRanking = new HashMap<>();
    }

    public void add(String companyName, String reason, Double delay) {
        boolean countTwice = false;
        if (delay > THRESHOLD_FOR_DOUBLE_SCORE) {
            countTwice = true;
        }
        this.companyRanking.merge(companyName, DelayScorer.computeAmount(reason, countTwice), Double::sum);
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public HashMap<String, Double> getCompanyRanking() {
        return companyRanking;
    }
}