package flink_dsp.query3;

import java.util.Date;
import java.util.HashMap;

public class CompanyRankingAccumulator {

    private Date startDate;
    private final HashMap<String, Double> companyRanking;

    public CompanyRankingAccumulator() {
        this.startDate = new Date(Long.MAX_VALUE);
        companyRanking = new HashMap<>();
    }

    public void add(String companyName, String reason, boolean countTwice) {
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
