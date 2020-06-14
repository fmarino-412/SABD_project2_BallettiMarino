package query3;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.Date;

public class CompanyRankingOutcome {

    private Date startDate;
    private final ArrayList<Tuple2<String, Double>> companyRanking;

    public CompanyRankingOutcome(Date startDate) {
        this.startDate = startDate;
        this.companyRanking = new ArrayList<>();
    }

    public void addRanking(Tuple2<String, Double> score) {
        this.companyRanking.add(score);
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getStartDate() {
        return startDate;
    }

    public ArrayList<Tuple2<String, Double>> getCompanyRanking() {
        return companyRanking;
    }
}
