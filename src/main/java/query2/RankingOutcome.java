package query2;

import java.util.ArrayList;
import java.util.Date;

public class RankingOutcome {
    private Date startDate;
    private ArrayList<String> amRanking;
    private ArrayList<String> pmRanking;

    public RankingOutcome(Date startDate) {
        this.startDate = startDate;
        this.amRanking = new ArrayList<>();
        this.pmRanking = new ArrayList<>();
    }

    public void addAmRanking(String reason) {
        this.amRanking.add(reason);
    }

    public void addPmRanking(String reason) {
        this.pmRanking.add(reason);
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public ArrayList<String> getAmRanking() {
        return amRanking;
    }

    public void setAmRanking(ArrayList<String> amRanking) {
        this.amRanking = amRanking;
    }

    public ArrayList<String> getPmRanking() {
        return pmRanking;
    }

    public void setPmRanking(ArrayList<String> pmRanking) {
        this.pmRanking = pmRanking;
    }
}
