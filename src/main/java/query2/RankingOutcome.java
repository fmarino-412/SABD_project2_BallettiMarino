package query2;

import java.util.ArrayList;
import java.util.Date;

public class RankingOutcome {
    private final Date startDate;
    private final ArrayList<String> amRanking;
    private final ArrayList<String> pmRanking;

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

    public ArrayList<String> getAmRanking() {
        return amRanking;
    }

    public ArrayList<String> getPmRanking() {
        return pmRanking;
    }
}
