package query2;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

public class RankingAccumulator {
    private Date startDate;
    private HashMap<String, Long> amRanking;
    private HashMap<String, Long> pmRanking;

    public RankingAccumulator() {
        this.startDate = new Date(Long.MAX_VALUE);
        this.amRanking = new HashMap<>();
        this.pmRanking = new HashMap<>();
    }

    public void add(Date date, String reason, Long counter) {
        //threshold setup
        Calendar threshold = Calendar.getInstance(Locale.US);
        threshold.setTime(date);
        threshold.set(Calendar.HOUR_OF_DAY, 12);
        threshold.set(Calendar.MINUTE, 0);
        threshold.set(Calendar.SECOND, 0);
        threshold.set(Calendar.MILLISECOND, 0);

        //element setup
        Calendar elem = Calendar.getInstance(Locale.US);
        elem.setTime(date);

        //check if it falls in am or pm
        if (elem.before(threshold)) {
            this.amRanking.merge(reason, counter, Long::sum);
        } else {
            this.pmRanking.merge(reason, counter, Long::sum);
        }
    }

    public void mergeRankings(HashMap<String, Long> am, HashMap<String, Long> pm) {
        am.forEach((k, v) -> this.amRanking.merge(k, v, Long::sum));
        pm.forEach((k, v) -> this.pmRanking.merge(k, v, Long::sum));
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public HashMap<String, Long> getAmRanking() {
        return amRanking;
    }

    public void setAmRanking(HashMap<String, Long> amRanking) {
        this.amRanking = amRanking;
    }

    public HashMap<String, Long> getPmRanking() {
        return pmRanking;
    }

    public void setPmRanking(HashMap<String, Long> pmRanking) {
        this.pmRanking = pmRanking;
    }
}
