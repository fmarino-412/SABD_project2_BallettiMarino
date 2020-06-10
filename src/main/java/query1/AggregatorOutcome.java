package query1;

import java.util.Date;
import java.util.HashMap;

public class AggregatorOutcome {
    private Date startDate;
    private Date endDate;
    private HashMap<String, Double> boroMeans = new HashMap<>();

    public AggregatorOutcome(Date startDate, Date endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public void addMean(String boro, Double mean) {
        if (boro.equals("")) {
            //TODO: handle this case
            return;
        }
        this.boroMeans.put(boro, mean);
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public HashMap<String, Double> getBoroMeans() {
        return boroMeans;
    }

    public void setBoroMeans(HashMap<String, Double> boroMeans) {
        this.boroMeans = boroMeans;
    }
}
