package query1;

import java.util.Date;
import java.util.HashMap;

public class AverageDelayOutcome {
    private Date startDate;
    private HashMap<String, Double> boroMeans = new HashMap<>();

    public AverageDelayOutcome(Date startDate) {
        this.startDate = startDate;
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

    public HashMap<String, Double> getBoroMeans() {
        return boroMeans;
    }

    public void setBoroMeans(HashMap<String, Double> boroMeans) {
        this.boroMeans = boroMeans;
    }
}
