package query1;

import scala.Tuple2;

import java.util.Date;
import java.util.HashMap;

public class AverageDelayAccumulator {

    private HashMap<String, Tuple2<Long, Long>> boroMap = new HashMap<>();
    private Date startDate;
    private Date endDate;

    public AverageDelayAccumulator() {
        this.startDate = new Date(Long.MAX_VALUE);
        this.endDate = new Date(Long.MIN_VALUE);
    }

    public void add(String boro, Long total, Long counter) {
        Tuple2<Long, Long> elem = this.boroMap.get(boro);
        if (elem == null) {
            this.boroMap.put(boro, new Tuple2<>(total, counter));
        } else {
            this.boroMap.put(boro, new Tuple2<>(elem._1() + total, elem._2() + counter));
        }
    }

    public HashMap<String, Tuple2<Long, Long>> getBoroMap() {
        return boroMap;
    }

    public void setBoroMap(HashMap<String, Tuple2<Long, Long>> boroMap) {
        this.boroMap = boroMap;
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
}
