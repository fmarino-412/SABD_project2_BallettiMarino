package utility.accumulators;

import scala.Tuple2;

import java.util.Date;
import java.util.HashMap;

public class AverageDelayAccumulator {

    // TODO: use another class to do this with a default constructor and getter and setter operations!!
    private HashMap<String, Tuple2<Double, Long>> boroMap;
    private Date startDate;

    /* SERDE SCOPE */
    public AverageDelayAccumulator(HashMap<String, Tuple2<Double, Long>> boroMap, Date startDate) {
        this.boroMap = boroMap;
        this.startDate = startDate;
    }

    public AverageDelayAccumulator() {
        this.startDate = new Date(Long.MAX_VALUE);
        this.boroMap = new HashMap<>();
    }

    public void add(String boro, Double total, Long counter) {
        Tuple2<Double, Long> elem = this.boroMap.get(boro);
        if (elem == null) {
            this.boroMap.put(boro, new Tuple2<>(total, counter));
        } else {
            this.boroMap.put(boro, new Tuple2<>(elem._1() + total, elem._2() + counter));
        }
    }

    public HashMap<String, Tuple2<Double, Long>> getBoroMap() {
        return boroMap;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public void setBoroMap(HashMap<String, Tuple2<Double, Long>> boroMap) {
        this.boroMap = boroMap;
    }
}
