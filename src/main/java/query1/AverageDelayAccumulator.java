package query1;

import scala.Tuple2;

import java.util.Date;
import java.util.HashMap;

public class AverageDelayAccumulator {

    private final HashMap<String, Tuple2<Double, Long>> boroMap;
    private Date startDate;

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
}
