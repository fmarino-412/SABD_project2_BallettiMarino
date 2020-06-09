package utility;

import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple2;

public class AverageDelayAggregator implements AggregateFunction<BusData, Tuple2<Long, Long>, Double> {

    public Tuple2<Long, Long> createAccumulator() {
        // [total, counter]
        return new Tuple2<Long, Long>(0L, 0L);
    }

    public Tuple2<Long, Long> add(BusData busData, Tuple2<Long, Long> accumulator) {
        return new Tuple2<Long, Long>(accumulator._1() + busData.getDelay(), accumulator._2() + 1L);
    }

    public Double getResult(Tuple2<Long, Long> accumulator) {
        return Double.valueOf(accumulator._1()) / accumulator._2();
    }

    public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
        return new Tuple2<Long, Long>(acc1._1() + acc2._1(), acc1._2() + acc2._2());
    }
}
