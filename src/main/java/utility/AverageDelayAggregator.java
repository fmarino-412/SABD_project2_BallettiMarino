package utility;

import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple2;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AverageDelayAggregator implements AggregateFunction<BusData, Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>>, Tuple2<String, Double>> {

    public Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> createAccumulator() {

        // keeps [total, counter, start date, end date]
        return new Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>>(new Tuple2<Long, Long>(0L, 0L),
                new Tuple2<Date, Date>(new Date(Long.MAX_VALUE), new Date(Long.MIN_VALUE)));
    }

    public Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> add(BusData busData,
                                                          Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> accumulator) {

        Tuple2<Long, Long> info = new Tuple2<Long, Long>(busData.getDelay() + accumulator._1()._1(),
                accumulator._1()._2() + 1L);

        Date startDate = accumulator._2()._1();
        Date endDate = accumulator._2()._2();
        Date currentElemDate = busData.getEventTime();

        if (currentElemDate.before(startDate)) {
            startDate = currentElemDate;
        }
        if (currentElemDate.after(endDate)) {
            endDate = currentElemDate;
        }

        return new Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>>(info, new Tuple2<Date, Date>(startDate, endDate));
    }

    public Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> merge(Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> acc1,
                                                                Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> acc2) {
        Tuple2<Long, Long> info = new Tuple2<Long, Long>(acc1._1()._1() + acc2._1()._1(),
                acc1._1()._2() + acc2._1()._2());

        Date startDate = acc1._2()._1();
        if ((acc2._2()._1()).before(startDate)) {
            startDate = acc2._2()._1();
        }

        Date endDate = acc1._2()._2();
        if ((acc2._2()._2()).after(endDate)) {
            endDate = acc2._2()._2();
        }

        return new Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>>(info, new Tuple2<Date, Date>(startDate, endDate));
    }

    public Tuple2<String, Double> getResult(Tuple2<Tuple2<Long, Long>, Tuple2<Date, Date>> accumulator) {

        DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        String date = format.format(accumulator._2()._1()) + "-" + format.format(accumulator._2()._2());
        Double mean = Double.valueOf(accumulator._1()._1()) / accumulator._1()._2();

        return new Tuple2<String, Double>(date, mean);
    }
}
