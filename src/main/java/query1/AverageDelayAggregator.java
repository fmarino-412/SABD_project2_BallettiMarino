package query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple2;
import utility.BusData;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AverageDelayAggregator implements AggregateFunction<BusData, AverageDelayAccumulator,
        Tuple2<String, Double>> {

    public AverageDelayAccumulator createAccumulator() {
        // keeps [total, counter, start date, end date]
        return new AverageDelayAccumulator(0L, 0L, new Date(Long.MAX_VALUE), new Date(Long.MIN_VALUE));
    }

    public AverageDelayAccumulator add(BusData busData, AverageDelayAccumulator accumulator) {

        Date startDate = accumulator.getStartDate();
        Date endDate = accumulator.getEndDate();
        Date currentElemDate = busData.getEventTime();

        if (currentElemDate.before(startDate)) {
            startDate = currentElemDate;
        }
        if (currentElemDate.after(endDate)) {
            endDate = currentElemDate;
        }

        return new AverageDelayAccumulator(busData.getDelay() + accumulator.getTotal(),
                accumulator.getCount() + 1L, startDate, endDate);
    }

    public AverageDelayAccumulator merge(AverageDelayAccumulator acc1, AverageDelayAccumulator acc2) {

        Date startDate = acc1.getStartDate();
        if ((acc2.getStartDate()).before(startDate)) {
            startDate = acc2.getStartDate();
        }

        Date endDate = acc1.getEndDate();
        if ((acc2.getEndDate()).after(endDate)) {
            endDate = acc2.getEndDate();
        }

        return new AverageDelayAccumulator(acc1.getTotal() + acc2.getTotal(),
                acc1.getCount() + acc2.getCount(), startDate, endDate);
    }

    public Tuple2<String, Double> getResult(AverageDelayAccumulator accumulator) {

        DateFormat format = new SimpleDateFormat("yyyy/MM/dd");
        String date = format.format(accumulator.getStartDate()) + "-" + format.format(accumulator.getEndDate());
        Double mean = Double.valueOf(accumulator.getTotal()) / accumulator.getCount();

        return new Tuple2<>(date, mean);
    }
}
