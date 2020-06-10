package query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple2;
import utility.BusData;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class AverageDelayAggregator implements AggregateFunction<BusData, AverageDelayAccumulator, AggregatorOutcome> {

    public AverageDelayAccumulator createAccumulator() {
        return new AverageDelayAccumulator();
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

        accumulator.setStartDate(startDate);
        accumulator.setEndDate(endDate);

        accumulator.add(busData.getBoro(), busData.getDelay(), 1L);

        return accumulator;
    }

    public AverageDelayAccumulator merge(AverageDelayAccumulator acc1, AverageDelayAccumulator acc2) {
        // adjust start and end date
        if ((acc2.getStartDate()).before(acc1.getStartDate())) {
            acc1.setStartDate(acc2.getStartDate());
        }
        if ((acc2.getEndDate()).after(acc1.getEndDate())) {
            acc1.setEndDate(acc2.getEndDate());
        }

        acc2.getBoroMap().forEach((k, v) -> acc1.add(k, v._1(), v._2()));

        return acc1;
    }

    public AggregatorOutcome getResult(AverageDelayAccumulator accumulator) {
        AggregatorOutcome outcome = new AggregatorOutcome(accumulator.getStartDate(), accumulator.getEndDate());
        accumulator.getBoroMap().forEach((k, v) -> outcome.addMean(k, Double.valueOf(v._1()) / v._2()));

        return outcome;
    }
}
