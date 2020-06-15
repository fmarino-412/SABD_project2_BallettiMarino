package flink_dsp.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;
import utility.accumulators.AverageDelayAccumulator;

import java.util.Date;

public class AverageDelayAggregator implements AggregateFunction<BusData, AverageDelayAccumulator, AverageDelayOutcome> {

	public AverageDelayAccumulator createAccumulator() {
		return new AverageDelayAccumulator();
	}

	public AverageDelayAccumulator add(BusData busData, AverageDelayAccumulator accumulator) {

		Date startDate = accumulator.getStartDate();
		Date currentElemDate = busData.getEventTime();

		if (currentElemDate.before(startDate)) {
			startDate = currentElemDate;
		}

		accumulator.setStartDate(startDate);
		accumulator.add(busData.getBoro(), busData.getDelay(), 1L);

		return accumulator;
	}

	public AverageDelayAccumulator merge(AverageDelayAccumulator acc1, AverageDelayAccumulator acc2) {
		// adjust start date
		if ((acc2.getStartDate()).before(acc1.getStartDate())) {
			acc1.setStartDate(acc2.getStartDate());
		}

		acc2.getBoroMap().forEach((k, v) -> acc1.add(k, v.getTotal(), v.getCounter()));

		return acc1;
	}

	public AverageDelayOutcome getResult(AverageDelayAccumulator accumulator) {
		AverageDelayOutcome outcome = new AverageDelayOutcome(accumulator.getStartDate());
		accumulator.getBoroMap().forEach((k, v) -> outcome.addMean(k, v.getTotal() / v.getCounter()));

		return outcome;
	}
}
