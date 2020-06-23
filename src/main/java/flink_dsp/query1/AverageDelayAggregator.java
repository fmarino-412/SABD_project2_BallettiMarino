package flink_dsp.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;
import utility.accumulators.AverageDelayAccumulator;

/**
 * Class used to aggregate data for the first query
 */
public class AverageDelayAggregator implements AggregateFunction<BusData, AverageDelayAccumulator, AverageDelayOutcome> {

	/**
	 * Function that initializes the AverageDelayAccumulator
	 * @return a new accumulator
	 */
	public AverageDelayAccumulator createAccumulator() {
		return new AverageDelayAccumulator();
	}

	/**
	 * Function called to aggregate the busData's information to the accumulator
	 * @param busData contains all the information to be aggregated
	 * @param accumulator contains aggregated values so far
	 * @return updated accumulator
	 */
	public AverageDelayAccumulator add(BusData busData, AverageDelayAccumulator accumulator) {
		accumulator.add(busData.getBoro(), busData.getDelay(), 1L);
		return accumulator;
	}

	/**
	 * Function called to merge two accumulators, it adds acc2's boro information to acc1
	 * @param acc1 first accumulator to be merged
	 * @param acc2 second accumulator to be merged
	 * @return merged accumulator
	 */
	public AverageDelayAccumulator merge(AverageDelayAccumulator acc1, AverageDelayAccumulator acc2) {
		acc2.getBoroMap().forEach((k, v) -> acc1.add(k, v.getTotal(), v.getCounter()));
		return acc1;
	}

	/**
	 * Called at the end of the computation, used to gain the result from the accumulator
	 * @param accumulator containing all the information
	 * @return an AverageDelayOutcome with the results
	 */
	public AverageDelayOutcome getResult(AverageDelayAccumulator accumulator) {
		AverageDelayOutcome outcome = new AverageDelayOutcome(accumulator.getStartDate());
		accumulator.getBoroMap().forEach((k, v) -> outcome.addMean(k, v.getTotal() / v.getCounter()));
		return outcome;
	}
}
