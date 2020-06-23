package flink_dsp.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;
import utility.accumulators.ReasonRankingAccumulator;

import java.util.*;

/**
 * Class used to aggregate data for the second query
 */
public class ReasonRankingAggregator implements AggregateFunction<BusData, ReasonRankingAccumulator, ReasonRankingOutcome> {

	private static final int RANK_SIZE = 3;

	/**
	 * Function that initializes the ReasonRankingAccumulator
	 * @return a new accumulator
	 */
	@Override
	public ReasonRankingAccumulator createAccumulator() {
		return new ReasonRankingAccumulator();
	}

	/**
	 * Function called to aggregate the busData's information to the accumulator
	 * @param busData contains all the information to be aggregated
	 * @param accumulator contains aggregated values so far
	 * @return updated accumulator
	 */
	@Override
	public ReasonRankingAccumulator add(BusData busData, ReasonRankingAccumulator accumulator) {
		accumulator.add(busData.getEventTime(), busData.getReason());
		return accumulator;
	}

	/**
	 * Function called to merge two accumulators, it adds acc2's reason's information to acc1
	 * @param acc1 first accumulator to be merged
	 * @param acc2 second accumulator to be merged
	 * @return merged accumulator
	 */
	@Override
	public ReasonRankingAccumulator merge(ReasonRankingAccumulator acc1, ReasonRankingAccumulator acc2) {
		acc1.mergeRankings(acc2.getAmRanking(), acc2.getPmRanking());
		return acc1;
	}

	/**
	 * Called at the end of the computation, used to gain the result from the accumulator
	 * @param accumulator containing all the information
	 * @return a ReasonRankingOutcome with the results
	 */
	@Override
	public ReasonRankingOutcome getResult(ReasonRankingAccumulator accumulator) {

		// Create the lists from elements of HashMap
		List<Map.Entry<String, Long>> amList = new LinkedList<>(accumulator.getAmRanking().entrySet());
		List<Map.Entry<String, Long>> pmList = new LinkedList<>(accumulator.getPmRanking().entrySet());

		// Sort the lists in descending order
		amList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
		pmList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

		// Generating outcome
		ReasonRankingOutcome outcome = new ReasonRankingOutcome();
		for (int i = 0; i < RANK_SIZE; i++) {
			try {
				outcome.addAmRanking(amList.get(i).getKey());
			} catch (IndexOutOfBoundsException ignored) {
				// Less than RANK_SIZE elements
			}

			try {
				outcome.addPmRanking(pmList.get(i).getKey());
			} catch (IndexOutOfBoundsException ignored) {
				// Less than RANK_SIZE elements
			}
		}

		return outcome;
	}
}
