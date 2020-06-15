package flink_dsp.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;
import utility.accumulators.ReasonRankingAccumulator;

import java.util.*;

public class ReasonRankingAggregator implements AggregateFunction<BusData, ReasonRankingAccumulator, ReasonRankingOutcome> {

	private static final int RANK_SIZE = 3;

	@Override
	public ReasonRankingAccumulator createAccumulator() {
		return new ReasonRankingAccumulator();
	}

	@Override
	public ReasonRankingAccumulator add(BusData busData, ReasonRankingAccumulator accumulator) {
		accumulator.add(busData.getEventTime(), busData.getReason(), 1L);
		return accumulator;
	}

	@Override
	public ReasonRankingAccumulator merge(ReasonRankingAccumulator acc1, ReasonRankingAccumulator acc2) {
		// merge contents
		acc1.mergeRankings(acc2.getAmRanking(), acc2.getPmRanking());
		return acc1;
	}

	@Override
	public ReasonRankingOutcome getResult(ReasonRankingAccumulator accumulator) {

		// Create the lists from elements of HashMap
		List<Map.Entry<String, Long>> amList = new LinkedList<>(accumulator.getAmRanking().entrySet());
		List<Map.Entry<String, Long>> pmList = new LinkedList<>(accumulator.getPmRanking().entrySet());

		// Sort the lists
		amList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
		pmList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

		// Generating outcome
		ReasonRankingOutcome outcome = new ReasonRankingOutcome(accumulator.getStartDate());
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
