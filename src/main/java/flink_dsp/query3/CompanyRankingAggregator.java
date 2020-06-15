package flink_dsp.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import scala.Tuple2;
import utility.BusData;
import utility.accumulators.CompanyRankingAccumulator;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CompanyRankingAggregator implements AggregateFunction<BusData, CompanyRankingAccumulator, CompanyRankingOutcome> {

	private static final int RANK_SIZE = 5;

	@Override
	public CompanyRankingAccumulator createAccumulator() {
		return new CompanyRankingAccumulator();
	}

	@Override
	public CompanyRankingAccumulator add(BusData busData, CompanyRankingAccumulator accumulator) {
		accumulator.add(busData.getCompanyName(), busData.getReason(), busData.getDelay());
		return accumulator;
	}

	@Override
	public CompanyRankingAccumulator merge(CompanyRankingAccumulator acc1, CompanyRankingAccumulator acc2) {
		// merge contents
		acc2.getCompanyRanking().forEach((k, v) -> acc1.getCompanyRanking().merge(k, v, Double::sum));
		return acc1;
	}

	@Override
	public CompanyRankingOutcome getResult(CompanyRankingAccumulator accumulator) {

		// Create the list from elements of HashMap
		List<Map.Entry<String, Double>> ranking = new LinkedList<>(accumulator.getCompanyRanking().entrySet());

		// Sort the list
		ranking.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

		// Generating outcome
		CompanyRankingOutcome outcome = new CompanyRankingOutcome(accumulator.getStartDate());
		for (int i = 0; i < RANK_SIZE; i++) {
			try {
				outcome.addRanking(new Tuple2<>(ranking.get(i).getKey(), ranking.get(i).getValue()));
			} catch (IndexOutOfBoundsException ignored) {
				// less than RANK_SIZE elements
			}
		}

		return outcome;
	}
}
