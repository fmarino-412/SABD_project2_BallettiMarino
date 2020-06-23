package flink_dsp.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.CompanyRankingAccumulator;

/**
 * Class used to aggregate data for the third query
 */
public class CompanyRankingAggregator implements AggregateFunction<BusData, CompanyRankingAccumulator, CompanyRankingOutcome> {

	/**
	 * Function that initializes the CompanyRankingAccumulator
	 * @return a new accumulator
	 */
	@Override
	public CompanyRankingAccumulator createAccumulator() {
		return new CompanyRankingAccumulator();
	}

	/**
	 * Function called to aggregate the busData's information to the accumulator
	 * @param busData contains all the information to be aggregated
	 * @param accumulator contains aggregated values so far
	 * @return updated accumulator
	 */
	@Override
	public CompanyRankingAccumulator add(BusData busData, CompanyRankingAccumulator accumulator) {
		accumulator.add(busData.getCompanyName(), busData.getReason(), busData.getDelay());
		return accumulator;
	}

	/**
	 * Function called to merge two accumulators, it adds acc2's companies' information to acc1
	 * @param acc1 first accumulator to be merged
	 * @param acc2 second accumulator to be merged
	 * @return merged accumulator
	 */
	@Override
	public CompanyRankingAccumulator merge(CompanyRankingAccumulator acc1, CompanyRankingAccumulator acc2) {
		acc2.getCompanyRanking().forEach((k, v) -> acc1.getCompanyRanking().merge(k, v, Double::sum));
		return acc1;
	}

	/**
	 * Called at the end of the computation, used to gain the result from the accumulator
	 * @param accumulator containing all the information
	 * @return a CompanyRankingOutcome with the results
	 */
	@Override
	public CompanyRankingOutcome getResult(CompanyRankingAccumulator accumulator) {
		return DataCommonTransformation.buildCompanyRankingOutcome(accumulator);
	}
}
