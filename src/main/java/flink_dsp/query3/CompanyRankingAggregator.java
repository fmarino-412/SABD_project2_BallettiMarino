package flink_dsp.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;
import utility.DataCommonTransformation;
import utility.accumulators.CompanyRankingAccumulator;

public class CompanyRankingAggregator implements AggregateFunction<BusData, CompanyRankingAccumulator, CompanyRankingOutcome> {

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

		return DataCommonTransformation.buildCompanyRankingOutcome(accumulator);
	}
}
