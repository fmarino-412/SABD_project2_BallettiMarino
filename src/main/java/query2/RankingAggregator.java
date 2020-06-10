package query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import utility.BusData;

import java.util.*;

public class RankingAggregator implements AggregateFunction<BusData, RankingAccumulator, RankingOutcome> {

    private static final int RANK_SIZE = 3;

    @Override
    public RankingAccumulator createAccumulator() {
        return new RankingAccumulator();
    }

    @Override
    public RankingAccumulator add(BusData busData, RankingAccumulator accumulator) {

        Date startDate = accumulator.getStartDate();
        Date currentElemDate = busData.getEventTime();

        if (currentElemDate.before(startDate)) {
            startDate = currentElemDate;
        }

        accumulator.setStartDate(startDate);

        accumulator.add(currentElemDate, busData.getReason(), 1L);

        return accumulator;
    }

    @Override
    public RankingAccumulator merge(RankingAccumulator acc1, RankingAccumulator acc2) {
        // adjust start date
        if ((acc2.getStartDate()).before(acc1.getStartDate())) {
            acc1.setStartDate(acc2.getStartDate());
        }

        // merge contents
        acc1.mergeRankings(acc2.getAmRanking(), acc2.getPmRanking());

        return acc1;
    }

    @Override
    public RankingOutcome getResult(RankingAccumulator accumulator) {

        // Create the lists from elements of HashMap
        List<Map.Entry<String, Long>> amList = new LinkedList<>(accumulator.getAmRanking().entrySet());
        List<Map.Entry<String, Long>> pmList = new LinkedList<>(accumulator.getPmRanking().entrySet());

        // Sort the lists
        amList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        pmList.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        // Generating outcome
        RankingOutcome outcome = new RankingOutcome(accumulator.getStartDate());
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
