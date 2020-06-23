package utility.delay_utility;

/**
 * Scope: Query 3
 * Class used to compute delay score for third query evaluation
 */
public class DelayScorer {

    // static weights
    private static final Double WT = 0.3;
    private static final Double WM = 0.5;
    private static final Double WO = 0.2;

    private static final String TRAFFIC = "Heavy Traffic";
    private static final String MECHANIC = "Mechanical Problem";

    /**
     * Computes a score basing on the delay reason and time
     * @param reason string containing delay reason to evaluate score
     * @param countTwice true if score must be doubled, false elsewhere
     * @return score as double number
     */
    public static Double computeAmount(String reason, boolean countTwice) {
        int counter = countTwice ? 2:1;

        switch (reason) {
            case TRAFFIC:
                return WT * counter;
            case MECHANIC:
                return WM * counter;
            default:
                return WO * counter;
        }
    }
}
