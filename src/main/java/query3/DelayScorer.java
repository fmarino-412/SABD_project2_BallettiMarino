package query3;

public class DelayScorer {

    private static final Double WT = 0.3;
    private static final Double WM = 0.5;
    private static final Double WO = 0.2;

    private static final String TRAFFIC = "Heavy Traffic";
    private static final String MECHANIC = "Mechanical Problem";

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
