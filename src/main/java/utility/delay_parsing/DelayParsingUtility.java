package utility.delay_parsing;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelayParsingUtility {

    private final static String MIN_REGEX = "([0-9]*-*[0-9]+)(m.*)";
    private final static String HR_REGEX = "([0-9]*-*[0-9]+)(h.*)";
    private final static String ISOLATED_VALUE_REGEX = "([0-9]+$)";

    private static Pattern MIN_PATTERN = null;
    private static Pattern HOUR_PATTERN = null;
    private static Pattern ISOLATED_PATTERN = null;

    private static Pattern getMinPattern() {
        if (MIN_PATTERN == null) {
            MIN_PATTERN = Pattern.compile(MIN_REGEX);
        }
        return MIN_PATTERN;
    }

    private static Pattern getHourPattern() {
        if (HOUR_PATTERN == null) {
            HOUR_PATTERN = Pattern.compile(HR_REGEX);
        }
        return HOUR_PATTERN;
    }

    private static Pattern getIsolatedPattern() {
        if (ISOLATED_PATTERN == null) {
            ISOLATED_PATTERN = Pattern.compile(ISOLATED_VALUE_REGEX);
        }
        return ISOLATED_PATTERN;
    }

    public static Long parseDelay(String dirtyDelay) throws DelayFormatException {

        long minutes = 0;
        long hours = 0;

        String originalString = dirtyDelay;

        dirtyDelay = dirtyDelay.toLowerCase().replaceAll("\\s+", "")
                .replaceAll("\\+", "")
                .replaceAll("\\.", "")
                .replaceAll(",", "")
                .replaceAll("--", "")
                .replaceAll("\\?", "")
                .replaceAll(":", "")
                .replaceAll("!", "");

        // edit resilience to - and /
        if (dirtyDelay.contains("-")) {
            dirtyDelay = dirtyDelay.split("-")[1];
        }

        if (dirtyDelay.contains("/")) {
            dirtyDelay = dirtyDelay.split("/")[1];
        }

        Matcher minMatcher = getMinPattern().matcher(dirtyDelay);
        Matcher hourMatcher = getHourPattern().matcher(dirtyDelay);
        Matcher isolatedMatcher = getIsolatedPattern().matcher(dirtyDelay);
        if (minMatcher.find()) {
            minutes = Long.parseLong(minMatcher.group(1));
        }
        if (hourMatcher.find()) {
            hours = Long.parseLong(hourMatcher.group(1));
        }
        if (isolatedMatcher.find()) {
            minutes = Long.parseLong(isolatedMatcher.group(1));
        }

        if (hours == 0 && minutes == 0) {
            throw new DelayFormatException("Could not find any delay information in string: " + originalString);
        }

        return (60 * hours) + minutes;
    }
}
