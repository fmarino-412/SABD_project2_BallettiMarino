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

    public static Double parseDelay(String dirtyDelay) throws DelayFormatException {

        double totalMinutes = 0;
        DelayInfo current;

        String originalString = dirtyDelay;

        dirtyDelay = dirtyDelay.toLowerCase().replaceAll("\\s+", "")
                .replaceAll("\\+", "")
                .replaceAll("\\.", "")
                .replaceAll(",", "")
                .replaceAll("--", "")
                .replaceAll("\\?", "")
                .replaceAll(":", "")
                .replaceAll("!", "");

        // resilience to - and /, perform mean
        if (dirtyDelay.contains("-") || dirtyDelay.contains("/")) {
            String[] parts = dirtyDelay.split("[-/]");
            boolean singleAsHours = false;
            int counter = 0;
            for (int i = parts.length - 1; i >= 0; i--) {
                current = parseCleanDelay(parts[i], singleAsHours);
                if (current.getHours() != 0 || current.getMinutes() != 0) {
                    counter++;
                    totalMinutes += (current.getHours()*60) + current.getMinutes();
                }
                singleAsHours = current.hasHoursData();
            }
            totalMinutes = totalMinutes / counter;
        } else {
            current = parseCleanDelay(dirtyDelay, false);
            totalMinutes = (current.getHours()*60) + current.getMinutes();
        }

        if (totalMinutes == 0) {
            throw new DelayFormatException("Could not find any delay information in string: " + originalString);
        } else {
            return totalMinutes;
        }


    }

    private static DelayInfo parseCleanDelay(String cleanDelay, boolean singleAsHours) {

        long minutes = 0;
        long hours = 0;

        Matcher minMatcher = getMinPattern().matcher(cleanDelay);
        Matcher hourMatcher = getHourPattern().matcher(cleanDelay);
        Matcher isolatedMatcher = getIsolatedPattern().matcher(cleanDelay);
        if (minMatcher.find()) {
            minutes = Long.parseLong(minMatcher.group(1));
        }
        if (hourMatcher.find()) {
            hours = Long.parseLong(hourMatcher.group(1));
        }
        // to avoid typos like 15mins0 (for 15 minutes) or 3hr7 (for 3 hours)
        if (isolatedMatcher.find() && ((!singleAsHours && minutes == 0) || (singleAsHours && hours == 0))) {
            if (singleAsHours) {
                hours = Long.parseLong(isolatedMatcher.group(1));
            } else {
                minutes = Long.parseLong(isolatedMatcher.group(1));
            }
        }

        return new DelayInfo(hours, minutes, hours != 0);
    }
}
