package utility.delay;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Delay string parser
 */
public class DelayParsingUtility {

	// regex for minutes delay identification
	private final static String MIN_REGEX = "([0-9]*-*[0-9]+)(m.*)";
	// regex for hours delay identification
	private final static String HR_REGEX = "([0-9]*-*[0-9]+)(h.*)";
	// regex for isolated delay number identification
	private final static String ISOLATED_VALUE_REGEX = "([0-9]+$)";

	private static Pattern MIN_PATTERN = null;
	private static Pattern HOUR_PATTERN = null;
	private static Pattern ISOLATED_PATTERN = null;

	/**
	 * Builds a pattern to search for minutes delay
	 * @return regex pattern to use on a string
	 */
	private static Pattern getMinPattern() {
		// if not already defined, builds it
		if (MIN_PATTERN == null) {
			MIN_PATTERN = Pattern.compile(MIN_REGEX);
		}
		return MIN_PATTERN;
	}

	/**
	 * Builds a pattern to search for hours delay
	 * @return regex pattern to use on a string
	 */
	private static Pattern getHourPattern() {
		// if not already defined, builds it
		if (HOUR_PATTERN == null) {
			HOUR_PATTERN = Pattern.compile(HR_REGEX);
		}
		return HOUR_PATTERN;
	}

	/**
	 * Builds a pattern to search for isolated value delay
	 * @return regex pattern to use on a string
	 */
	private static Pattern getIsolatedPattern() {
		// if not already defined, builds it
		if (ISOLATED_PATTERN == null) {
			ISOLATED_PATTERN = Pattern.compile(ISOLATED_VALUE_REGEX);
		}
		return ISOLATED_PATTERN;
	}

	/**
	 * Parse a delay string
	 * @param dirtyDelay string containing delay to parse
	 * @return total delay minutes identified from the string as a long integer
	 * @throws DelayFormatException in case of absence of information in string
	 */
	public static Double parseDelay(String dirtyDelay) throws DelayFormatException {

		double totalMinutes = 0;
		boolean foundSomething = false;
		DelayInfo current;

		String originalString = dirtyDelay;

		// cleans the original string removing spaces, symbols and converting it to lower case
		dirtyDelay = dirtyDelay.toLowerCase().replaceAll("\\s+", "")
				.replaceAll("\\+", "")
				.replaceAll("\\.", "")
				.replaceAll(",", "")
				.replaceAll("--", "")
				.replaceAll("\\?", "")
				.replaceAll(":", "")
				.replaceAll("!", "");

		// in case of - or / evaluate every part separately and returns mean value of the parts
		if (dirtyDelay.contains("-") || dirtyDelay.contains("/")) {
			String[] parts = dirtyDelay.split("[-/]");
			boolean singleAsHours = false;
			int counter = 0;
			// process delay from right to left for a correct interpretation of delays
			for (int i = parts.length - 1; i >= 0; i--) {
				current = parseCleanDelay(parts[i], singleAsHours);
				if (current != null) {
					foundSomething = true;
					counter++;
					totalMinutes += (current.getHours()*60) + current.getMinutes();
					// if there were hours in the current string it will consider next string isolated values as hours
					// resilience to strings like 1/2hours, the first isolated value must be considered as hour.
					singleAsHours = current.hasHoursData();
				}
			}
			// evaluate mean
			totalMinutes = totalMinutes / counter;
		} else {
			// single value of delay in string
			current = parseCleanDelay(dirtyDelay, false);
			if (current != null) {
				foundSomething = true;
				totalMinutes = (current.getHours()*60) + current.getMinutes();
			}
		}

		if (!foundSomething) {
			throw new DelayFormatException("Could not find any delay information in string: " + originalString);
		} else {
			return totalMinutes;
		}


	}

	/**
	 * Parse a single part of a cleaned delay string
	 * @param cleanDelay cleaned input string
	 * @param singleAsHours tells whether to consider isolated values as hours or minutes
	 * @return delay information
	 */
	private static DelayInfo parseCleanDelay(String cleanDelay, boolean singleAsHours) {

		Long minutes = null;
		Long hours = null;

		Matcher minMatcher = getMinPattern().matcher(cleanDelay);
		Matcher hourMatcher = getHourPattern().matcher(cleanDelay);
		Matcher isolatedMatcher = getIsolatedPattern().matcher(cleanDelay);
		if (minMatcher.find()) {
			// minutes in string
			minutes = Long.parseLong(minMatcher.group(1));
		}
		if (hourMatcher.find()) {
			// hours in string
			hours = Long.parseLong(hourMatcher.group(1));
		}
		// isolated value in string; checks for previous translations in the same string to avoid typos
		// like 15mins0 (for 15 minutes) or 3hr7 (for 3 hours)
		if (isolatedMatcher.find() && ((!singleAsHours && minutes == null) || (singleAsHours && hours == null))) {
			if (singleAsHours) {
				// isolated value to consider as hour value
				hours = Long.parseLong(isolatedMatcher.group(1));
			} else {
				// isolated value to consider as minute value
				minutes = Long.parseLong(isolatedMatcher.group(1));
			}
		}
		if (hours == null && minutes == null) {
			return null;
		} else {
			return new DelayInfo(hours == null ? 0 : hours, minutes == null ? 0 : minutes, hours != null);
		}
	}
}
