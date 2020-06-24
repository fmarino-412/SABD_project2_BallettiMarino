package utility.delay;

/**
 * Class representing parsed delay information
 */
public class DelayInfo {

	// hours of delay
	private final Long hours;
	// minutes of delay
	private final Long minutes;
	// true if biggest time unit is hours, false elsewhere
	private final boolean hoursData;

	public DelayInfo(Long hours, Long minutes, boolean hoursData) {
		this.hours = hours;
		this.minutes = minutes;
		this.hoursData = hoursData;
	}

	/**
	 * Get delay hours
	 * @return delay hours as long integer
	 */
	public Long getHours() {
		return hours;
	}

	/**
	 * Get delay minutes
	 * @return delay minutes as long integer
	 */
	public Long getMinutes() {
		return minutes;
	}

	/**
	 * Tells if biggest delay time unit is hours or minutes
	 * @return true if delay has hours components, false elsewhere
	 */
	public boolean hasHoursData() {
		return hoursData;
	}
}
